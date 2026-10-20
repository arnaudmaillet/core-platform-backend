use std::sync::Arc;

use chrono::{DateTime, Utc};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope, QueryBus};

use transport::grpc::edge;
use crate::domain::value_object::{
    InteractionAudience, InteractionSettings, LocationAudience, LocationPrecision,
};
use crate::application::command::{
    SetCommentFiltersCommand, SetDiscoverySettingsCommand, SetFeedSettingsCommand, SetInteractionSettingsCommand,
    SetTabSettingsCommand, SetLocationSettingsCommand,
    ChangeHandleCommand, CreateProfileCommand, DeleteProfileCommand, HideProfileCommand,
    RestoreProfileCommand, SetVisibilityCommand, UpdateAvatarCommand, UpdateBannerCommand,
    UpdateProfileCommand, VerifyProfileCommand,
};
use crate::application::port::{ProfileSummary, ProfileView};
use crate::domain::value_object::Viewer;
use crate::infrastructure::client::MediaDelivery;
use crate::application::query::{
    GetProfileByHandleQuery, GetProfileByIdQuery, ListProfilesByAccountQuery,
};

// ── Proto inclusion ───────────────────────────────────────────────────────────

pub use profile_api as proto;

pub use proto::profile_service_server::ProfileServiceServer;

/// gRPC handler for the Profile service.
///
/// Bridges Protobuf RPCs to CQRS command/query envelopes and back.
pub struct ProfileServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    command_bus: CB,
    query_bus:   QB,
    /// Verifies the staff token on staff calls (#837); denies all by default.
    staff:       edge::StaffGate,
    /// Delivers avatars and banners on read (#907); `None` when media is not
    /// configured.
    media:       Option<Arc<dyn MediaDelivery>>,
    /// Profile slots (#916); `None`: GetProfileSlots / BuyProfileSlot are
    /// UNAVAILABLE.
    slots:       Option<Arc<crate::application::profile_slots::ProfileSlots>>,
    /// Paid handle changes (#911); `None`: GetHandleChangePrice is UNAVAILABLE.
    changes:     Option<Arc<crate::application::handle_changes::HandleChanges>>,
}

impl<CB, QB> ProfileServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub fn new(command_bus: CB, query_bus: QB) -> Self {
        Self { command_bus, query_bus, staff: edge::StaffGate::deny_all(), media: None, slots: None, changes: None }
    }

    /// Avatars and banners carry their media asset's delivery (#907).
    pub fn with_media(mut self, media: Arc<dyn MediaDelivery>) -> Self {
        self.media = Some(media);
        self
    }

    pub fn with_slots(mut self, slots: Arc<crate::application::profile_slots::ProfileSlots>) -> Self {
        self.slots = Some(slots);
        self
    }

    pub fn with_changes(mut self, changes: Arc<crate::application::handle_changes::HandleChanges>) -> Self {
        self.changes = Some(changes);
        self
    }

    /// The end user behind a call, for a priced action (#911); `None` on the
    /// mesh.
    fn payer<T>(request: &Request<T>) -> Option<crate::application::handle_changes::HandlePayer> {
        edge::principal(request).map(|p| crate::application::handle_changes::HandlePayer {
            adult: p.is_adult(),
            token: edge::bearer_token(request.metadata()),
        })
    }

    /// What the profile's next handle change costs (#911). Edge: the
    /// profile's owner.
    pub async fn get_handle_change_price(
        &self,
        request: Request<proto::GetHandleChangePriceRequest>,
    ) -> Result<Response<proto::HandleChangePrice>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let changes = self.changes.as_deref().ok_or_else(|| Status::unavailable("handle change prices are not configured"))?;
        let profile = crate::domain::value_object::ProfileId::try_from(request.get_ref().profile_id.as_str())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        // A mesh reader is priced as an adult.
        let payer = Self::payer(&request).unwrap_or(crate::application::handle_changes::HandlePayer { adult: true, token: None });
        let quote = changes.quote_for_profile(&profile, &payer, chrono::Utc::now()).await.map_err(profile_error_to_status)?;
        Ok(Response::new(proto::HandleChangePrice {
            price_gems:              quote.price,
            gem_spending_restricted: quote.restricted,
            next_change_at:          quote.next_change_at.map(|at| prost_types::Timestamp {
                seconds: at.timestamp(),
                nanos:   i32::try_from(at.timestamp_subsec_nanos()).unwrap_or(0),
            }),
        }))
    }

    fn slots(&self) -> Result<&crate::application::profile_slots::ProfileSlots, Status> {
        self.slots.as_deref().ok_or_else(|| Status::unavailable("profile slots are not configured"))
    }

    /// The caller's account's profile slots (#916). Edge: the caller's account.
    pub async fn get_profile_slots(
        &self,
        request: Request<proto::GetProfileSlotsRequest>,
    ) -> Result<Response<proto::ProfileSlots>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let account = crate::domain::value_object::AccountId::try_from(request.get_ref().account_id.as_str())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let state = self.slots()?.state(&account).await.map_err(profile_error_to_status)?;
        Ok(Response::new(slots_to_proto(state)))
    }

    /// Buys the account's next profile slot (#916). Edge: the caller's
    /// account, an adult (the token's `age`), whose token goes on to the
    /// wallet.
    pub async fn buy_profile_slot(
        &self,
        request: Request<proto::BuyProfileSlotRequest>,
    ) -> Result<Response<proto::BuyProfileSlotResponse>, Status> {
        use crate::application::profile_slots::SlotOutcome;
        edge::require_account(&request, &request.get_ref().account_id)?;
        let adult = edge::principal(&request).is_some_and(|p| p.is_adult());
        let token = edge::bearer_token(request.metadata());
        let req = request.into_inner();
        let account = crate::domain::value_object::AccountId::try_from(req.account_id.as_str())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let reply = self
            .slots()?
            .buy(&account, req.expected_price, adult, token.as_deref())
            .await
            .map_err(profile_error_to_status)?;
        Ok(Response::new(proto::BuyProfileSlotResponse {
            outcome:    match reply.outcome {
                SlotOutcome::Bought => proto::ProfileSlotOutcome::Bought,
                SlotOutcome::MaxReached => proto::ProfileSlotOutcome::MaxReached,
                SlotOutcome::InsufficientGems => proto::ProfileSlotOutcome::InsufficientGems,
                SlotOutcome::PriceChanged => proto::ProfileSlotOutcome::PriceChanged,
            } as i32,
            slots:      Some(slots_to_proto(reply.state)),
            gems:       reply.gems.unwrap_or_default(),
            price_gems: reply.price,
        }))
    }

    /// Only for clients: a mesh reader has no use for renditions, and would
    /// only load media.
    fn media_for(&self, internal: bool) -> Option<&dyn MediaDelivery> {
        if internal { None } else { self.media.as_deref() }
    }

    /// The staff-token verifier for the verification review RPCs (#837).
    pub fn with_staff_gate(mut self, gate: edge::StaffGate) -> Self {
        self.staff = gate;
        self
    }

    fn ok_cmd(profile_id: &str) -> Response<proto::CommandResponse> {
        Response::new(proto::CommandResponse { success: true, profile_id: profile_id.to_owned(), ..Default::default() })
    }
}

// ── Command implementations ───────────────────────────────────────────────────

impl<CB, QB> ProfileServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn create_profile(
        &self,
        request: Request<proto::CreateProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        // Teen defaults: a 13–17 holder's profile starts private, with only
        // followers commenting / mentioning / messaging and no downloads (they
        // may relax them later).
        let minor = edge::principal(&request).is_some_and(|p| p.is_minor());
        let req = request.into_inner();
        let kind = profile_kind_i32_to_str(req.profile_kind)
            .ok_or_else(|| Status::invalid_argument("unknown profile_kind"))?;
        let cmd = CreateProfileCommand {
            account_id:   req.account_id.clone(),
            handle:       req.handle,
            display_name: req.display_name,
            bio:          Some(req.bio).filter(|s| !s.is_empty()),
            avatar_url:   Some(req.avatar_url).filter(|s| !s.is_empty()),
            banner_url:   Some(req.banner_url).filter(|s| !s.is_empty()),
            profile_kind: kind.to_owned(),
            locale:       req.locale,
            minor,
        };
        match self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await {
            Ok(_) => Ok(Self::ok_cmd(&req.account_id)),
            // Every slot in use (#916): answered in-band, nothing created.
            Err(cqrs::error::CqrsError::Handler(e)) if {
                use error::AppError as _;
                e.error_code() == "PRF-6001"
            } =>
            {
                Ok(Response::new(proto::CommandResponse { success: false, no_free_slot: true, ..Default::default() }))
            }
            Err(e) => Err(cqrs_error_to_status(e)),
        }
    }

    pub async fn update_profile(
        &self,
        request: Request<proto::UpdateProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let profile_id = req.profile_id.clone();  // saved before req is consumed
        let links = req.custom_links
            .into_iter()
            .map(|l| (l.label, l.url))
            .collect();
        let cmd = UpdateProfileCommand {
            profile_id: profile_id.clone(),
            display_name: Some(req.display_name).filter(|s| !s.is_empty()),
            bio:          Some(req.bio).filter(|s| !s.is_empty()),
            website_url:  Some(req.website_url).filter(|s| !s.is_empty()),
            locale:       Some(req.locale).filter(|s| !s.is_empty()),
            custom_links: links,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&profile_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn change_handle(
        &self,
        request: Request<proto::ChangeHandleRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let payer = Self::payer(&request);
        let req = request.into_inner();
        let cmd = ChangeHandleCommand {
            profile_id: req.profile_id.clone(),
            new_handle: req.new_handle,
            expected_price: req.expected_price,
            payer,
        };
        // A refusal of the price (#911) is answered in-band: nothing changed,
        // nothing charged.
        let refused = |outcome: proto::HandleChangeOutcome| {
            Ok(Response::new(proto::CommandResponse { success: false, handle_change: outcome as i32, ..Default::default() }))
        };
        match self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await {
            Ok(_) => {
                let mut ok = Self::ok_cmd(&req.profile_id);
                ok.get_mut().handle_change = proto::HandleChangeOutcome::Changed as i32;
                Ok(ok)
            }
            Err(cqrs::error::CqrsError::Handler(e)) => {
                use error::AppError as _;
                match e.error_code() {
                    "PRF-6004" => refused(proto::HandleChangeOutcome::PriceChanged),
                    "PRF-6005" => refused(proto::HandleChangeOutcome::Cooldown),
                    "PRF-6006" => refused(proto::HandleChangeOutcome::InsufficientGems),
                    _ => Err(cqrs_error_to_status(cqrs::error::CqrsError::Handler(e))),
                }
            }
            Err(e) => Err(cqrs_error_to_status(e)),
        }
    }

    /// Picks the badges the profile shows (#912). Edge: the profile's owner.
    pub async fn set_badge_showcase(
        &self,
        request: Request<proto::SetBadgeShowcaseRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = crate::application::command::SetBadgeShowcaseCommand { profile_id: req.profile_id.clone(), badge_ids: req.badge_ids };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// Wears an avatar frame, or none (#913). Edge: the profile's owner.
    pub async fn equip_avatar_frame(
        &self,
        request: Request<proto::EquipAvatarFrameRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = crate::application::command::EquipAvatarFrameCommand {
            profile_id: req.profile_id.clone(),
            frame_id:   Some(req.frame_id).filter(|f| !f.is_empty()),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn update_avatar(
        &self,
        request: Request<proto::UpdateAvatarRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = UpdateAvatarCommand {
            profile_id: req.profile_id.clone(),
            avatar_url: Some(req.avatar_url).filter(|s| !s.is_empty()),
            avatar_asset_id: Some(req.avatar_asset_id).filter(|s| !s.is_empty()),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn update_banner(
        &self,
        request: Request<proto::UpdateBannerRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = UpdateBannerCommand {
            profile_id: req.profile_id.clone(),
            banner_url: Some(req.banner_url).filter(|s| !s.is_empty()),
            banner_asset_id: Some(req.banner_asset_id).filter(|s| !s.is_empty()),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn set_visibility(
        &self,
        request: Request<proto::SetVisibilityRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let visibility = profile_visibility_i32_to_str(req.visibility)
            .ok_or_else(|| Status::invalid_argument("unknown visibility value"))?;
        let cmd = SetVisibilityCommand {
            profile_id: req.profile_id.clone(),
            visibility: visibility.to_owned(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// Turns the owner's temporary interaction limit on (edge: one of the caller's profiles).
    pub async fn set_interaction_limit(
        &self,
        request: Request<proto::SetInteractionLimitRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        use crate::application::command::SetInteractionLimitCommand;
        use crate::domain::value_object::{InteractionLimit, LimitAudience};
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let audience = match proto::LimitAudience::try_from(req.audience) {
            Ok(proto::LimitAudience::NonFollowers) => LimitAudience::NonFollowers,
            Ok(proto::LimitAudience::RecentFollowers) => LimitAudience::RecentFollowers,
            _ => return Err(Status::invalid_argument("audience must be set")),
        };
        let until = chrono::DateTime::from_timestamp_millis(req.until_ms)
            .ok_or_else(|| Status::invalid_argument("until_ms out of range"))?;
        let limit = InteractionLimit::new(audience, until, chrono::Utc::now())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let cmd = SetInteractionLimitCommand { profile_id: req.profile_id.clone(), limit: Some(limit) };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// Turns the owner's temporary interaction limit off.
    pub async fn clear_interaction_limit(
        &self,
        request: Request<proto::ClearInteractionLimitRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        use crate::application::command::SetInteractionLimitCommand;
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = SetInteractionLimitCommand { profile_id: req.profile_id.clone(), limit: None };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// The owner's interaction settings (edge: one of the caller's profiles).
    pub async fn set_interaction_settings(
        &self,
        request: Request<proto::SetInteractionSettingsRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let s = req.settings.ok_or_else(|| Status::invalid_argument("settings are required"))?;
        let cmd = SetInteractionSettingsCommand {
            profile_id: req.profile_id.clone(),
            settings: InteractionSettings {
                comments: audience_from_proto(s.comments)?,
                mentions: audience_from_proto(s.mentions)?,
                messages: audience_from_proto(s.messages)?,
                allow_downloads: s.allow_downloads,
                show_like_counts: s.show_like_counts,
                // Overwritten by the command from the two options below.
                ..InteractionSettings::default()
            },
            allow_remix: s.allow_remix,
            allow_sound_reuse: s.allow_sound_reuse,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// The owner's ghost mode / location precision (edge: one of the caller's profiles).
    pub async fn set_location_settings(
        &self,
        request: Request<proto::SetLocationSettingsRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        // Capped unless the token says 18+: an unknown age (no date of birth
        // on file) is not known to be an adult, so the cap fails closed. The
        // mesh carries no principal and is not capped.
        let minor = edge::principal(&request).is_some_and(|p| !p.is_adult());
        let req = request.into_inner();
        let s = req.settings.ok_or_else(|| Status::invalid_argument("settings are required"))?;
        let precision = match proto::LocationPrecision::try_from(s.precision) {
            Ok(proto::LocationPrecision::Precise) => LocationPrecision::Precise,
            Ok(proto::LocationPrecision::City) => LocationPrecision::City,
            _ => return Err(Status::invalid_argument("precision must be set")),
        };
        let audience = match proto::LocationAudience::try_from(s.audience) {
            Ok(proto::LocationAudience::Everyone) => Some(LocationAudience::Everyone),
            Ok(proto::LocationAudience::Followers) => Some(LocationAudience::Followers),
            Ok(proto::LocationAudience::Mutuals) => Some(LocationAudience::Mutuals),
            Ok(proto::LocationAudience::Unspecified) => None,
            Err(_) => return Err(Status::invalid_argument("unknown location audience")),
        };
        let cmd = SetLocationSettingsCommand {
            profile_id: req.profile_id.clone(),
            ghost: s.ghost,
            precision,
            audience,
            on_new_posts: s.on_new_posts,
            minor,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// The owner's feed controls (edge: one of the caller's profiles).
    pub async fn set_feed_settings(
        &self,
        request: Request<proto::SetFeedSettingsRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        use crate::domain::value_object::{FeedSettings, SensitiveContent};
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let settings = req.settings.ok_or_else(|| Status::invalid_argument("settings are required"))?;
        let sensitive_content = match proto::SensitiveContent::try_from(settings.sensitive_content) {
            Ok(proto::SensitiveContent::Less) => SensitiveContent::Less,
            Ok(proto::SensitiveContent::Standard) => SensitiveContent::Standard,
            _ => return Err(Status::invalid_argument("sensitive_content must be set")),
        };
        let cmd = SetFeedSettingsCommand {
            profile_id: req.profile_id.clone(),
            settings:   FeedSettings { sensitive_content, non_personalized: settings.non_personalized },
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// The owner's post window and tab visibility (edge: one of the caller's profiles).
    pub async fn set_tab_settings(
        &self,
        request: Request<proto::SetTabSettingsRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        use crate::domain::value_object::PostWindow;
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let post_window = match proto::PostWindow::try_from(req.post_window) {
            Ok(proto::PostWindow::Unspecified) => None,
            Ok(proto::PostWindow::All) => Some(PostWindow::All),
            Ok(proto::PostWindow::SixMonths) => Some(PostWindow::SixMonths),
            Ok(proto::PostWindow::OneMonth) => Some(PostWindow::OneMonth),
            Ok(proto::PostWindow::ThreeDays) => Some(PostWindow::ThreeDays),
            Err(_) => return Err(Status::invalid_argument("unknown post_window")),
        };
        let cmd = SetTabSettingsCommand {
            profile_id:   req.profile_id.clone(),
            post_window,
            show_likes:   req.show_likes,
            show_saved:   req.show_saved,
            show_reposts: req.show_reposts,
            show_places:  req.show_places,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// The owner's hidden words and offensive filter (edge: one of the caller's profiles).
    pub async fn set_comment_filters(
        &self,
        request: Request<proto::SetCommentFiltersRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let filters = req.filters.ok_or_else(|| Status::invalid_argument("filters are required"))?;
        let cmd = SetCommentFiltersCommand {
            profile_id:       req.profile_id.clone(),
            hidden_words:     filters.hidden_words,
            filter_offensive: filters.filter_offensive,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// The owner's presence and discoverability (edge: one of the caller's profiles).
    pub async fn set_discovery_settings(
        &self,
        request: Request<proto::SetDiscoverySettingsRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        // Unless the token says 18+ (an unknown age fails closed); the mesh
        // carries no principal.
        let minor = edge::principal(&request).is_some_and(|p| !p.is_adult());
        let req = request.into_inner();
        let cmd = SetDiscoverySettingsCommand {
            profile_id:       req.profile_id.clone(),
            activity_status:  req.activity_status,
            read_receipts:    req.read_receipts,
            by_phone:         req.by_phone,
            by_email:         req.by_email,
            by_handle_search: req.by_handle_search,
            by_qr:            req.by_qr,
            in_suggestions:   req.in_suggestions,
            minor,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// The owner's account type (edge: one of the caller's profiles).
    pub async fn set_account_type(
        &self,
        request: Request<proto::SetAccountTypeRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        use crate::application::command::SetAccountTypeCommand;
        use crate::domain::value_object::{BusinessInfo, ProfileKind};
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        // Creator and business accounts are 18+: a teen's profile stays
        // personal (and never publishes a business contact card).
        let minor = edge::principal(&request).is_some_and(|p| p.is_minor());
        let req = request.into_inner();
        let kind = profile_kind_i32_to_str(req.kind)
            .and_then(|k| ProfileKind::try_from(k).ok())
            .ok_or_else(|| Status::invalid_argument("unknown kind"))?;
        if !account_type_allowed(kind, minor) {
            return Err(Status::failed_precondition("creator and business accounts are for holders 18 and over"));
        }
        let business = match req.business {
            Some(b) if kind == ProfileKind::Brand => Some(
                BusinessInfo::new(b.category, Some(b.contact_email), Some(b.contact_phone))
                    .map_err(|e| Status::invalid_argument(e.to_string()))?,
            ),
            _ => None,
        };
        let cmd = SetAccountTypeCommand { profile_id: req.profile_id.clone(), kind, business };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// The owner asks for a verification badge (edge: one of the caller's profiles).
    pub async fn request_verification(
        &self,
        request: Request<proto::RequestVerificationRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        use crate::application::command::RequestVerificationCommand;
        use crate::domain::value_object::VerificationKind;
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let category = verification_kind_i32_to_str(req.category)
            .and_then(|k| VerificationKind::try_from(k).ok())
            .ok_or_else(|| Status::invalid_argument("unknown category"))?;
        let cmd = RequestVerificationCommand {
            profile_id: req.profile_id.clone(),
            category,
            documents: req.documents,
            private_documents: req.private_document_ids,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    /// The owner follows its request (edge: one of the caller's profiles).
    pub async fn get_verification_request(
        &self,
        request: Request<proto::GetVerificationRequestRequest>,
    ) -> Result<Response<proto::GetVerificationRequestResponse>, Status> {
        use crate::application::query::GetVerificationRequestQuery;
        use crate::domain::entity::VerificationRequest;
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let profile_id = request.into_inner().profile_id;
        let found: Option<VerificationRequest> = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetVerificationRequestQuery { profile_id: profile_id.clone() }))
            .await
            .map_err(cqrs_error_to_status)?;
        Ok(Response::new(proto::GetVerificationRequestResponse {
            request: found.map(|r| verification_to_proto(&profile_id, r)),
        }))
    }

    /// Staff queue. Mesh only, and staff only (#837): the staff member's token
    /// (`authorization: Bearer`, holding `verification:review`) is verified.
    pub async fn list_pending_verification_requests(
        &self,
        request: Request<proto::ListPendingVerificationRequestsRequest>,
    ) -> Result<Response<proto::ListPendingVerificationRequestsResponse>, Status> {
        self.staff.require(&request, edge::VERIFICATION_REVIEW).await?;
        use crate::application::query::ListPendingVerificationsQuery;
        use crate::domain::entity::VerificationRequest;
        use crate::domain::value_object::ProfileId;
        let req = request.into_inner();
        let query = ListPendingVerificationsQuery {
            limit:      req.limit,
            page_token: Some(req.page_token).filter(|t| !t.is_empty()),
        };
        let (requests, next): (Vec<(ProfileId, VerificationRequest)>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;
        Ok(Response::new(proto::ListPendingVerificationRequestsResponse {
            requests: requests.into_iter().map(|(id, r)| verification_to_proto(&id.to_string(), r)).collect(),
            next_page_token: next.unwrap_or_default(),
        }))
    }

    /// Staff decision. Mesh only, and staff only (#837): the staff member's
    /// token (`verification:review`) is verified; the reviewer is logged.
    pub async fn decide_verification_request(
        &self,
        request: Request<proto::DecideVerificationRequestRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let reviewer = self.staff.require(&request, edge::VERIFICATION_REVIEW).await?.account_id().to_owned();
        use crate::application::command::DecideVerificationCommand;
        let req = request.into_inner();
        let cmd = DecideVerificationCommand {
            profile_id: req.profile_id.clone(),
            approve:    req.approve,
            reason:     Some(req.reason).filter(|r| !r.is_empty()),
        };
        tracing::info!(reviewer = %reviewer, profile_id = %req.profile_id, approve = req.approve, "verification decided");
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn verify_profile(
        &self,
        request: Request<proto::VerifyProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let kind = verification_kind_i32_to_str(req.verification_kind)
            .ok_or_else(|| Status::invalid_argument("unknown verification_kind"))?;
        let cmd = VerifyProfileCommand {
            profile_id:        req.profile_id.clone(),
            verification_kind: kind.to_owned(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn hide_profile(
        &self,
        request: Request<proto::HideProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let reason = masking_reason_i32_to_str(req.masking_reason)
            .ok_or_else(|| Status::invalid_argument("unknown masking_reason"))?;
        let cmd = HideProfileCommand {
            profile_id:        req.profile_id.clone(),
            masking_reason:    reason.to_owned(),
            suspension_reason: Some(req.suspension_reason).filter(|s| !s.is_empty()),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn restore_profile(
        &self,
        request: Request<proto::RestoreProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let cmd = RestoreProfileCommand {
            profile_id: req.profile_id.clone(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn delete_profile(
        &self,
        request: Request<proto::DeleteProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = DeleteProfileCommand {
            profile_id: req.profile_id.clone(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.profile_id))
            .map_err(cqrs_error_to_status)
    }
}

// ── Query implementations ─────────────────────────────────────────────────────

impl<CB, QB> ProfileServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn get_profile_by_id(
        &self,
        request: Request<proto::GetProfileByIdRequest>,
    ) -> Result<Response<proto::ProfileView>, Status> {
        let viewer = viewer_of(&request);
        let internal = viewer == Viewer::Internal;
        let req = request.into_inner();
        let query = GetProfileByIdQuery { profile_id: req.profile_id, viewer };
        let view: Option<ProfileView> = self.query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;

        self.deliver_view(view, internal).await
    }

    /// The owner's QR / share-link token (#661), issued on first ask.
    pub async fn get_share_token(
        &self,
        request: Request<proto::GetShareTokenRequest>,
    ) -> Result<Response<proto::ShareTokenResponse>, Status> {
        use crate::application::query::GetShareTokenQuery;
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let query = GetShareTokenQuery { profile_id: request.into_inner().profile_id };
        let token: String =
            self.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.map_err(cqrs_error_to_status)?;
        Ok(Response::new(proto::ShareTokenResponse { token }))
    }

    /// Revokes the owner's token for a new one (#661); the old one stops
    /// resolving at once.
    pub async fn rotate_share_token(
        &self,
        request: Request<proto::RotateShareTokenRequest>,
    ) -> Result<Response<proto::ShareTokenResponse>, Status> {
        use crate::application::query::{GetShareTokenQuery, RotateShareTokenCommand};
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let profile_id = request.into_inner().profile_id;
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), RotateShareTokenCommand { profile_id: profile_id.clone() }))
            .await
            .map_err(cqrs_error_to_status)?;
        let token: String = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetShareTokenQuery { profile_id }))
            .await
            .map_err(cqrs_error_to_status)?;
        Ok(Response::new(proto::ShareTokenResponse { token }))
    }

    /// A scanned token, resolved as the caller may see the profile (#661).
    pub async fn resolve_share_token(
        &self,
        request: Request<proto::ResolveShareTokenRequest>,
    ) -> Result<Response<proto::ProfileView>, Status> {
        use crate::application::query::ResolveShareTokenQuery;
        let viewer = viewer_of(&request);
        let internal = viewer == Viewer::Internal;
        let query = ResolveShareTokenQuery { token: request.into_inner().token, viewer };
        let view: Option<ProfileView> =
            self.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.map_err(cqrs_error_to_status)?;
        self.deliver_view(view, internal).await
    }

    pub async fn get_profile_by_handle(
        &self,
        request: Request<proto::GetProfileByHandleRequest>,
    ) -> Result<Response<proto::ProfileView>, Status> {
        let viewer = viewer_of(&request);
        let internal = viewer == Viewer::Internal;
        let req = request.into_inner();
        let query = GetProfileByHandleQuery { handle: req.handle, viewer };
        let view: Option<ProfileView> = self.query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;

        self.deliver_view(view, internal).await
    }

    pub async fn check_handle_availability(
        &self,
        request: Request<proto::CheckHandleAvailabilityRequest>,
    ) -> Result<Response<proto::CheckHandleAvailabilityResponse>, Status> {
        use crate::application::query::{CheckHandleAvailabilityQuery, HandleAvailability};
        let query = CheckHandleAvailabilityQuery { handle: request.into_inner().handle };
        let answer: HandleAvailability = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;
        let response = match answer {
            HandleAvailability::Available(handle) => proto::CheckHandleAvailabilityResponse {
                availability: proto::HandleAvailability::Available as i32,
                handle,
                invalid_reason: String::new(),
            },
            HandleAvailability::Taken(handle) => proto::CheckHandleAvailabilityResponse {
                availability: proto::HandleAvailability::Taken as i32,
                handle,
                invalid_reason: String::new(),
            },
            HandleAvailability::Invalid(reason) => proto::CheckHandleAvailabilityResponse {
                availability: proto::HandleAvailability::Invalid as i32,
                handle: String::new(),
                invalid_reason: reason,
            },
        };
        Ok(Response::new(response))
    }

    pub async fn list_profiles_by_account(
        &self,
        request: Request<proto::ListProfilesByAccountRequest>,
    ) -> Result<Response<proto::ListProfilesByAccountResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let internal = viewer_of(&request) == Viewer::Internal;
        let req = request.into_inner();
        let limit = req.limit.clamp(1, 100) as u32;
        let query = ListProfilesByAccountQuery {
            account_id:  req.account_id,
            limit,
            page_token:  Some(req.page_token).filter(|s| !s.is_empty()),
        };

        let (summaries, next_page_token): (Vec<ProfileSummary>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;

        let mut profiles: Vec<proto::ProfileSummaryView> = summaries.into_iter().map(summary_to_proto).collect();
        deliver_media(
            self.media_for(internal),
            profiles.iter_mut().map(|p| (p.avatar_asset_id.as_str(), &mut p.avatar)).collect(),
        )
        .await;
        Ok(Response::new(proto::ListProfilesByAccountResponse {
            profiles,
            next_page_token: next_page_token.unwrap_or_default(),
        }))
    }

    /// A profile view for the wire, its avatar and banner delivered (#907).
    async fn deliver_view(&self, view: Option<ProfileView>, internal: bool) -> Result<Response<proto::ProfileView>, Status> {
        let mut view = view.map(profile_view_to_proto).ok_or_else(|| Status::not_found("profile not found"))?;
        let proto::ProfileView { avatar_asset_id, avatar, banner_asset_id, banner, .. } = &mut view;
        deliver_media(
            self.media_for(internal),
            vec![(avatar_asset_id.as_str(), avatar), (banner_asset_id.as_str(), banner)],
        )
        .await;
        // An animated banner (#947) has no still URL: its poster is it.
        if view.banner_url.is_empty()
            && let Some(poster) = view.banner.as_ref().and_then(|b| {
                b.renditions.iter().find(|r| r.kind == media_api::RenditionKind::MediaRenditionKindPoster as i32)
            })
        {
            view.banner_url = poster.url.clone();
        }
        Ok(Response::new(view))
    }
}

/// Fills each `(asset id, slot)` with the asset's delivery (#907): one media
/// call for all of them. Fail-open — media unreachable or slow, the profiles
/// go out with their URLs only.
async fn deliver_media(media: Option<&dyn MediaDelivery>, slots: Vec<(&str, &mut Option<media_api::DeliveredMedia>)>) {
    let Some(media) = media else { return };
    let mut ids: Vec<String> = slots.iter().map(|(id, _)| (*id).to_owned()).filter(|id| !id.is_empty()).collect();
    ids.sort();
    ids.dedup();
    if ids.is_empty() {
        return;
    }
    let delivered = match media.deliver(&ids).await {
        Ok(delivered) => delivered,
        Err(status) => {
            tracing::warn!(%status, assets = ids.len(), "media delivery unavailable; images served by URL");
            return;
        }
    };
    let by_id: std::collections::HashMap<&str, &media_api::DeliveredMedia> =
        delivered.iter().map(|m| (m.asset_id.as_str(), m)).collect();
    for (id, slot) in slots {
        *slot = by_id.get(id).map(|m| (*m).clone());
    }
}

// ── Proto conversion helpers ──────────────────────────────────────────────────

fn dt_to_ts(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos:   dt.timestamp_subsec_nanos() as i32,
    }
}

/// The reader of a viewer-aware RPC, from how the request arrived.
fn viewer_of<T>(request: &Request<T>) -> Viewer {
    match edge::viewer(request) {
        edge::Viewer::Internal => Viewer::Internal,
        edge::Viewer::Anonymous => Viewer::Anonymous,
        edge::Viewer::Member { account_id, .. } => Viewer::Account(account_id),
    }
}

fn profile_view_to_proto(v: ProfileView) -> proto::ProfileView {
    proto::ProfileView {
        profile_id:        v.id,
        account_id:        v.account_id,
        handle:            v.handle,
        display_name:      v.display_name,
        bio:               v.bio.unwrap_or_default(),
        avatar_url:        v.avatar_url.unwrap_or_default(),
        banner_url:        v.banner_url.unwrap_or_default(),
        avatar_asset_id:   v.avatar_asset_id.unwrap_or_default(),
        banner_asset_id:   v.banner_asset_id.unwrap_or_default(),
        avatar_frame_id:   v.avatar_frame_id.unwrap_or_default(),
        badges:            v.badges.into_iter()
            .map(|b| proto::ShowcasedBadge {
                badge_id:   b.badge_id,
                awarded_at: Some(prost_types::Timestamp {
                    seconds: b.awarded_at.timestamp(),
                    nanos:   i32::try_from(b.awarded_at.timestamp_subsec_nanos()).unwrap_or(0),
                }),
            })
            .collect(),
        avatar:            None,
        banner:            None,
        website_url:       v.website_url.unwrap_or_default(),
        custom_links:      v.custom_links.into_iter()
            .map(|l| proto::ProfileLinkProto { label: l.label, url: l.url })
            .collect(),
        profile_kind:      profile_kind_str_to_i32(&v.profile_kind),
        visibility:        profile_visibility_str_to_i32(&v.visibility),
        verified:          v.verified,
        verification_kind: v.verification_kind
            .as_deref()
            .map(verification_kind_str_to_i32)
            .unwrap_or(0),
        locale:            v.locale,
        timezone:          v.timezone.unwrap_or_default(),
        status:            profile_status_str_to_i32(&v.status),
        masking_reason:    v.masking_reason.unwrap_or_default(),
        masked_at:         v.masked_at.map(dt_to_ts),
        created_at:        Some(dt_to_ts(v.created_at)),
        updated_at:        Some(dt_to_ts(v.updated_at)),
        version:           v.version,
        interaction_settings: Some(interaction_to_proto(v.interaction)),
        location_settings: v.location.map(|l| proto::LocationSettings {
            ghost: l.ghost,
            precision: (match l.precision {
                LocationPrecision::Precise => proto::LocationPrecision::Precise,
                LocationPrecision::City => proto::LocationPrecision::City,
            }) as i32,
            audience: (match l.audience {
                LocationAudience::Everyone => proto::LocationAudience::Everyone,
                LocationAudience::Followers => proto::LocationAudience::Followers,
                LocationAudience::Mutuals => proto::LocationAudience::Mutuals,
            }) as i32,
            on_new_posts: Some(l.on_new_posts),
        }),
        business_info: v.business_info.map(|b| proto::BusinessInfo {
            category:      b.category,
            contact_email: b.contact_email.unwrap_or_default(),
            contact_phone: b.contact_phone.unwrap_or_default(),
        }),
        feed_settings: v.feed_settings.map(|f| proto::FeedSettings {
            sensitive_content: (match f.sensitive_content {
                crate::domain::value_object::SensitiveContent::Less => proto::SensitiveContent::Less,
                crate::domain::value_object::SensitiveContent::Standard => proto::SensitiveContent::Standard,
            }) as i32,
            non_personalized: f.non_personalized,
        }),
        visible_tabs: v.visible_tabs.map(|t| proto::VisibleTabs {
            likes:   t.likes,
            saved:   t.saved,
            reposts: t.reposts,
            places:  t.places,
        }),
        tab_settings: v.tab_settings.map(|t| proto::TabSettings {
            post_window: (match t.post_window {
                crate::domain::value_object::PostWindow::All => proto::PostWindow::All,
                crate::domain::value_object::PostWindow::SixMonths => proto::PostWindow::SixMonths,
                crate::domain::value_object::PostWindow::OneMonth => proto::PostWindow::OneMonth,
                crate::domain::value_object::PostWindow::ThreeDays => proto::PostWindow::ThreeDays,
            }) as i32,
            show_likes:   t.show_likes,
            show_saved:   t.show_saved,
            show_reposts: t.show_reposts,
            show_places:  t.show_places,
        }),
        comment_filters: v.comment_filters.map(|f| proto::CommentFilters {
            hidden_words:     f.hidden_words,
            filter_offensive: f.filter_offensive,
        }),
        discovery_settings: v.discovery.map(|d| proto::DiscoverySettings {
            activity_status:  d.activity_status,
            read_receipts:    d.read_receipts,
            by_phone:         d.by_phone,
            by_email:         d.by_email,
            by_handle_search: d.by_handle_search,
            by_qr:            d.by_qr,
            in_suggestions:   d.in_suggestions,
        }),
    }
}

fn audience_to_proto(a: InteractionAudience) -> i32 {
    (match a {
        InteractionAudience::Everyone => proto::InteractionAudience::Everyone,
        InteractionAudience::Followers => proto::InteractionAudience::Followers,
        InteractionAudience::Mutuals => proto::InteractionAudience::Mutuals,
        InteractionAudience::NoOne => proto::InteractionAudience::NoOne,
    }) as i32
}

fn audience_from_proto(v: i32) -> Result<InteractionAudience, Status> {
    match proto::InteractionAudience::try_from(v) {
        Ok(proto::InteractionAudience::Everyone) => Ok(InteractionAudience::Everyone),
        Ok(proto::InteractionAudience::Followers) => Ok(InteractionAudience::Followers),
        Ok(proto::InteractionAudience::Mutuals) => Ok(InteractionAudience::Mutuals),
        Ok(proto::InteractionAudience::NoOne) => Ok(InteractionAudience::NoOne),
        _ => Err(Status::invalid_argument("every interaction audience must be set")),
    }
}

fn interaction_to_proto(s: InteractionSettings) -> proto::InteractionSettings {
    proto::InteractionSettings {
        comments: audience_to_proto(s.comments),
        mentions: audience_to_proto(s.mentions),
        messages: audience_to_proto(s.messages),
        allow_downloads: s.allow_downloads,
        show_like_counts: s.show_like_counts,
        allow_remix: Some(s.allow_remix),
        allow_sound_reuse: Some(s.allow_sound_reuse),
        limit: s.limit.map(|l| proto::InteractionLimit {
            audience: (match l.audience {
                crate::domain::value_object::LimitAudience::NonFollowers => proto::LimitAudience::NonFollowers,
                crate::domain::value_object::LimitAudience::RecentFollowers => proto::LimitAudience::RecentFollowers,
            }) as i32,
            until_ms: l.until.timestamp_millis(),
        }),
    }
}

fn summary_to_proto(s: ProfileSummary) -> proto::ProfileSummaryView {
    proto::ProfileSummaryView {
        profile_id:   s.profile_id.as_str(),
        handle:       s.handle,
        display_name: s.display_name,
        avatar_url:   s.avatar_url.unwrap_or_default(),
        avatar_asset_id: s.avatar_asset_id.map(|a| a.as_str()).unwrap_or_default(),
        avatar_frame_id: s.avatar_frame_id.unwrap_or_default(),
        avatar:       None,
        profile_kind: profile_kind_str_to_i32(&s.profile_kind),
        visibility:   profile_visibility_str_to_i32(&s.visibility),
        status:       profile_status_str_to_i32(&s.status),
    }
}

// ── Enum converters ───────────────────────────────────────────────────────────

fn profile_kind_str_to_i32(s: &str) -> i32 {
    match s {
        "personal"     => 1,
        "professional" => 2,
        "brand"        => 3,
        "bot"          => 4,
        _              => 0,
    }
}

fn profile_kind_i32_to_str(v: i32) -> Option<&'static str> {
    match v {
        1 => Some("personal"),
        2 => Some("professional"),
        3 => Some("brand"),
        4 => Some("bot"),
        _ => None,
    }
}

fn profile_visibility_str_to_i32(s: &str) -> i32 {
    match s {
        "public"  => 1,
        "private" => 2,
        _         => 0,
    }
}

fn profile_visibility_i32_to_str(v: i32) -> Option<&'static str> {
    match v {
        1 => Some("public"),
        2 => Some("private"),
        _ => None,
    }
}

fn profile_status_str_to_i32(s: &str) -> i32 {
    match s {
        "active"    => 1,
        "suspended" => 2,
        "hidden"    => 3,
        "deleted"   => 4,
        _           => 0,
    }
}

/// Creator (professional) and business (brand) accounts are for holders 18
/// and over; a 13–17 holder keeps a personal profile.
fn account_type_allowed(kind: crate::domain::value_object::ProfileKind, minor: bool) -> bool {
    use crate::domain::value_object::ProfileKind;
    !minor || kind == ProfileKind::Personal
}

fn verification_to_proto(profile_id: &str, r: crate::domain::entity::VerificationRequest) -> proto::VerificationRequestView {
    use crate::domain::entity::VerificationStatus;
    proto::VerificationRequestView {
        profile_id:      profile_id.to_owned(),
        category:        verification_kind_str_to_i32(r.category.as_str()),
        documents:       r.documents,
        status:          (match r.status {
            VerificationStatus::Pending => proto::VerificationRequestStatus::Pending,
            VerificationStatus::Approved => proto::VerificationRequestStatus::Approved,
            VerificationStatus::Rejected => proto::VerificationRequestStatus::Rejected,
        }) as i32,
        reason:          r.reason.unwrap_or_default(),
        submitted_at_ms: r.submitted_at.timestamp_millis(),
        decided_at_ms:   r.decided_at.map(|t| t.timestamp_millis()).unwrap_or_default(),
        private_document_ids: r.private_documents,
    }
}

fn verification_kind_str_to_i32(s: &str) -> i32 {
    match s {
        "official" => 1,
        "notable"  => 2,
        "business" => 3,
        _          => 0,
    }
}

fn verification_kind_i32_to_str(v: i32) -> Option<&'static str> {
    match v {
        1 => Some("official"),
        2 => Some("notable"),
        3 => Some("business"),
        _ => None,
    }
}

fn masking_reason_i32_to_str(v: i32) -> Option<&'static str> {
    match v {
        1 => Some("account_suspended"),
        2 => Some("account_deleted"),
        3 => Some("content_policy_violation"),
        _ => None,
    }
}

// ── Error mapping ─────────────────────────────────────────────────────────────

fn slots_to_proto(state: crate::application::profile_slots::SlotState) -> proto::ProfileSlots {
    proto::ProfileSlots {
        slots:      state.slots,
        used:       state.used,
        max_slots:  state.max_slots,
        next_price: state.next_price.unwrap_or_default(),
    }
}

/// A slot call's error (#916): `PERMISSION_DENIED` for a minor, `UNAVAILABLE`
/// when the wallet cannot answer; the stable code in `x-error-code`.
fn profile_error_to_status(err: crate::error::ProfileError) -> Status {
    use error::AppError as _;
    let message = err.to_string();
    let mut status = match err.http_status().as_u16() {
        403 => Status::permission_denied(message),
        404 => Status::not_found(message),
        422 => Status::invalid_argument(message),
        502 | 503 => Status::unavailable(message),
        _ => Status::internal(message),
    };
    if let Ok(code) = err.error_code().parse() {
        status.metadata_mut().insert("x-error-code", code);
    }
    status
}

pub fn cqrs_error_to_status(err: cqrs::error::CqrsError) -> Status {
    use cqrs::error::CqrsError;
    match err {
        CqrsError::HandlerNotFound { type_name } => {
            Status::unimplemented(format!("no handler registered for {type_name}"))
        }
        CqrsError::DuplicateRegistration { type_name } => {
            Status::internal(format!("duplicate handler for {type_name}"))
        }
        CqrsError::Handler(boxed) => {
            use error::AppError as _;
            let msg = boxed.to_string();
            let retryable = boxed.is_retryable();
            match boxed.http_status().as_u16() {
                403 => Status::permission_denied(msg),
                404 => Status::not_found(msg),
                409 if retryable => Status::aborted(msg),
                409 => Status::already_exists(msg),
                400 | 422 => Status::failed_precondition(msg),
                503 | 502 => Status::unavailable(msg),
                _ => Status::internal(msg),
            }
        }
    }
}

#[cfg(test)]
mod account_type_tests {
    use super::*;
    use crate::domain::value_object::ProfileKind;

    #[test]
    fn creator_and_business_accounts_are_18_plus() {
        assert!(account_type_allowed(ProfileKind::Brand, false));
        assert!(account_type_allowed(ProfileKind::Professional, false));
        assert!(!account_type_allowed(ProfileKind::Brand, true));
        assert!(!account_type_allowed(ProfileKind::Professional, true));
        assert!(account_type_allowed(ProfileKind::Personal, true));
    }
}

/// #837: the staff verification queue and decision are mesh calls carrying a
/// staff token that holds `verification:review`.
#[cfg(test)]
mod staff_gate_tests {
    use std::sync::Arc;

    use cqrs::command::CommandBusBuilder;
    use cqrs::query::QueryBusBuilder;
    use tonic::Code;

    use super::*;

    struct Tokens;

    impl edge::StaffVerifier for Tokens {
        fn verify<'a>(&'a self, token: &'a str) -> edge::StaffVerification<'a> {
            let permissions: Option<&[&str]> = match token {
                "reviewer" => Some(&[edge::VERIFICATION_REVIEW]),
                "member" => Some(&[]),
                _ => None,
            };
            let principal = permissions.map(|permissions| {
                let raw: auth_context::OidcClaims =
                    serde_json::from_value(serde_json::json!({ "sub": "staff-1", "exp": 4_102_444_800_i64 })).unwrap();
                auth_context::CurrentPrincipal {
                    user_id: auth_context::PrincipalId::new("staff-1"),
                    tenant_id: None,
                    permissions: permissions.iter().map(|p| auth_context::Permission::new(*p)).collect(),
                    raw_claims: raw,
                }
            });
            Box::pin(async move { principal })
        }
    }

    fn with_token<T>(message: T, token: Option<&str>) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(token) = token {
            request.metadata_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
        }
        request
    }

    #[tokio::test]
    async fn the_review_rpcs_need_a_reviewer_token() {
        let handler = ProfileServiceHandler::new(
            Arc::new(CommandBusBuilder::new().build()),
            Arc::new(QueryBusBuilder::new().build()),
        )
        .with_staff_gate(edge::StaffGate::new(Arc::new(Tokens)));
        let list = || proto::ListPendingVerificationRequestsRequest { limit: 10, page_token: String::new() };
        let decide = || proto::DecideVerificationRequestRequest {
            profile_id: Uuid::now_v7().to_string(),
            approve:    true,
            reason:     String::new(),
        };

        for (token, code) in [(None, Code::Unauthenticated), (Some("forged"), Code::Unauthenticated), (Some("member"), Code::PermissionDenied)] {
            let listed = handler.list_pending_verification_requests(with_token(list(), token)).await.unwrap_err();
            assert_eq!(listed.code(), code, "{token:?}");
            let decided = handler.decide_verification_request(with_token(decide(), token)).await.unwrap_err();
            assert_eq!(decided.code(), code, "{token:?}");
        }

        // A reviewer gets through the gate (the empty bus then answers).
        let reviewer = handler.list_pending_verification_requests(with_token(list(), Some("reviewer"))).await.unwrap_err();
        assert!(!matches!(reviewer.code(), Code::PermissionDenied | Code::Unauthenticated));
    }
}

/// #907: avatars and banners carry their asset's delivery, fail-open.
#[cfg(test)]
mod media_delivery_tests {
    use async_trait::async_trait;
    use media_api::DeliveredMedia;

    use super::*;

    struct FakeMedia(Result<Vec<DeliveredMedia>, Status>);

    #[async_trait]
    impl MediaDelivery for FakeMedia {
        async fn deliver(&self, _asset_ids: &[String]) -> Result<Vec<DeliveredMedia>, Status> {
            self.0.clone()
        }
    }

    fn delivered(id: &str) -> DeliveredMedia {
        DeliveredMedia { asset_id: id.to_owned(), blurhash: format!("bh-{id}"), ..Default::default() }
    }

    #[tokio::test]
    async fn each_image_with_an_asset_gets_its_delivery() {
        let media = FakeMedia(Ok(vec![delivered("a")]));
        let (mut avatar, mut banner, mut none) = (None, None, None);
        deliver_media(Some(&media), vec![("a", &mut avatar), ("unknown", &mut banner), ("", &mut none)]).await;
        assert_eq!(avatar.map(|m| m.blurhash), Some("bh-a".to_owned()));
        assert!(banner.is_none() && none.is_none());
    }

    #[tokio::test]
    async fn a_media_outage_leaves_the_urls_alone() {
        let media = FakeMedia(Err(Status::unavailable("down")));
        let mut avatar = None;
        deliver_media(Some(&media), vec![("a", &mut avatar)]).await;
        assert!(avatar.is_none());
    }

    #[test]
    fn a_cached_view_from_before_reads_without_assets() {
        let mut cached = serde_json::to_value(ProfileView::from(&sample())).unwrap();
        let fields = cached.as_object_mut().unwrap();
        fields.remove("avatar_asset_id");
        fields.remove("banner_asset_id");
        let view: ProfileView = serde_json::from_value(cached).unwrap();
        assert_eq!(view.avatar_asset_id, None);
        assert_eq!(profile_view_to_proto(view).avatar_asset_id, "");
    }

    fn sample() -> crate::domain::aggregate::Profile {
        use crate::domain::aggregate::ProfileCreateParams;
        use crate::domain::value_object::*;
        crate::domain::aggregate::Profile::create(ProfileCreateParams {
            account_id: AccountId::try_from(Uuid::now_v7().to_string().as_str()).unwrap(),
            handle: Handle::new("alicehandle").unwrap(),
            display_name: DisplayName::new("Alice").unwrap(),
            bio: None,
            avatar_url: None,
            banner_url: None,
            profile_kind: ProfileKind::try_from("personal").unwrap(),
            locale: Locale::new("en-US").unwrap(),
            visibility: ProfileVisibility::Public,
            interaction: InteractionSettings::default(),
            location: LocationSettings::default(),
            discovery: DiscoverySettings::default(),
            feed: FeedSettings::default(),
            correlation_id: Uuid::now_v7(),
        })
    }
}
