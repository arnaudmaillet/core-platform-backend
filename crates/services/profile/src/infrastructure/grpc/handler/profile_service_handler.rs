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
}

impl<CB, QB> ProfileServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub fn new(command_bus: CB, query_bus: QB) -> Self {
        Self { command_bus, query_bus }
    }

    fn ok_cmd(profile_id: &str) -> Response<proto::CommandResponse> {
        Response::new(proto::CommandResponse {
            success:    true,
            profile_id: profile_id.to_owned(),
        })
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
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_cmd(&req.account_id))
            .map_err(cqrs_error_to_status)
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
        let req = request.into_inner();
        let cmd = ChangeHandleCommand {
            profile_id: req.profile_id.clone(),
            new_handle: req.new_handle,
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
        let cmd = RequestVerificationCommand { profile_id: req.profile_id.clone(), category, documents: req.documents };
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

    /// Staff queue (mesh-only, absent from the edge policy).
    pub async fn list_pending_verification_requests(
        &self,
        request: Request<proto::ListPendingVerificationRequestsRequest>,
    ) -> Result<Response<proto::ListPendingVerificationRequestsResponse>, Status> {
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

    /// Staff decision (mesh-only, absent from the edge policy).
    pub async fn decide_verification_request(
        &self,
        request: Request<proto::DecideVerificationRequestRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        use crate::application::command::DecideVerificationCommand;
        let req = request.into_inner();
        let cmd = DecideVerificationCommand {
            profile_id: req.profile_id.clone(),
            approve:    req.approve,
            reason:     Some(req.reason).filter(|r| !r.is_empty()),
        };
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
        let req = request.into_inner();
        let query = GetProfileByIdQuery { profile_id: req.profile_id, viewer };
        let view: Option<ProfileView> = self.query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;

        view.map(profile_view_to_proto)
            .map(Response::new)
            .ok_or_else(|| Status::not_found("profile not found"))
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
        let query = ResolveShareTokenQuery { token: request.into_inner().token, viewer };
        let view: Option<ProfileView> =
            self.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.map_err(cqrs_error_to_status)?;
        view.map(profile_view_to_proto).map(Response::new).ok_or_else(|| Status::not_found("profile not found"))
    }

    pub async fn get_profile_by_handle(
        &self,
        request: Request<proto::GetProfileByHandleRequest>,
    ) -> Result<Response<proto::ProfileView>, Status> {
        let viewer = viewer_of(&request);
        let req = request.into_inner();
        let query = GetProfileByHandleQuery { handle: req.handle, viewer };
        let view: Option<ProfileView> = self.query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;

        view.map(profile_view_to_proto)
            .map(Response::new)
            .ok_or_else(|| Status::not_found("profile not found"))
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

        Ok(Response::new(proto::ListProfilesByAccountResponse {
            profiles:        summaries.into_iter().map(summary_to_proto).collect(),
            next_page_token: next_page_token.unwrap_or_default(),
        }))
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
