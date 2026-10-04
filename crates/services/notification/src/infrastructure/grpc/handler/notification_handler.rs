use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use tonic::{Request, Response, Status};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;
use uuid::Uuid;

use cqrs::{CommandBus, Envelope, QueryBus};

use transport::grpc::edge;
use crate::application::command::mark_read::{MarkAllReadCommand, MarkReadCommand};
use crate::application::port::{NotificationSummary, StreamRegistry};
use crate::application::query::{
    get_unread_count::GetUnreadCountQuery,
    list_notifications::ListNotificationsQuery,
};
use crate::application::command::push_settings::{
    RegisterDeviceCommand, UnregisterDeviceCommand, UpdatePreferencesCommand,
};
use crate::application::query::push_settings::{GetPreferencesQuery, PushTargets, ResolvePushTargetsQuery};
use crate::domain::device::{DevicePlatform, PushEnvironment};
use crate::domain::preferences::{NotificationPreferences, PushCategory, QuietHours};
use crate::domain::value_object::ProfileId;

// ── Proto inclusion ───────────────────────────────────────────────────────────

pub use notification_api as proto;

pub use proto::notification_service_server::NotificationServiceServer;

// ── Handler struct ────────────────────────────────────────────────────────────

pub struct NotificationServiceHandler<CB, QB, SR>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
    SR: StreamRegistry + Send + Sync + 'static,
{
    command_bus:     CB,
    query_bus:       QB,
    stream_registry: Arc<SR>,
}

impl<CB, QB, SR> NotificationServiceHandler<CB, QB, SR>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
    SR: StreamRegistry + Send + Sync + 'static,
{
    pub fn new(command_bus: CB, query_bus: QB, stream_registry: Arc<SR>) -> Self {
        Self { command_bus, query_bus, stream_registry }
    }
}

// ── RPC implementations ───────────────────────────────────────────────────────

impl<CB, QB, SR> NotificationServiceHandler<CB, QB, SR>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
    SR: StreamRegistry + Send + Sync + 'static,
{
    pub async fn list_notifications(
        &self,
        request: Request<proto::ListNotificationsRequest>,
    ) -> Result<Response<proto::ListNotificationsResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let query = ListNotificationsQuery {
            profile_id: req.profile_id,
            limit:      req.limit,
            page_token: Some(req.page_token).filter(|s| !s.is_empty()),
        };

        let page = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::ListNotificationsResponse {
            notifications:   page.notifications.into_iter().map(summary_to_proto).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
            read_horizon_ms: page.read_horizon_ms,
        }))
    }

    pub async fn get_unread_count(
        &self,
        request: Request<proto::GetUnreadCountRequest>,
    ) -> Result<Response<proto::GetUnreadCountResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let query = GetUnreadCountQuery { profile_id: request.into_inner().profile_id };

        let count: i64 = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::GetUnreadCountResponse { unread_count: count }))
    }

    pub async fn mark_read(
        &self,
        request: Request<proto::MarkReadRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = MarkReadCommand {
            profile_id:      req.profile_id,
            notification_id: req.notification_id,
            created_at_ms:   req.created_at_ms,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| ok_response())
            .map_err(cqrs_to_status)
    }

    pub async fn mark_all_read(
        &self,
        request: Request<proto::MarkAllReadRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let cmd = MarkAllReadCommand { profile_id: request.into_inner().profile_id };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| ok_response())
            .map_err(cqrs_to_status)
    }

    pub async fn stream_notifications(
        &self,
        request: Request<proto::StreamNotificationsRequest>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<proto::StreamNotificationsResponse, Status>> + Send + 'static>>>,
        Status,
    > {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let profile_id = ProfileId::try_from(request.into_inner().profile_id.as_str())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let rx     = self.stream_registry.subscribe(&profile_id);
        let stream = BroadcastStream::new(rx).filter_map(|result| {
            match result {
                Ok(payload) => {
                    let view = payload_to_proto(&payload);
                    Some(Ok(proto::StreamNotificationsResponse {
                        notification: Some(view),
                    }))
                }
                Err(_lagged) => {
                    // Receiver fell behind — the client must re-poll ListNotifications.
                    // Terminating the stream signals the client to reconnect.
                    Some(Err(Status::data_loss(
                        "stream lagged: re-poll ListNotifications to recover missed notifications"
                    )))
                }
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }
}

// ── Push devices and preferences (#654) ───────────────────────────────────────

impl<CB, QB, SR> NotificationServiceHandler<CB, QB, SR>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
    SR: StreamRegistry + Send + Sync + 'static,
{
    pub async fn register_device(
        &self,
        request: Request<proto::RegisterDeviceRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let principal = edge::principal(&request);
        let account_id = principal.map(|p| p.account_id().to_owned()).unwrap_or_default();
        let minor = principal.is_some_and(|p| p.is_minor());
        let req = request.into_inner();
        let cmd = RegisterDeviceCommand {
            profile_id:  req.profile_id,
            account_id,
            device_id:   req.device_id,
            token:       req.token,
            platform:    platform_from_proto(req.platform)?,
            environment: environment_from_proto(req.environment)?,
            timezone:    Some(req.timezone).filter(|z| !z.is_empty()),
            minor,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| ok_response())
            .map_err(cqrs_to_status)
    }

    pub async fn unregister_device(
        &self,
        request: Request<proto::UnregisterDeviceRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = UnregisterDeviceCommand { profile_id: req.profile_id, device_id: req.device_id };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| ok_response())
            .map_err(cqrs_to_status)
    }

    pub async fn get_notification_preferences(
        &self,
        request: Request<proto::GetNotificationPreferencesRequest>,
    ) -> Result<Response<proto::NotificationPreferences>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let minor = edge::principal(&request).is_some_and(|p| p.is_minor());
        self.preferences_of(request.into_inner().profile_id, minor).await
    }

    pub async fn update_notification_preferences(
        &self,
        request: Request<proto::UpdateNotificationPreferencesRequest>,
    ) -> Result<Response<proto::NotificationPreferences>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let minor = edge::principal(&request).is_some_and(|p| p.is_minor());
        let req = request.into_inner();
        let mut push = Vec::with_capacity(req.categories.len());
        let mut email = Vec::with_capacity(req.categories.len());
        for c in &req.categories {
            let category = category_from_proto(c.category)?;
            push.push((category, c.push));
            email.push((category, c.email));
        }
        let pause = match req.paused_until_ms {
            None => None,
            Some(0) => Some(None),
            Some(ms) => Some(Some(
                chrono::DateTime::from_timestamp_millis(ms)
                    .ok_or_else(|| Status::invalid_argument("paused_until_ms out of range"))?,
            )),
        };
        let quiet_hours = req.quiet_hours.map(quiet_hours_from_proto).transpose()?;
        let cmd = UpdatePreferencesCommand {
            profile_id: req.profile_id.clone(),
            minor,
            push,
            email,
            pause,
            quiet_hours,
            timezone: Some(req.timezone).filter(|z| !z.is_empty()),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map_err(cqrs_to_status)?;
        self.preferences_of(req.profile_id, minor).await
    }

    /// Mesh-only (absent from the edge policy): the push sender asks.
    pub async fn resolve_push_targets(
        &self,
        request: Request<proto::ResolvePushTargetsRequest>,
    ) -> Result<Response<proto::ResolvePushTargetsResponse>, Status> {
        let req = request.into_inner();
        let query = ResolvePushTargetsQuery {
            profile_id: req.profile_id,
            category:   category_from_proto(req.category)?,
        };
        let targets: PushTargets = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::ResolvePushTargetsResponse {
            allowed: targets.allowed,
            devices: targets
                .devices
                .into_iter()
                .map(|d| proto::PushDevice {
                    device_id:   d.device_id,
                    token:       d.token,
                    platform:    platform_to_proto(d.platform) as i32,
                    environment: environment_to_proto(d.environment) as i32,
                })
                .collect(),
        }))
    }

    async fn preferences_of(&self, profile_id: String, minor: bool) -> Result<Response<proto::NotificationPreferences>, Status> {
        let preferences: NotificationPreferences = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetPreferencesQuery { profile_id, minor }))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(preferences_to_proto(&preferences)))
    }
}

fn category_from_proto(value: i32) -> Result<PushCategory, Status> {
    use proto::PushCategory as P;
    Ok(match P::try_from(value) {
        Ok(P::Likes) => PushCategory::Likes,
        Ok(P::Comments) => PushCategory::Comments,
        Ok(P::Mentions) => PushCategory::Mentions,
        Ok(P::NewFollowers) => PushCategory::NewFollowers,
        Ok(P::FollowRequests) => PushCategory::FollowRequests,
        Ok(P::Messages) => PushCategory::Messages,
        Ok(P::FollowedPosts) => PushCategory::FollowedPosts,
        Ok(P::PlacesNearby) => PushCategory::PlacesNearby,
        Ok(P::Wallet) => PushCategory::Wallet,
        _ => return Err(Status::invalid_argument(format!("a push category is required (got {value})"))),
    })
}

fn category_to_proto(category: PushCategory) -> proto::PushCategory {
    use proto::PushCategory as P;
    match category {
        PushCategory::Likes => P::Likes,
        PushCategory::Comments => P::Comments,
        PushCategory::Mentions => P::Mentions,
        PushCategory::NewFollowers => P::NewFollowers,
        PushCategory::FollowRequests => P::FollowRequests,
        PushCategory::Messages => P::Messages,
        PushCategory::FollowedPosts => P::FollowedPosts,
        PushCategory::PlacesNearby => P::PlacesNearby,
        PushCategory::Wallet => P::Wallet,
    }
}

fn platform_from_proto(value: i32) -> Result<DevicePlatform, Status> {
    match proto::DevicePlatform::try_from(value) {
        Ok(proto::DevicePlatform::Ios) => Ok(DevicePlatform::Ios),
        Ok(proto::DevicePlatform::Android) => Ok(DevicePlatform::Android),
        _ => Err(Status::invalid_argument("platform is required")),
    }
}

fn platform_to_proto(platform: DevicePlatform) -> proto::DevicePlatform {
    match platform {
        DevicePlatform::Ios => proto::DevicePlatform::Ios,
        DevicePlatform::Android => proto::DevicePlatform::Android,
    }
}

fn environment_from_proto(value: i32) -> Result<PushEnvironment, Status> {
    match proto::PushEnvironment::try_from(value) {
        Ok(proto::PushEnvironment::Sandbox) => Ok(PushEnvironment::Sandbox),
        Ok(proto::PushEnvironment::Production) => Ok(PushEnvironment::Production),
        _ => Err(Status::invalid_argument("environment is required")),
    }
}

fn environment_to_proto(environment: PushEnvironment) -> proto::PushEnvironment {
    match environment {
        PushEnvironment::Sandbox => proto::PushEnvironment::Sandbox,
        PushEnvironment::Production => proto::PushEnvironment::Production,
    }
}

/// `None` turns quiet hours off.
fn quiet_hours_from_proto(q: proto::QuietHours) -> Result<Option<QuietHours>, Status> {
    if !q.enabled {
        return Ok(None);
    }
    let minute = |m: i32| u16::try_from(m).ok();
    match (minute(q.start_minute), minute(q.end_minute)) {
        (Some(start), Some(end)) => QuietHours::new(start, end)
            .map(Some)
            .ok_or_else(|| Status::invalid_argument("quiet hours minutes are 0–1439")),
        _ => Err(Status::invalid_argument("quiet hours minutes are 0–1439")),
    }
}

fn preferences_to_proto(p: &NotificationPreferences) -> proto::NotificationPreferences {
    proto::NotificationPreferences {
        categories: PushCategory::ALL
            .iter()
            .map(|c| proto::CategoryChannels {
                category: category_to_proto(*c) as i32,
                push:     p.push_on(*c),
                email:    p.email_on(*c),
            })
            .collect(),
        paused_until_ms: p.paused_until.map(|t| t.timestamp_millis()).unwrap_or_default(),
        quiet_hours: Some(match p.quiet_hours {
            Some(q) => proto::QuietHours {
                enabled:      true,
                start_minute: i32::from(q.start_minute),
                end_minute:   i32::from(q.end_minute),
            },
            None => proto::QuietHours::default(),
        }),
        timezone: p.timezone.clone().unwrap_or_default(),
    }
}

// ── Proto trait implementation ────────────────────────────────────────────────

#[tonic::async_trait]
impl<CB, QB, SR> proto::notification_service_server::NotificationService
    for NotificationServiceHandler<CB, QB, SR>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
    SR: StreamRegistry + Send + Sync + 'static,
{
    type StreamNotificationsStream =
        Pin<Box<dyn Stream<Item = Result<proto::StreamNotificationsResponse, Status>> + Send + 'static>>;

    async fn list_notifications(
        &self,
        request: Request<proto::ListNotificationsRequest>,
    ) -> Result<Response<proto::ListNotificationsResponse>, Status> {
        self.list_notifications(request).await
    }

    async fn get_unread_count(
        &self,
        request: Request<proto::GetUnreadCountRequest>,
    ) -> Result<Response<proto::GetUnreadCountResponse>, Status> {
        self.get_unread_count(request).await
    }

    async fn mark_read(
        &self,
        request: Request<proto::MarkReadRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.mark_read(request).await
    }

    async fn mark_all_read(
        &self,
        request: Request<proto::MarkAllReadRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.mark_all_read(request).await
    }

    async fn stream_notifications(
        &self,
        request: Request<proto::StreamNotificationsRequest>,
    ) -> Result<Response<Self::StreamNotificationsStream>, Status> {
        self.stream_notifications(request).await
    }

    async fn register_device(
        &self,
        request: Request<proto::RegisterDeviceRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.register_device(request).await
    }

    async fn unregister_device(
        &self,
        request: Request<proto::UnregisterDeviceRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.unregister_device(request).await
    }

    async fn get_notification_preferences(
        &self,
        request: Request<proto::GetNotificationPreferencesRequest>,
    ) -> Result<Response<proto::NotificationPreferences>, Status> {
        self.get_notification_preferences(request).await
    }

    async fn update_notification_preferences(
        &self,
        request: Request<proto::UpdateNotificationPreferencesRequest>,
    ) -> Result<Response<proto::NotificationPreferences>, Status> {
        self.update_notification_preferences(request).await
    }

    async fn resolve_push_targets(
        &self,
        request: Request<proto::ResolvePushTargetsRequest>,
    ) -> Result<Response<proto::ResolvePushTargetsResponse>, Status> {
        self.resolve_push_targets(request).await
    }
}

// ── Conversion helpers ────────────────────────────────────────────────────────

fn ok_response() -> Response<proto::CommandResponse> {
    Response::new(proto::CommandResponse { success: true, message: String::new() })
}

fn summary_to_proto(s: NotificationSummary) -> proto::NotificationView {
    proto::NotificationView {
        notification_id:   s.notification_id.to_string(),
        target_profile_id: s.target_profile_id.to_string(),
        sender_profile_id: s.sender_profile_id.to_string(),
        sample_sender_ids: s.sample_sender_ids.iter().map(|u| u.to_string()).collect(),
        sender_count:      s.sender_count,
        kind:              s.kind.as_tinyint() as i32,
        subject_kind:      s.subject_kind.as_tinyint() as i32,
        subject_id:        s.subject_id.to_string(),
        created_at_ms:     s.created_at.timestamp_millis(),
        is_read:           s.is_read,
    }
}

fn payload_to_proto(
    p: &crate::application::port::stream_registry::NotificationPayload,
) -> proto::NotificationView {
    proto::NotificationView {
        notification_id:   p.notification_id.to_string(),
        target_profile_id: p.target_profile_id.to_string(),
        sender_profile_id: p.sender_profile_id.to_string(),
        sample_sender_ids: p.sample_sender_ids.iter().map(|u| u.to_string()).collect(),
        sender_count:      p.sender_count,
        kind:              p.kind.as_tinyint() as i32,
        subject_kind:      p.subject_kind.as_tinyint() as i32,
        subject_id:        p.subject_id.to_string(),
        created_at_ms:     p.created_at_ms,
        is_read:           false,
    }
}

// ── Error mapping ─────────────────────────────────────────────────────────────

pub fn cqrs_to_status(err: cqrs::error::CqrsError) -> Status {
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
            let msg       = boxed.to_string();
            let retryable = boxed.is_retryable();
            match boxed.http_status().as_u16() {
                403       => Status::permission_denied(msg),
                404       => Status::not_found(msg),
                409 if retryable => Status::aborted(msg),
                409       => Status::already_exists(msg),
                400 | 422 => Status::failed_precondition(msg),
                503 | 502 => Status::unavailable(msg),
                _         => Status::internal(msg),
            }
        }
    }
}
