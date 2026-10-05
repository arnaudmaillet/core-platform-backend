use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::Utc;
use futures::Stream;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope, QueryBus};

use transport::grpc::edge;
use crate::application::command::{
    CreateConversationCommand, DirectMessaging, InviteMemberCommand, JoinAsMemberCommand, MarkReadCommand,
    SendMessageCommand, SendMessages, SubscribeCommand, ToggleVisibilityCommand, UnsubscribeCommand,
};
use crate::application::port::{
    ConversationRepository, MemberRepository, MessageSummary, PresenceSettingsStore, PresenceStore,
    ReceiptStore, RoutingRegistry,
};
use crate::application::query::{
    GetHistoryQuery, InboxPage, ListConversationsByMemberQuery, ListInboxQuery, ListMembersQuery,
    ListSubscriptionsQuery, MemberConversation, MemberView as QueryMemberView,
};
use crate::application::port::Folder;
use crate::domain::value_object::{ContentType, ConversationId, MessageId, ProfileId};
use crate::error::ChatError;
use crate::infrastructure::cache::keys::audience_shard_for;
use crate::infrastructure::routing::{Fanout, PlaneAttach, PlaneEvent};
use crate::infrastructure::streaming::ConversationBroadcastRegistry;

// ── Proto inclusion ─────────────────────────────────────────────────────────

pub use chat_api as proto;

pub use proto::chat_service_server::ChatServiceServer;

// ── Handler ─────────────────────────────────────────────────────────────────

/// Runtime knobs threaded into the handler from [`ChatConfig`](crate::config::ChatConfig).
#[derive(Clone, Copy)]
pub struct StreamingParams {
    pub presence_ttl_secs:    u64,
    pub typing_ttl_secs:      u64,
    pub audience_shard_count: u16,
    /// Liveness TTL for the audience-shard routing registry (pod heartbeat window).
    pub audience_ttl_secs:    u64,
}

pub struct ChatServiceHandler<CB, QB> {
    command_bus:       CB,
    query_bus:         QB,
    fanout:            Arc<dyn Fanout>,
    attach:            Arc<dyn PlaneAttach>,
    member_registry:   Arc<ConversationBroadcastRegistry>,
    audience_registry: Arc<ConversationBroadcastRegistry>,
    presence:          Arc<dyn PresenceStore>,
    receipt:           Arc<dyn ReceiptStore>,
    routing:           Arc<dyn RoutingRegistry>,
    conversation_repo: Arc<dyn ConversationRepository>,
    member_repo:       Arc<dyn MemberRepository>,
    presence_settings: Arc<dyn PresenceSettingsStore>,
    /// Sends (and says whether a message is withheld, #656).
    sender:            Arc<dyn SendMessages>,
    /// Direct conversations and message requests (#656).
    direct:            Arc<dyn DirectMessaging>,
    params:            StreamingParams,
}

impl<CB, QB> ChatServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        command_bus:       CB,
        query_bus:         QB,
        fanout:            Arc<dyn Fanout>,
        attach:            Arc<dyn PlaneAttach>,
        member_registry:   Arc<ConversationBroadcastRegistry>,
        audience_registry: Arc<ConversationBroadcastRegistry>,
        presence:          Arc<dyn PresenceStore>,
        receipt:           Arc<dyn ReceiptStore>,
        routing:           Arc<dyn RoutingRegistry>,
        conversation_repo: Arc<dyn ConversationRepository>,
        member_repo:       Arc<dyn MemberRepository>,
        presence_settings: Arc<dyn PresenceSettingsStore>,
        sender:            Arc<dyn SendMessages>,
        direct:            Arc<dyn DirectMessaging>,
        params:            StreamingParams,
    ) -> Self {
        Self {
            command_bus,
            query_bus,
            fanout,
            attach,
            member_registry,
            audience_registry,
            presence,
            receipt,
            routing,
            conversation_repo,
            member_repo,
            presence_settings,
            sender,
            direct,
            params,
        }
    }

    /// Member-Plane access (stream, presence, typing, receipts) is for roster
    /// members only: a non-member gets `PERMISSION_DENIED` on a public
    /// conversation and `NOT_FOUND` — byte-identical to a missing one — on a
    /// private one. Answers whether the member-plane signals flow: not on a
    /// direct conversation whose request is unanswered (#656).
    async fn require_member_signals(&self, conversation_id: &ConversationId, member_id: &ProfileId) -> Result<bool, Status> {
        let (member, conversation) = tokio::join!(
            self.member_repo.find(conversation_id, member_id),
            self.conversation_repo.find(conversation_id),
        );
        let conversation = conversation.map_err(chat_err_to_status)?;
        if member.map_err(chat_err_to_status)?.is_none() {
            let denied = match conversation {
                Some(c) => c.deny_outsider(*member_id),
                None => ChatError::ConversationNotFound { conversation_id: conversation_id.as_str() },
            };
            return Err(chat_err_to_status(denied));
        }
        Ok(conversation.is_none_or(|c| c.signals_flow()))
    }

    // ── Commands ────────────────────────────────────────────────────────────

    async fn open_direct_conversation(
        &self,
        request: Request<proto::OpenDirectConversationRequest>,
    ) -> Result<Response<proto::OpenDirectConversationResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let opened = self
            .direct
            .open(parse_profile(&req.profile_id)?, parse_profile(&req.peer_id)?)
            .await
            .map_err(chat_err_to_status)?;
        Ok(Response::new(proto::OpenDirectConversationResponse {
            conversation_id: opened.conversation_id.as_str(),
            request:         opened.request,
        }))
    }

    async fn respond_to_message_request(
        &self,
        request: Request<proto::RespondToMessageRequestRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        self.direct
            .respond(parse_conversation(&req.conversation_id)?, parse_profile(&req.profile_id)?, req.accept)
            .await
            .map_err(chat_err_to_status)?;
        Ok(ok_response())
    }

    async fn create_conversation(
        &self,
        request: Request<proto::CreateConversationRequest>,
    ) -> Result<Response<proto::CreateConversationResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().owner_id)?;
        let req = request.into_inner();
        let conversation_id = Uuid::now_v7().to_string();

        let cmd = CreateConversationCommand {
            conversation_id: conversation_id.clone(),
            kind:            req.kind,
            owner_id:        req.owner_id,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::CreateConversationResponse { conversation_id }))
    }

    async fn toggle_visibility(
        &self,
        request: Request<proto::ToggleVisibilityRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let cmd = ToggleVisibilityCommand {
            conversation_id: req.conversation_id,
            actor_id:        req.actor_id,
            make_public:     req.make_public,
        };
        self.dispatch_command(cmd).await
    }

    async fn join_as_member(
        &self,
        request: Request<proto::JoinAsMemberRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = JoinAsMemberCommand {
            conversation_id: req.conversation_id,
            profile_id:      req.profile_id,
        };
        self.dispatch_command(cmd).await
    }

    async fn invite_member(
        &self,
        request: Request<proto::InviteMemberRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        // The inviter is the acting identity; the invitee is just a target.
        edge::require_profile(&request, &request.get_ref().inviter_id)?;
        let req = request.into_inner();
        let cmd = InviteMemberCommand {
            conversation_id: req.conversation_id,
            inviter_id:      req.inviter_id,
            invitee_id:      req.invitee_id,
        };
        self.dispatch_command(cmd).await
    }

    async fn subscribe(
        &self,
        request: Request<proto::SubscribeRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().subscriber_id)?;
        let req = request.into_inner();
        let cmd = SubscribeCommand {
            conversation_id: req.conversation_id,
            subscriber_id:   req.subscriber_id,
        };
        self.dispatch_command(cmd).await
    }

    async fn unsubscribe(
        &self,
        request: Request<proto::UnsubscribeRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().subscriber_id)?;
        let req = request.into_inner();
        let cmd = UnsubscribeCommand {
            conversation_id: req.conversation_id,
            subscriber_id:   req.subscriber_id,
        };
        self.dispatch_command(cmd).await
    }

    async fn send_message(
        &self,
        request: Request<proto::SendMessageRequest>,
    ) -> Result<Response<proto::SendMessageResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().sender_id)?;
        let req = request.into_inner();
        let message_id = Uuid::now_v7().to_string();

        let content_type = ContentType::try_from(req.content_type as i8)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let media_ref = non_empty(req.media_ref);
        let reply_to  = non_empty(req.reply_to);

        let cmd = SendMessageCommand {
            message_id:      message_id.clone(),
            conversation_id: req.conversation_id.clone(),
            sender_id:       req.sender_id.clone(),
            content_type:    req.content_type,
            body:            req.body.clone(),
            media_ref:       media_ref.clone(),
            reply_to:        reply_to.clone(),
        };

        // Durable write (and Kafka seam) first.
        let sent = self.sender.send(&cmd).await.map_err(chat_err_to_status)?;

        // Then the best-effort real-time fork (hot-tail cache + both planes).
        // A failure here does not fail the RPC — the message is already durable.
        if let (Ok(conversation_id), Ok(mid), Ok(sender_id)) = (
            ConversationId::try_from(req.conversation_id.as_str()),
            MessageId::try_from(message_id.as_str()),
            ProfileId::try_from(req.sender_id.as_str()),
        ) {
            let now = Utc::now();
            let summary = MessageSummary {
                message_id:   mid.as_uuid(),
                sender_id:    sender_id.as_uuid(),
                content_type,
                body:         req.body,
                media_ref,
                reply_to:     reply_to.and_then(|s| Uuid::parse_str(&s).ok()),
                created_at:   now,
                withheld:     sent.withheld,
            };
            if let Err(e) = self
                .fanout
                .dispatch_message(&conversation_id, &summary, now.timestamp_millis())
                .await
            {
                tracing::warn!(error = %e, "real-time message fan-out failed (message is durable)");
            }
        }

        Ok(Response::new(proto::SendMessageResponse { message_id }))
    }

    async fn mark_read(
        &self,
        request: Request<proto::MarkReadRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().member_id)?;
        let req = request.into_inner();

        // No read receipts before a request is answered (#656): nothing kept,
        // nothing told.
        let conversation_id = parse_conversation(&req.conversation_id)?;
        if !self.require_member_signals(&conversation_id, &parse_profile(&req.member_id)?).await? {
            return Ok(ok_response());
        }

        let cmd = MarkReadCommand {
            conversation_id: req.conversation_id.clone(),
            member_id:       req.member_id.clone(),
            message_id:      req.message_id.clone(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map_err(cqrs_to_status)?;

        // Live receipt mirror + Member-Plane broadcast (best-effort).
        if let (Ok(conversation_id), Ok(member_id), Ok(message_id)) = (
            ConversationId::try_from(req.conversation_id.as_str()),
            ProfileId::try_from(req.member_id.as_str()),
            MessageId::try_from(req.message_id.as_str()),
        ) && self.presence_settings.get_or_withheld(&member_id).await.read_receipts
        {
            let _ = self.receipt.set(&conversation_id, &member_id, message_id).await;
            let event = PlaneEvent::Receipt {
                member_id: member_id.as_str(),
                last_read: message_id.as_str(),
            };
            let _ = self.fanout.dispatch_member_signal(&conversation_id, &event).await;
        }

        Ok(ok_response())
    }

    async fn send_typing(
        &self,
        request: Request<proto::SendTypingRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().member_id)?;
        let req = request.into_inner();
        let conversation_id = parse_conversation(&req.conversation_id)?;
        let member_id       = parse_profile(&req.member_id)?;
        if !self.require_member_signals(&conversation_id, &member_id).await? {
            return Ok(ok_response());
        }
        let now             = Utc::now().timestamp_millis();

        let _ = self
            .presence
            .start_typing(&conversation_id, &member_id, now, self.params.typing_ttl_secs)
            .await;
        let event = PlaneEvent::Typing { member_id: member_id.as_str() };
        let _ = self.fanout.dispatch_member_signal(&conversation_id, &event).await;

        Ok(ok_response())
    }

    async fn heartbeat(
        &self,
        request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().member_id)?;
        let req = request.into_inner();
        let conversation_id = parse_conversation(&req.conversation_id)?;
        let member_id       = parse_profile(&req.member_id)?;
        let signals = self.require_member_signals(&conversation_id, &member_id).await?;
        // Activity status off, or a request unanswered: nothing to announce.
        if !signals || !self.presence_settings.get_or_withheld(&member_id).await.activity_status {
            return Ok(ok_response());
        }
        let now             = Utc::now().timestamp_millis();

        let _ = self
            .presence
            .heartbeat(&conversation_id, &member_id, now, self.params.presence_ttl_secs)
            .await;
        let event = PlaneEvent::Presence { member_id: member_id.as_str(), online: true };
        let _ = self.fanout.dispatch_member_signal(&conversation_id, &event).await;

        Ok(ok_response())
    }

    // ── Queries ───────────────────────────────────────────────────────────────

    async fn get_history(
        &self,
        request: Request<proto::GetHistoryRequest>,
    ) -> Result<Response<proto::GetHistoryResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().requester_id)?;
        let req = request.into_inner();
        let query = GetHistoryQuery {
            conversation_id: req.conversation_id,
            requester_id:    req.requester_id,
            limit:           req.limit,
            page_token:      non_empty(req.page_token),
        };

        let page = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::GetHistoryResponse {
            messages:        page.messages.into_iter().map(summary_to_view).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
        }))
    }

    async fn list_members(
        &self,
        request: Request<proto::ListMembersRequest>,
    ) -> Result<Response<proto::ListMembersResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().requester_id)?;
        let req = request.into_inner();
        let requester_id = req.requester_id.clone();
        let query = ListMembersQuery {
            conversation_id: req.conversation_id,
            requester_id:    req.requester_id,
        };

        let members = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        // Read receipts are shared only by members who share them (#661); a
        // member always sees its own. Unreadable settings withhold them.
        let mut members: Vec<QueryMemberView> = members;
        let ids: Vec<ProfileId> = members.iter().map(|m| ProfileId::from_uuid(m.profile_id)).collect();
        let shared = self.presence_settings.get_many(&ids).await;
        for m in &mut members {
            let own = m.profile_id.to_string() == requester_id;
            let shares = match &shared {
                Ok(map) => map.get(&ProfileId::from_uuid(m.profile_id)).is_none_or(|s| s.read_receipts),
                Err(_) => false,
            };
            if !own && !shares {
                m.last_read = None;
            }
        }

        Ok(Response::new(proto::ListMembersResponse {
            members: members.into_iter().map(member_view_to_proto).collect(),
        }))
    }

    async fn list_subscriptions(
        &self,
        request: Request<proto::ListSubscriptionsRequest>,
    ) -> Result<Response<proto::ListSubscriptionsResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().subscriber_id)?;
        let req = request.into_inner();
        let query = ListSubscriptionsQuery {
            subscriber_id: req.subscriber_id,
            limit:         req.limit,
            page_token:    non_empty(req.page_token),
        };

        let page = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::ListSubscriptionsResponse {
            conversation_ids: page.conversation_ids,
            next_page_token:  page.next_page_token.unwrap_or_default(),
        }))
    }

    /// The caller's inbox, one folder (#656).
    async fn list_inbox(
        &self,
        request: Request<proto::ListInboxRequest>,
    ) -> Result<Response<proto::ListInboxResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let folder = match proto::InboxFolder::try_from(req.folder) {
            Ok(proto::InboxFolder::Requests) => Folder::Requests,
            Ok(proto::InboxFolder::Inbox) => Folder::Inbox,
            Err(_) => return Err(Status::invalid_argument("unknown inbox folder")),
        };
        let query = ListInboxQuery {
            profile_id: req.profile_id,
            folder,
            limit:      if req.limit > 0 { req.limit } else { 20 },
            page_token: non_empty(req.page_token),
        };
        let page: InboxPage = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::ListInboxResponse {
            entries: page
                .items
                .into_iter()
                .map(|item| proto::InboxEntryView {
                    conversation_id:  item.entry.conversation_id.as_str(),
                    kind:             item.entry.kind.as_tinyint() as i32,
                    peer_id:          item.entry.peer.map(|p| p.as_str()).unwrap_or_default(),
                    last_activity_ms: item.entry.activity.timestamp_millis(),
                    last_message:     item.entry.last.map(|l| proto::MessagePreview {
                        message_id:   l.message_id.as_str(),
                        sender_id:    l.sender_id.as_str(),
                        content_type: l.content_type.as_tinyint() as i32,
                        preview:      l.preview,
                    }),
                    unread:           item.unread,
                    request:          item.request,
                })
                .collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
        }))
    }

    /// Mesh only (#653): the conversations a profile is a member of, for the
    /// GDPR export.
    async fn list_conversations_by_member(
        &self,
        request: Request<proto::ListConversationsByMemberRequest>,
    ) -> Result<Response<proto::ListConversationsByMemberResponse>, Status> {
        let req = request.into_inner();
        let limit = req.limit.clamp(1, 500);
        let query = ListConversationsByMemberQuery { member_id: req.member_id, limit, after: non_empty(req.page_token) };
        let memberships: Vec<MemberConversation> = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        let next_page_token = match memberships.last() {
            Some(last) if memberships.len() == limit as usize => last.membership.conversation_id.as_uuid().to_string(),
            _ => String::new(),
        };
        Ok(Response::new(proto::ListConversationsByMemberResponse {
            memberships: memberships
                .into_iter()
                .map(|m| proto::MembershipView {
                    conversation_id: m.membership.conversation_id.as_uuid().to_string(),
                    role:            m.membership.role.as_tinyint() as i32,
                    joined_at_ms:    m.membership.joined_at.timestamp_millis(),
                    kind:            m.kind.map_or(0, |k| k.as_tinyint() as i32),
                })
                .collect(),
            next_page_token,
        }))
    }

    // ── Streaming ─────────────────────────────────────────────────────────────

    async fn stream_conversation(
        &self,
        request: Request<proto::StreamConversationRequest>,
    ) -> Result<Response<StreamConversationStream>, Status> {
        edge::require_profile(&request, &request.get_ref().member_id)?;
        let req = request.into_inner();
        let conversation_id = parse_conversation(&req.conversation_id)?;
        let member_id       = parse_profile(&req.member_id)?;

        // Authorization: Member-Plane access requires roster membership.
        let signals = self.require_member_signals(&conversation_id, &member_id).await?;

        // Pod-level Redis subscription (refcounted) + local fan-out receiver.
        self.attach.attach_member(&conversation_id).await.map_err(chat_err_to_status)?;
        let rx = self.member_registry.subscribe(&conversation_id);

        // Announce presence and keep it alive for the duration of the stream —
        // unless the member turned activity status off (#661), or the request
        // is unanswered (#656).
        let announce = signals && self.presence_settings.get_or_withheld(&member_id).await.activity_status;
        let heartbeat = if announce {
            let now = Utc::now().timestamp_millis();
            let _ = self
                .presence
                .heartbeat(&conversation_id, &member_id, now, self.params.presence_ttl_secs)
                .await;
            let online = PlaneEvent::Presence { member_id: member_id.as_str(), online: true };
            let _ = self.fanout.dispatch_member_signal(&conversation_id, &online).await;
            Some(spawn_presence_heartbeat(
                Arc::clone(&self.presence),
                conversation_id,
                member_id,
                self.params.presence_ttl_secs,
            ))
        } else {
            None
        };

        let guard = MemberStreamGuard {
            attach:          Arc::clone(&self.attach),
            presence:        Arc::clone(&self.presence),
            fanout:          Arc::clone(&self.fanout),
            conversation_id,
            member_id,
            heartbeat,
        };

        // A withheld message is its sender's alone (#656).
        let reader = member_id.as_str();
        let mapped = BroadcastStream::new(rx).filter_map(move |res| match res {
            Ok(event) => match event.as_ref() {
                PlaneEvent::Message(frame) if !frame.visible_to(&reader) => None,
                _ => Some(Ok(proto::StreamConversationResponse { event: Some(plane_to_chat_event(&event)) })),
            },
            Err(_lagged) => Some(Err(Status::data_loss(
                "stream lagged: re-poll GetHistory to recover missed messages",
            ))),
        });

        let stream: StreamConversationStream = Box::pin(GuardedStream {
            inner:  Box::pin(mapped),
            _guard: guard,
        });
        Ok(Response::new(stream))
    }

    async fn stream_public(
        &self,
        request: Request<proto::StreamPublicRequest>,
    ) -> Result<Response<StreamPublicStream>, Status> {
        // The subscriber is the caller's own profile, like every other actor
        // field: it keys the audience shard and refines the private-conversation
        // answer below, so it must not be someone else's.
        edge::require_profile(&request, &request.get_ref().subscriber_id)?;
        let req = request.into_inner();
        let conversation_id = parse_conversation(&req.conversation_id)?;
        let subscriber_id   = parse_profile(&req.subscriber_id)?;

        // Authorization: Audience-Plane access requires a public conversation.
        // On a private one, a member gets the precise "not public"; anyone else
        // gets the same answer as for a missing conversation.
        let conversation = self
            .conversation_repo
            .find(&conversation_id)
            .await
            .map_err(chat_err_to_status)?
            .ok_or_else(|| {
                chat_err_to_status(ChatError::ConversationNotFound {
                    conversation_id: conversation_id.as_str(),
                })
            })?;
        if !conversation.visibility().is_public() {
            let is_member = self
                .member_repo
                .find(&conversation_id, &subscriber_id)
                .await
                .map_err(chat_err_to_status)?
                .is_some();
            return Err(chat_err_to_status(conversation.deny_audience(is_member)));
        }

        let shard = audience_shard_for(subscriber_id.as_uuid(), self.params.audience_shard_count);

        // Refcounted shard subscription; activate the shard in the registry on the
        // first local subscriber so the publisher starts fanning to it.
        let first = self
            .attach
            .attach_audience(&conversation_id, shard)
            .await
            .map_err(chat_err_to_status)?;
        let now = Utc::now().timestamp_millis();
        if first {
            let _ = self
                .routing
                .activate_shard(&conversation_id, shard, now, self.params.audience_ttl_secs)
                .await;
        }

        let rx = self.audience_registry.subscribe(&conversation_id);

        let heartbeat = spawn_shard_heartbeat(
            Arc::clone(&self.routing),
            conversation_id,
            shard,
            self.params.audience_ttl_secs,
        );

        let guard = AudienceStreamGuard {
            attach:  Arc::clone(&self.attach),
            routing: Arc::clone(&self.routing),
            conversation_id,
            shard,
            heartbeat,
        };

        // The Audience Plane carries only message frames; anything else is ignored.
        let mapped = BroadcastStream::new(rx).filter_map(|res| match res {
            Ok(event) => match event.as_ref() {
                PlaneEvent::Message(frame) => Some(Ok(proto::StreamPublicResponse {
                    message: Some(frame_to_view(frame)),
                })),
                _ => None,
            },
            Err(_lagged) => Some(Err(Status::data_loss(
                "stream lagged: re-poll GetHistory to recover missed messages",
            ))),
        });

        let stream: StreamPublicStream = Box::pin(GuardedStream {
            inner:  Box::pin(mapped),
            _guard: guard,
        });
        Ok(Response::new(stream))
    }

    // ── Shared command dispatch (CommandResponse-returning RPCs) ───────────────

    async fn dispatch_command<C>(&self, cmd: C) -> Result<Response<proto::CommandResponse>, Status>
    where
        C: cqrs::Command,
    {
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| ok_response())
            .map_err(cqrs_to_status)
    }
}

// ── Stream type aliases ─────────────────────────────────────────────────────

type StreamConversationStream =
    Pin<Box<dyn Stream<Item = Result<proto::StreamConversationResponse, Status>> + Send + 'static>>;
type StreamPublicStream =
    Pin<Box<dyn Stream<Item = Result<proto::StreamPublicResponse, Status>> + Send + 'static>>;

// ── Guarded stream: ties cleanup to the stream's lifetime ───────────────────

/// Wraps a response stream with a drop guard so per-stream resources (Redis
/// subscriptions, presence, shard activation, heartbeat tasks) are released the
/// moment the client disconnects and tonic drops the stream.
struct GuardedStream<T, G> {
    inner:  Pin<Box<dyn Stream<Item = T> + Send>>,
    _guard: G,
}

impl<T, G: Unpin> Stream for GuardedStream<T, G> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

/// Releases Member-Plane resources when the stream ends.
struct MemberStreamGuard {
    attach:          Arc<dyn PlaneAttach>,
    presence:        Arc<dyn PresenceStore>,
    fanout:          Arc<dyn Fanout>,
    conversation_id: ConversationId,
    member_id:       ProfileId,
    /// `None` when the member does not share activity status (nothing was
    /// announced, so nothing is withdrawn).
    heartbeat:       Option<JoinHandle<()>>,
}

impl Drop for MemberStreamGuard {
    fn drop(&mut self) {
        let announced = self.heartbeat.take().map(|h| h.abort()).is_some();
        let attach   = Arc::clone(&self.attach);
        let presence = Arc::clone(&self.presence);
        let fanout   = Arc::clone(&self.fanout);
        let conv     = self.conversation_id;
        let member   = self.member_id;
        tokio::spawn(async move {
            let _ = attach.detach_member(&conv).await;
            if announced {
                let _ = presence.leave(&conv, &member).await;
                let offline = PlaneEvent::Presence { member_id: member.as_str(), online: false };
                let _ = fanout.dispatch_member_signal(&conv, &offline).await;
            }
        });
    }
}

/// Releases Audience-Plane resources when the stream ends.
struct AudienceStreamGuard {
    attach:          Arc<dyn PlaneAttach>,
    routing:         Arc<dyn RoutingRegistry>,
    conversation_id: ConversationId,
    shard:           u16,
    heartbeat:       JoinHandle<()>,
}

impl Drop for AudienceStreamGuard {
    fn drop(&mut self) {
        self.heartbeat.abort();
        let attach  = Arc::clone(&self.attach);
        let routing = Arc::clone(&self.routing);
        let conv    = self.conversation_id;
        let shard   = self.shard;
        tokio::spawn(async move {
            // Deactivate the shard only when this pod's last local subscriber left.
            if attach.detach_audience(&conv, shard).await.unwrap_or(false) {
                let _ = routing.deactivate_shard(&conv, shard).await;
            }
        });
    }
}

fn spawn_presence_heartbeat(
    presence:        Arc<dyn PresenceStore>,
    conversation_id: ConversationId,
    member_id:       ProfileId,
    ttl_secs:        u64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs((ttl_secs / 2).max(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let now = Utc::now().timestamp_millis();
            let _ = presence.heartbeat(&conversation_id, &member_id, now, ttl_secs).await;
        }
    })
}

fn spawn_shard_heartbeat(
    routing:         Arc<dyn RoutingRegistry>,
    conversation_id: ConversationId,
    shard:           u16,
    ttl_secs:        u64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs((ttl_secs / 2).max(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let now = Utc::now().timestamp_millis();
            let _ = routing.activate_shard(&conversation_id, shard, now, ttl_secs).await;
        }
    })
}

// ── Proto trait implementation ──────────────────────────────────────────────

#[tonic::async_trait]
impl<CB, QB> proto::chat_service_server::ChatService for ChatServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    type StreamConversationStream = StreamConversationStream;
    type StreamPublicStream = StreamPublicStream;

    async fn create_conversation(
        &self,
        request: Request<proto::CreateConversationRequest>,
    ) -> Result<Response<proto::CreateConversationResponse>, Status> {
        self.create_conversation(request).await
    }

    async fn open_direct_conversation(
        &self,
        request: Request<proto::OpenDirectConversationRequest>,
    ) -> Result<Response<proto::OpenDirectConversationResponse>, Status> {
        self.open_direct_conversation(request).await
    }

    async fn respond_to_message_request(
        &self,
        request: Request<proto::RespondToMessageRequestRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.respond_to_message_request(request).await
    }

    async fn toggle_visibility(
        &self,
        request: Request<proto::ToggleVisibilityRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.toggle_visibility(request).await
    }

    async fn join_as_member(
        &self,
        request: Request<proto::JoinAsMemberRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.join_as_member(request).await
    }

    async fn invite_member(
        &self,
        request: Request<proto::InviteMemberRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.invite_member(request).await
    }

    async fn subscribe(
        &self,
        request: Request<proto::SubscribeRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.subscribe(request).await
    }

    async fn unsubscribe(
        &self,
        request: Request<proto::UnsubscribeRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.unsubscribe(request).await
    }

    async fn send_message(
        &self,
        request: Request<proto::SendMessageRequest>,
    ) -> Result<Response<proto::SendMessageResponse>, Status> {
        self.send_message(request).await
    }

    async fn mark_read(
        &self,
        request: Request<proto::MarkReadRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.mark_read(request).await
    }

    async fn send_typing(
        &self,
        request: Request<proto::SendTypingRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.send_typing(request).await
    }

    async fn heartbeat(
        &self,
        request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.heartbeat(request).await
    }

    async fn get_history(
        &self,
        request: Request<proto::GetHistoryRequest>,
    ) -> Result<Response<proto::GetHistoryResponse>, Status> {
        self.get_history(request).await
    }

    async fn list_members(
        &self,
        request: Request<proto::ListMembersRequest>,
    ) -> Result<Response<proto::ListMembersResponse>, Status> {
        self.list_members(request).await
    }

    async fn list_subscriptions(
        &self,
        request: Request<proto::ListSubscriptionsRequest>,
    ) -> Result<Response<proto::ListSubscriptionsResponse>, Status> {
        self.list_subscriptions(request).await
    }

    async fn list_conversations_by_member(
        &self,
        request: Request<proto::ListConversationsByMemberRequest>,
    ) -> Result<Response<proto::ListConversationsByMemberResponse>, Status> {
        self.list_conversations_by_member(request).await
    }

    async fn list_inbox(
        &self,
        request: Request<proto::ListInboxRequest>,
    ) -> Result<Response<proto::ListInboxResponse>, Status> {
        self.list_inbox(request).await
    }

    async fn stream_conversation(
        &self,
        request: Request<proto::StreamConversationRequest>,
    ) -> Result<Response<Self::StreamConversationStream>, Status> {
        self.stream_conversation(request).await
    }

    async fn stream_public(
        &self,
        request: Request<proto::StreamPublicRequest>,
    ) -> Result<Response<Self::StreamPublicStream>, Status> {
        self.stream_public(request).await
    }
}

// ── Conversion helpers ──────────────────────────────────────────────────────

fn ok_response() -> Response<proto::CommandResponse> {
    Response::new(proto::CommandResponse { success: true, message: String::new() })
}

fn non_empty(s: String) -> Option<String> {
    Some(s).filter(|s| !s.is_empty())
}

fn parse_conversation(s: &str) -> Result<ConversationId, Status> {
    ConversationId::try_from(s).map_err(|e| Status::invalid_argument(e.to_string()))
}

fn parse_profile(s: &str) -> Result<ProfileId, Status> {
    ProfileId::try_from(s).map_err(|e| Status::invalid_argument(e.to_string()))
}

fn summary_to_view(s: MessageSummary) -> proto::MessageView {
    proto::MessageView {
        message_id:    s.message_id.to_string(),
        sender_id:     s.sender_id.to_string(),
        content_type:  s.content_type.as_tinyint() as i32,
        body:          s.body,
        media_ref:     s.media_ref.unwrap_or_default(),
        reply_to:      s.reply_to.map(|u| u.to_string()).unwrap_or_default(),
        created_at_ms: s.created_at.timestamp_millis(),
    }
}

fn member_view_to_proto(m: QueryMemberView) -> proto::MemberView {
    proto::MemberView {
        profile_id:   m.profile_id.to_string(),
        role:         m.role.as_tinyint() as i32,
        joined_at_ms: m.joined_at_ms,
        last_read:    m.last_read.map(|u| u.to_string()).unwrap_or_default(),
    }
}

fn frame_to_view(f: &crate::infrastructure::routing::MessageFrame) -> proto::MessageView {
    proto::MessageView {
        message_id:    f.message_id.clone(),
        sender_id:     f.sender_id.clone(),
        content_type:  f.content_type as i32,
        body:          f.body.clone(),
        media_ref:     f.media_ref.clone().unwrap_or_default(),
        reply_to:      f.reply_to.clone().unwrap_or_default(),
        created_at_ms: f.created_at_ms,
    }
}

fn plane_to_chat_event(event: &PlaneEvent) -> proto::ChatEvent {
    use proto::chat_event::Event;
    let inner = match event {
        PlaneEvent::Message(frame) => Event::Message(frame_to_view(frame)),
        PlaneEvent::Presence { member_id, online } => Event::Presence(proto::PresenceEvent {
            member_id: member_id.clone(),
            online:    *online,
        }),
        PlaneEvent::Typing { member_id } => {
            Event::Typing(proto::TypingEvent { member_id: member_id.clone() })
        }
        PlaneEvent::Receipt { member_id, last_read } => Event::Receipt(proto::ReceiptEvent {
            member_id: member_id.clone(),
            last_read: last_read.clone(),
        }),
    };
    proto::ChatEvent { event: Some(inner) }
}

// ── Error mapping ───────────────────────────────────────────────────────────

fn status_from_app(http_status: u16, retryable: bool, msg: String) -> Status {
    match http_status {
        403 => Status::permission_denied(msg),
        404 => Status::not_found(msg),
        409 if retryable => Status::aborted(msg),
        409 => Status::already_exists(msg),
        400 | 422 => Status::failed_precondition(msg),
        503 | 502 => Status::unavailable(msg),
        _ => Status::internal(msg),
    }
}

fn chat_err_to_status(err: ChatError) -> Status {
    use error::AppError as _;
    status_from_app(err.http_status().as_u16(), err.is_retryable(), err.to_string())
}

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
            status_from_app(boxed.http_status().as_u16(), boxed.is_retryable(), boxed.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A refused private join must be byte-identical on the wire to a missing
    /// conversation: same gRPC code, same message.
    #[test]
    fn concealed_conversation_maps_exactly_like_not_found() {
        let id = ConversationId::new().as_str();
        let concealed = chat_err_to_status(ChatError::ConversationConcealed { conversation_id: id.clone() });
        let missing   = chat_err_to_status(ChatError::ConversationNotFound { conversation_id: id });

        assert_eq!(concealed.code(), tonic::Code::NotFound);
        assert_eq!(concealed.code(), missing.code());
        assert_eq!(concealed.message(), missing.message());
    }
}
