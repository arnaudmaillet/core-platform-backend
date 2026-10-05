use std::sync::Arc;

use async_trait::async_trait;

use crate::application::port::{HotTailCache, MessageSummary, RoutingRegistry};
use crate::domain::value_object::ConversationId;
use crate::error::ChatError;
use crate::infrastructure::routing::broadcaster::PlaneBroadcaster;
use crate::infrastructure::routing::plane::{MessageFrame, PlaneEvent};

/// Real-time fan-out surface consumed by the gRPC layer. Boxed as `Arc<dyn Fanout>`
/// so the handler need not carry the broadcaster/registry/cache generics.
#[async_trait]
pub trait Fanout: Send + Sync + 'static {
    /// Forks a freshly-persisted message into the hot-tail cache and both planes.
    async fn dispatch_message(
        &self,
        conversation_id: &ConversationId,
        message:         &MessageSummary,
        now_ms:          i64,
    ) -> Result<(), ChatError>;

    /// Publishes a Member-Plane-only signal (presence/typing/receipt).
    async fn dispatch_member_signal(
        &self,
        conversation_id: &ConversationId,
        event:           &PlaneEvent,
    ) -> Result<(), ChatError>;
}

/// The message-fork orchestrator that drives both planes from a single sent
/// message — the runtime embodiment of the Shadowing Pattern.
///
/// For each durably-persisted message it:
/// 1. pushes to the hot-tail cache (so new joiners load the last screen without
///    touching the live ScyllaDB write partition);
/// 2. broadcasts the full event to the Member Plane;
/// 3. broadcasts the *same* frame — the shadow — to each currently-active
///    Audience-Plane shard (read from the routing registry, so dead shards are
///    skipped and the fan-out spreads across the cluster).
///
/// Member-only signals (presence/typing/receipts) go through
/// [`dispatch_member_signal`](Self::dispatch_member_signal), which can only ever
/// reach the member channel — so the audience structurally never sees them.
pub struct MessageFanout<B, RR, HC> {
    broadcaster:       Arc<B>,
    routing:           Arc<RR>,
    hot_tail:          Arc<HC>,
    hot_tail_cap:      u16,
    audience_ttl_secs: u64,
}

impl<B, RR, HC> MessageFanout<B, RR, HC>
where
    B:  PlaneBroadcaster,
    RR: RoutingRegistry,
    HC: HotTailCache,
{
    pub fn new(
        broadcaster:       Arc<B>,
        routing:           Arc<RR>,
        hot_tail:          Arc<HC>,
        hot_tail_cap:      u16,
        audience_ttl_secs: u64,
    ) -> Self {
        Self { broadcaster, routing, hot_tail, hot_tail_cap, audience_ttl_secs }
    }

    /// Forks a freshly-persisted message into the hot-tail cache and both planes.
    pub async fn dispatch_message(
        &self,
        conversation_id: &ConversationId,
        message:         &MessageSummary,
        now_ms:          i64,
    ) -> Result<(), ChatError> {
        let event = PlaneEvent::Message(MessageFrame::from_summary(message));

        // A withheld message (#656) reaches only its sender's own streams:
        // the member channel, where every other stream drops it. Never the
        // cache, never the audience.
        if message.withheld {
            return self.broadcaster.broadcast_member(conversation_id, &event).await;
        }

        self.hot_tail.push(conversation_id, message, self.hot_tail_cap).await?;

        self.broadcaster.broadcast_member(conversation_id, &event).await?;

        let shards = self
            .routing
            .active_shards(conversation_id, now_ms, self.audience_ttl_secs)
            .await?;
        for shard in shards {
            self.broadcaster.broadcast_audience(conversation_id, shard, &event).await?;
        }

        Ok(())
    }

    /// Publishes a Member-Plane-only signal (presence/typing/receipt). By
    /// construction it never reaches the Audience Plane.
    pub async fn dispatch_member_signal(
        &self,
        conversation_id: &ConversationId,
        event:           &PlaneEvent,
    ) -> Result<(), ChatError> {
        self.broadcaster.broadcast_member(conversation_id, event).await
    }
}

#[async_trait]
impl<B, RR, HC> Fanout for MessageFanout<B, RR, HC>
where
    B:  PlaneBroadcaster,
    RR: RoutingRegistry,
    HC: HotTailCache,
{
    async fn dispatch_message(
        &self,
        conversation_id: &ConversationId,
        message:         &MessageSummary,
        now_ms:          i64,
    ) -> Result<(), ChatError> {
        MessageFanout::dispatch_message(self, conversation_id, message, now_ms).await
    }

    async fn dispatch_member_signal(
        &self,
        conversation_id: &ConversationId,
        event:           &PlaneEvent,
    ) -> Result<(), ChatError> {
        MessageFanout::dispatch_member_signal(self, conversation_id, event).await
    }
}
