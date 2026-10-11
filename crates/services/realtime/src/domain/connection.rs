use chrono::{DateTime, Duration, Utc};

use crate::domain::session::Session;
use crate::domain::subscription::SubscriptionSet;
use crate::domain::value_object::{
    ChannelRef, ConnectionId, DeviceId, NodeId, PresenceState, StreamSeq, UserId,
};
use crate::error::RealtimeError;

/// The lifecycle state of a connection.
///
/// `Active → Draining → Closed` is the only legal progression. `Draining` is the
/// graceful-rollout state: in-flight events still deliver, but no *new*
/// subscriptions are accepted (the client has been told to reconnect elsewhere).
/// `Closed` is terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Active,
    Draining,
    Closed,
}

/// Why a connection was closed. Mirrors `realtime.v1.CloseReason`; the proto
/// mapping lives in the infrastructure tier. Each maps to an `RTM-1xxx`/`RTM-5xxx`
/// lifecycle code so churn can be attributed in telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// Graceful node drain on rollout (`RTM-5003`).
    Draining,
    /// Heartbeat deadline exceeded; half-open connection reaped (`RTM-5002`).
    HeartbeatTimeout,
    /// Per-connection send queue overflowed; slow consumer shed (`RTM-5001`).
    SendQueueOverflow,
    /// Edge token expired and was not refreshed (`RTM-1002`).
    AuthExpired,
    /// Transport/framing/protocol violation (`RTM-2xxx`).
    ProtocolError,
}

impl CloseReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            CloseReason::Draining => "draining",
            CloseReason::HeartbeatTimeout => "heartbeat_timeout",
            CloseReason::SendQueueOverflow => "send_queue_overflow",
            CloseReason::AuthExpired => "auth_expired",
            CloseReason::ProtocolError => "protocol_error",
        }
    }
}

/// The aggregate root for one live client connection: its pinned [`Session`], its
/// [`SubscriptionSet`], its lifecycle state, and its heartbeat freshness. Pure —
/// the wall clock is injected as `now`, and there is no I/O, transport, or store
/// awareness. The gateway holds one of these per socket; everything the plane
/// decides about a socket (may it subscribe here, what sequence does this event
/// get, is it dead, is it draining, is it online) is a method on this type.
#[derive(Debug, Clone)]
pub struct Connection {
    id: ConnectionId,
    node_id: NodeId,
    session: Session,
    subscriptions: SubscriptionSet,
    state: ConnectionState,
    last_heartbeat: DateTime<Utc>,
}

impl Connection {
    /// Open a connection on `node_id` for an already-authenticated `session`,
    /// bounded at `subscription_cap` channels. Starts `Active` with a fresh
    /// heartbeat at `now`.
    pub fn open(
        id: ConnectionId,
        node_id: NodeId,
        session: Session,
        subscription_cap: usize,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            node_id,
            session,
            subscriptions: SubscriptionSet::new(subscription_cap),
            state: ConnectionState::Active,
            last_heartbeat: now,
        }
    }

    pub fn id(&self) -> &ConnectionId {
        &self.id
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// The pinned identity (and the token ids it is re-checked by, #903).
    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn user_id(&self) -> &UserId {
        &self.session.user_id
    }

    pub fn device_id(&self) -> &DeviceId {
        &self.session.device_id
    }

    pub fn state(&self) -> ConnectionState {
        self.state
    }

    pub fn is_subscribed(&self, channel: &ChannelRef) -> bool {
        self.subscriptions.is_subscribed(channel)
    }

    /// The channels this connection is currently subscribed to.
    pub fn subscribed_channels(&self) -> Vec<ChannelRef> {
        self.subscriptions.channels()
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Subscribe to a channel: authorize it against the pinned session, then add
    /// it to the bounded set. Rejected while `Draining` (`RTM-5003`, reconnect
    /// elsewhere) or `Closed` (`RTM-9001`). Returns `true` if newly added.
    pub fn subscribe(&mut self, channel: ChannelRef) -> Result<bool, RealtimeError> {
        match self.state {
            ConnectionState::Draining => return Err(RealtimeError::ConnectionDraining),
            ConnectionState::Closed => {
                return Err(RealtimeError::DomainViolation {
                    field: "connection_state".to_owned(),
                    message: "cannot subscribe on a closed connection".to_owned(),
                });
            }
            ConnectionState::Active => {}
        }
        self.session.authorize(&channel)?;
        self.subscriptions.subscribe(channel)
    }

    /// Unsubscribe from a channel. Rejected only when `Closed`.
    pub fn unsubscribe(&mut self, channel: &ChannelRef) -> Result<(), RealtimeError> {
        if self.state == ConnectionState::Closed {
            return Err(RealtimeError::DomainViolation {
                field: "connection_state".to_owned(),
                message: "cannot unsubscribe on a closed connection".to_owned(),
            });
        }
        self.subscriptions.unsubscribe(channel)
    }

    /// Assign the next stream sequence for an event about to be delivered on
    /// `channel`. Permitted while `Active` or `Draining` (in-flight delivery
    /// continues during a drain); rejected when `Closed`.
    pub fn issue_seq(&mut self, channel: &ChannelRef) -> Result<StreamSeq, RealtimeError> {
        if self.state == ConnectionState::Closed {
            return Err(RealtimeError::DomainViolation {
                field: "connection_state".to_owned(),
                message: "cannot deliver to a closed connection".to_owned(),
            });
        }
        self.subscriptions.issue_seq(channel)
    }

    /// Record a client ack on `channel`.
    pub fn ack(&mut self, channel: &ChannelRef, seq: StreamSeq) -> Result<(), RealtimeError> {
        self.subscriptions.ack(channel, seq)
    }

    /// Refresh the heartbeat (on a client Pong / any inbound frame). A no-op once
    /// `Closed`.
    pub fn heartbeat(&mut self, now: DateTime<Utc>) {
        if self.state != ConnectionState::Closed {
            self.last_heartbeat = now;
        }
    }

    /// Whether the connection has missed its heartbeat deadline and should be
    /// reaped (freeing its file descriptor + registry slot). Always false once
    /// `Closed`.
    pub fn should_reap(&self, now: DateTime<Utc>, timeout: Duration) -> bool {
        self.state != ConnectionState::Closed
            && now.signed_duration_since(self.last_heartbeat) > timeout
    }

    /// Whether the session's edge token has lapsed and the client must re-auth
    /// (`RTM-1002`).
    pub fn needs_reauth(&self, now: DateTime<Utc>) -> bool {
        self.session.is_expired(now)
    }

    /// Begin draining (graceful rollout): `Active → Draining`. Idempotent while
    /// already `Draining`; `RTM-9001 DomainViolation` if already `Closed`.
    pub fn begin_drain(&mut self) -> Result<(), RealtimeError> {
        match self.state {
            ConnectionState::Active | ConnectionState::Draining => {
                self.state = ConnectionState::Draining;
                Ok(())
            }
            ConnectionState::Closed => Err(RealtimeError::DomainViolation {
                field: "connection_state".to_owned(),
                message: "cannot drain a closed connection".to_owned(),
            }),
        }
    }

    /// Close the connection with a reason. Terminal and idempotent.
    pub fn close(&mut self, _reason: CloseReason) {
        self.state = ConnectionState::Closed;
    }

    /// The derived presence for this connection: `Online` while active with a
    /// fresh heartbeat, `Away` while active but past the heartbeat deadline (not
    /// yet reaped — a radio sleep or flaky link), `Offline` once `Closed`.
    pub fn presence(&self, now: DateTime<Utc>, heartbeat_timeout: Duration) -> PresenceState {
        if self.state == ConnectionState::Closed {
            return PresenceState::Offline;
        }
        if now.signed_duration_since(self.last_heartbeat) > heartbeat_timeout {
            PresenceState::Away
        } else {
            PresenceState::Online
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use error::AppError;

    use super::*;
    use crate::domain::value_object::{ChannelClass, ChannelKey};

    fn at(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 26, h, m, s).unwrap()
    }

    fn conn(now: DateTime<Utc>) -> Connection {
        let session = Session::new(
            UserId::new("alice").unwrap(),
            DeviceId::new("dev-1").unwrap(),
            at(13, 0, 0),
        );
        Connection::open(
            ConnectionId::new("conn-1").unwrap(),
            NodeId::new("node-7").unwrap(),
            session,
            8,
            now,
        )
    }

    fn channel(class: ChannelClass, key: &str) -> ChannelRef {
        ChannelRef::new(class, ChannelKey::new(key).unwrap())
    }

    #[test]
    fn opens_active_and_online() {
        let c = conn(at(12, 0, 0));
        assert_eq!(c.state(), ConnectionState::Active);
        assert_eq!(
            c.presence(at(12, 0, 10), Duration::seconds(90)),
            PresenceState::Online
        );
    }

    #[test]
    fn authorizes_subscriptions_against_the_pinned_session() {
        let mut c = conn(at(12, 0, 0));
        c.subscribe(channel(ChannelClass::Dm, "alice")).unwrap();
        let err = c
            .subscribe(channel(ChannelClass::Dm, "bob"))
            .unwrap_err();
        assert_eq!(err.error_code(), "RTM-3001");
    }

    #[test]
    fn draining_rejects_new_subscriptions_but_still_delivers() {
        let mut c = conn(at(12, 0, 0));
        let counter = channel(ChannelClass::Counter, "post-1");
        c.subscribe(counter.clone()).unwrap();
        c.begin_drain().unwrap();
        // No new subscriptions while draining …
        let err = c
            .subscribe(channel(ChannelClass::Counter, "post-2"))
            .unwrap_err();
        assert_eq!(err.error_code(), "RTM-5003");
        // … but an already-subscribed channel still gets sequenced.
        assert_eq!(c.issue_seq(&counter).unwrap().get(), 1);
    }

    #[test]
    fn closed_connection_rejects_everything_and_is_offline() {
        let mut c = conn(at(12, 0, 0));
        c.close(CloseReason::HeartbeatTimeout);
        assert_eq!(
            c.subscribe(channel(ChannelClass::Dm, "alice"))
                .unwrap_err()
                .error_code(),
            "RTM-9001"
        );
        assert_eq!(
            c.presence(at(12, 0, 1), Duration::seconds(90)),
            PresenceState::Offline
        );
        // close is idempotent; drain after close is a violation.
        c.close(CloseReason::Draining);
        assert_eq!(c.begin_drain().unwrap_err().error_code(), "RTM-9001");
    }

    #[test]
    fn heartbeat_governs_reaping_and_presence() {
        let mut c = conn(at(12, 0, 0));
        let timeout = Duration::seconds(90);
        // Fresh: online, not reapable.
        assert!(!c.should_reap(at(12, 1, 0), timeout));
        assert_eq!(c.presence(at(12, 1, 0), timeout), PresenceState::Online);
        // Overdue: away, reapable.
        assert!(c.should_reap(at(12, 2, 0), timeout));
        assert_eq!(c.presence(at(12, 2, 0), timeout), PresenceState::Away);
        // A heartbeat resets the clock.
        c.heartbeat(at(12, 2, 0));
        assert!(!c.should_reap(at(12, 2, 30), timeout));
    }

    #[test]
    fn needs_reauth_after_token_expiry() {
        let c = conn(at(12, 0, 0));
        assert!(!c.needs_reauth(at(12, 59, 59)));
        assert!(c.needs_reauth(at(13, 0, 0)));
    }
}
