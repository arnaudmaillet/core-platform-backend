//! The gateway edge runtime: the public WebSocket server, the per-socket
//! lifecycle, and the node-channel subscriber that turns inbound `DeliverEnvelope`s
//! into `Event` frames on the right sockets.
//!
//! This is live networking — its behaviour is exercised by the Phase 6 suite, not
//! unit tests. The routing it drives ([`ConnectionTable`]) is tested in isolation.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use axum::routing::get;
use chrono::Utc;
use error::AppError;
use fred::interfaces::{EventInterface, PubsubInterface};
use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use realtime_api as pb;
use redis_storage::RedisSubscriber;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use transport::grpc::revocation::Revocations;
use uuid::Uuid;

use crate::application::handshake::HandshakeHandler;
use crate::application::lifecycle::ReapHandler;
use crate::domain::{Connection, ConnectionId, Session};
use crate::error::RealtimeError;
use crate::infrastructure::codec::{self, ClientIntent};
use crate::infrastructure::redis_node_channel::BROADCAST_CHANNEL;
use crate::infrastructure::runtime::connection_table::{ConnHandle, ConnectionTable};

/// Shared state every WebSocket connection is served against.
#[derive(Clone)]
pub struct GatewayState {
    pub handshake: Arc<HandshakeHandler>,
    pub reap: Arc<ReapHandler>,
    pub table: Arc<ConnectionTable>,
    pub send_queue_cap: usize,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    /// Re-checks each live socket's token against `auth`'s revocation state on
    /// the heartbeat (#903); `None` when not configured.
    pub revocations: Option<Revocations>,
}

#[derive(Debug, Deserialize)]
struct WsQuery {
    /// The edge token, presented as a query parameter on the upgrade request.
    access_token: String,
}

/// Bind the public WS listener and serve until shutdown.
pub async fn serve_ws(state: GatewayState, addr: String) -> anyhow::Result<()> {
    let app = Router::new().route("/ws", get(ws_upgrade)).with_state(state);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "realtime gateway WS listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ws_upgrade(
    State(state): State<GatewayState>,
    Query(query): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state, query.access_token))
}

/// The per-connection lifecycle: authenticate, register, then pump frames until
/// close/reap, then tear down the registry + table slot.
async fn handle_socket(socket: WebSocket, state: GatewayState, token: String) {
    let now = Utc::now();
    let Ok(connection_id) = ConnectionId::new(Uuid::now_v7().to_string()) else {
        return;
    };

    let connection = match state
        .handshake
        .accept(&token, connection_id.clone(), now)
        .await
    {
        Ok(connection) => connection,
        Err(error) => {
            tracing::debug!(%error, "ws handshake rejected");
            return;
        }
    };

    let user_id = connection.user_id().clone();
    let device_id = connection.device_id().clone();
    let connection = Arc::new(Mutex::new(connection));

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(state.send_queue_cap);
    let handle = ConnHandle {
        connection_id: connection_id.clone(),
        device_id,
        connection: Arc::clone(&connection),
        sender: tx.clone(),
    };
    state.table.insert(user_id.as_str(), handle.clone());

    let (mut ws_tx, mut ws_rx) = socket.split();

    // Writer task: drain the bounded mailbox onto the socket.
    let writer = tokio::spawn(async move {
        while let Some(bytes) = rx.recv().await {
            if ws_tx.send(Message::Binary(bytes.into())).await.is_err() {
                break;
            }
        }
    });

    // Reader + heartbeat loop.
    let mut ticker = tokio::time::interval(state.heartbeat_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let timeout = chrono::Duration::from_std(state.heartbeat_timeout)
        .unwrap_or_else(|_| chrono::Duration::seconds(90));
    let mut nonce = 0u64;

    loop {
        tokio::select! {
            inbound = ws_rx.next() => match inbound {
                Some(Ok(Message::Binary(data))) => {
                    connection.lock().await.heartbeat(Utc::now());
                    if let Err(error) =
                        handle_client_frame(&data, &connection, &tx, &state, &handle).await
                    {
                        tracing::debug!(%error, "client frame rejected");
                    }
                }
                Some(Ok(Message::Pong(_))) => connection.lock().await.heartbeat(Utc::now()),
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                // Ping (axum auto-pongs) and Text are ignored.
                Some(Ok(_)) => {}
            },
            _ = ticker.tick() => {
                let now = Utc::now();
                let (reap, expired) = {
                    let conn = connection.lock().await;
                    (conn.should_reap(now, timeout), conn.needs_reauth(now))
                };
                if reap {
                    break;
                }
                let session = connection.lock().await.session().clone();
                if is_revoked(state.revocations.as_ref(), &session).await {
                    let frame = codec::control_frame(
                        pb::ServerControl::ReauthRequired, None, "RTM-1001",
                        "edge token revoked; sign in again", 0,
                    ).encode_to_vec();
                    let _ = tx.try_send(frame);
                    tracing::debug!("ws closed: token revoked");
                    break;
                }
                if expired {
                    let frame = codec::control_frame(
                        pb::ServerControl::ReauthRequired, None, "RTM-1002",
                        "edge token expired; refresh and re-handshake", 0,
                    ).encode_to_vec();
                    let _ = tx.try_send(frame);
                }
                nonce += 1;
                let _ = tx.try_send(codec::ping_frame(nonce).encode_to_vec());
            }
        }
    }

    // Teardown: drop the connection from the broadcast index for every channel it
    // held, free the routing slots (idempotent), and stop the writer.
    for channel in connection.lock().await.subscribed_channels() {
        state
            .table
            .unsubscribe_channel(&channel.to_string(), &connection_id);
    }
    state.table.remove(user_id.as_str(), &connection_id);
    if let Err(error) = state.reap.evict(&user_id, &connection_id).await {
        tracing::warn!(%error, "registry evict failed on teardown");
    }
    // Let the writer flush what is queued (a last `ReauthRequired`, #903): it
    // ends once every sender is gone; past a second it is stopped.
    drop(tx);
    drop(handle);
    let stop = writer.abort_handle();
    if tokio::time::timeout(WRITER_DRAIN, writer).await.is_err() {
        stop.abort();
    }
}

/// How long a closing socket's writer may take to flush its queue.
const WRITER_DRAIN: Duration = Duration::from_secs(1);

/// `true` when the socket's token was revoked in `auth` since its handshake
/// (#903). Unconfigured, a token without ids, or an unreadable state (realtime
/// fails open) keep the socket.
async fn is_revoked(revocations: Option<&Revocations>, session: &Session) -> bool {
    let (Some(revocations), Some(token)) = (revocations, &session.token) else {
        return false;
    };
    revocations
        .admit_ids(session.user_id.as_str(), &token.session_id, token.generation)
        .await
        .is_err()
}

/// Decode one client frame and apply its intent to the connection, keeping the
/// table's broadcast index in sync with the connection's subscriptions.
async fn handle_client_frame(
    data: &[u8],
    connection: &Arc<Mutex<Connection>>,
    tx: &mpsc::Sender<Vec<u8>>,
    state: &GatewayState,
    handle: &ConnHandle,
) -> Result<(), RealtimeError> {
    let frame = pb::ClientFrame::decode(data).map_err(|e| RealtimeError::MalformedFrame {
        reason: e.to_string(),
    })?;

    match codec::decode_client_frame(&frame)? {
        ClientIntent::Subscribe(channels) => {
            let mut conn = connection.lock().await;
            for channel in channels {
                match conn.subscribe(channel.clone()) {
                    Ok(_) => {
                        state
                            .table
                            .subscribe_channel(&channel.to_string(), handle.clone());
                        send_control(tx, pb::ServerControl::Subscribed, Some(&channel), "", "");
                    }
                    Err(error) => send_control(
                        tx,
                        pb::ServerControl::Error,
                        Some(&channel),
                        error.error_code(),
                        &error.to_string(),
                    ),
                }
            }
        }
        ClientIntent::Unsubscribe(channels) => {
            let mut conn = connection.lock().await;
            for channel in channels {
                if conn.unsubscribe(&channel).is_ok() {
                    state
                        .table
                        .unsubscribe_channel(&channel.to_string(), &handle.connection_id);
                    send_control(tx, pb::ServerControl::Unsubscribed, Some(&channel), "", "");
                }
            }
        }
        ClientIntent::Ack { channel, stream_seq } => {
            let _ = connection.lock().await.ack(&channel, stream_seq);
        }
        ClientIntent::Pong { .. } => connection.lock().await.heartbeat(Utc::now()),
        // A live token refresh is accepted but full re-verification is a follow-up;
        // the session's existing expiry still governs (RTM-1002 on lapse).
        ClientIntent::AuthRefresh { .. } => {}
        // Resume re-subscribes the client's channels; live flow resumes, and any
        // gap older than what is in flight is the client's re-sync against the SoR.
        ClientIntent::Resume(cursors) => {
            let mut conn = connection.lock().await;
            for (channel, _seq) in cursors {
                if conn.subscribe(channel.clone()).is_ok() {
                    state
                        .table
                        .subscribe_channel(&channel.to_string(), handle.clone());
                }
            }
        }
    }
    Ok(())
}

fn send_control(
    tx: &mpsc::Sender<Vec<u8>>,
    control: pb::ServerControl,
    channel: Option<&crate::domain::ChannelRef>,
    code: &str,
    message: &str,
) {
    let frame = codec::control_frame(control, channel, code, message, 0).encode_to_vec();
    let _ = tx.try_send(frame);
}

/// Spawn the node-channel subscriber: `SSUBSCRIBE rt:node:{node_id}`, decode each
/// `DeliverEnvelope`, and route it to the local connections via the table.
pub fn spawn_node_subscriber(
    subscriber: RedisSubscriber,
    node_id: String,
    table: Arc<ConnectionTable>,
) {
    tokio::spawn(async move {
        // The node's own targeted hop channel, plus the fleet broadcast channel.
        let node = format!("rt:node:{{{node_id}}}");
        if let Err(error) = subscriber.inner.ssubscribe(node.clone()).await {
            tracing::error!(%error, channel = node, "failed to subscribe node channel");
            return;
        }
        if let Err(error) = subscriber.inner.ssubscribe(BROADCAST_CHANNEL).await {
            tracing::error!(%error, "failed to subscribe broadcast channel");
            return;
        }
        tracing::info!(channel = node, "node subscriber listening (targeted + broadcast)");

        let mut rx = subscriber.inner.message_rx();
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    let Some(bytes) = msg.value.as_bytes() else {
                        continue;
                    };
                    let Ok(envelope) = pb::DeliverEnvelope::decode(bytes) else {
                        continue;
                    };
                    let Ok(event) = codec::envelope_from_pb(&envelope) else {
                        continue;
                    };
                    // A broadcast (no recipient) goes to local channel subscribers;
                    // a targeted event goes to the recipient's connections.
                    if event.is_broadcast() {
                        table.deliver_broadcast(&event).await;
                    } else {
                        table.deliver(&event).await;
                    }
                }
                Err(RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "node subscriber lagged")
                }
                Err(RecvError::Closed) => break,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::DateTime;
    use transport::grpc::revocation::{
        RevocationCheck, RevocationFailure, RevocationLookup, RevocationUnavailable, Revocations,
    };

    use super::*;
    use crate::domain::DeviceId;

    /// Revoked when `gen` is below the current one; `None` = unreadable.
    struct Current(Option<i64>);

    impl RevocationCheck for Current {
        fn is_revoked<'a>(&'a self, _: &'a str, _: &'a str, generation: i64) -> RevocationLookup<'a> {
            Box::pin(async move { self.0.map(|current| generation < current).ok_or(RevocationUnavailable) })
        }
    }

    fn session(token: Option<i64>) -> Session {
        let session = Session::new(
            crate::domain::UserId::new("acct-1".to_owned()).unwrap(),
            DeviceId::new("d".to_owned()).unwrap(),
            DateTime::from_timestamp(4_102_444_800, 0).unwrap(),
        );
        match token {
            Some(generation) => session.with_token("s-1", generation),
            None => session,
        }
    }

    /// #903: a live socket whose token was revoked since its handshake is
    /// closed at the next heartbeat; an unreadable state (fail open), a token
    /// without ids, or no check configured keep it.
    #[tokio::test]
    async fn a_socket_is_closed_once_its_token_is_revoked() {
        let revocations = |current| Revocations::new(Arc::new(Current(current)), RevocationFailure::Open);
        assert!(!is_revoked(Some(&revocations(Some(1))), &session(Some(1))).await);
        assert!(is_revoked(Some(&revocations(Some(2))), &session(Some(1))).await, "signed out since");
        assert!(!is_revoked(Some(&revocations(None)), &session(Some(1))).await, "fail open");
        assert!(!is_revoked(Some(&revocations(Some(2))), &session(None)).await, "no ids to check");
        assert!(!is_revoked(None, &session(Some(1))).await, "not configured");
    }
}
