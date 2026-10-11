//! #884 against the live Redis: what auth writes on a sign-out (generation bump,
//! session blacklist) is what the client edge of every service reads
//! (`service_runtime::edge_revocation`, keys from `auth_context`). A token is
//! live until its session is signed out or its account signed out everywhere.

use std::time::Duration;

use service_runtime::edge_revocation::{CachedRevocationCheck, RedisRevocationState};
use transport::grpc::revocation::RevocationCheck;

use crate::auth_it::harness::{random_user, Harness};

#[tokio::test]
async fn the_edge_sees_what_auth_revokes() {
    let h = Harness::start().await;
    let edge = CachedRevocationCheck::new(RedisRevocationState::new(h.redis.clone()), Duration::ZERO);
    let user = random_user();
    let a = h.login(&user).await.expect("login a").tokens.unwrap();
    let b = h.login(&user).await.expect("login b").tokens.unwrap();
    let a = h.introspect(&a.access_token).await.unwrap();
    let b = h.introspect(&b.access_token).await.unwrap();
    let revoked = |token: &auth::infrastructure::grpc::handler::proto::IntrospectResponse| {
        let (account, session, generation) = (token.account_id.clone(), token.session_id.clone(), token.generation);
        let edge = &edge;
        async move { edge.is_revoked(&account, &session, generation).await.expect("redis readable") }
    };
    assert!(!revoked(&a).await && !revoked(&b).await, "fresh tokens are live");

    // One session signed out: its token only.
    h.logout(&a.session_id).await.expect("logout");
    assert!(revoked(&a).await, "the signed-out session is blacklisted");
    assert!(!revoked(&b).await, "the other session is untouched");

    // Signed out everywhere: every token minted before.
    h.logout_all(&b.account_id).await.expect("logout all");
    assert!(revoked(&b).await, "the generation was bumped");
}
