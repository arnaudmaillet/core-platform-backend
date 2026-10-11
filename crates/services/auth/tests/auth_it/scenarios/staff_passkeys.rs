//! #880 against the live Postgres: a session keeps how it was opened (and its
//! passkey), a staff enrollment grant is used once, by its own account, before
//! it expires, and the bootstrap check sees a staff passkey on any shard.

use chrono::{Duration, Utc};
use postgres_storage::TransactionManager;
use uuid::Uuid;

use auth::application::port::{PasskeyRepository, SessionRepository, StaffEnrollmentGrants, StaffGrant, StoredPasskey};
use auth::domain::aggregate::{Session, SessionIssueParams};
use auth::domain::value_object::{
    AccountId, DeviceFingerprint, Generation, IdpSubject, SessionKind, SessionMethod, SessionSignIn,
};
use auth::infrastructure::persistence::{PgPasskeyRepository, PgSessionRepository, PgStaffGrants};

use crate::auth_it::harness::Harness;

use chrono::SubsecRound;

fn passkey(id: u8, staff: bool) -> StoredPasskey {
    StoredPasskey {
        credential_id: vec![id; 16],
        public_key: vec![4; 65],
        sign_count: 0,
        name: "Key".into(),
        aaguid: Uuid::nil(),
        backup_eligible: true,
        backed_up: true,
        created_at: Utc::now().trunc_subsecs(6),
        last_used_at: None,
        staff_enrolled: staff,
    }
}

#[tokio::test]
async fn sessions_keep_their_passkey_and_staff_grants_are_single_use() {
    let h = Harness::start().await;
    let tx = TransactionManager::new(h.pool.clone());
    let (sessions, passkeys, grants) =
        (PgSessionRepository::new(tx.clone()), PgPasskeyRepository::new(tx.clone()), PgStaffGrants::new(tx));
    let account = AccountId::from_uuid(Uuid::now_v7());
    let now = Utc::now();

    // How a session was opened survives the round trip.
    for sign_in in [SessionSignIn::by_passkey(vec![7; 16]), SessionSignIn::by(SessionMethod::EmailCode)] {
        let session = Session::issue(SessionIssueParams {
            account_id: account,
            kind: SessionKind::Member,
            subject: IdpSubject::new("urn:core-platform:email", "ada@example.com").unwrap(),
            generation: Generation::INITIAL,
            device: DeviceFingerprint::default(),
            sign_in: sign_in.clone(),
            issued_at: now,
            expires_at: now + Duration::minutes(30),
            absolute_expiry: now + Duration::hours(8),
            correlation_id: Uuid::now_v7(),
        })
        .unwrap();
        sessions.save(&session).await.unwrap();
        assert_eq!(sessions.find_by_id(&session.id()).await.unwrap().unwrap().sign_in(), &sign_in);
    }

    // The staff flag is stored; the bootstrap check sees it.
    passkeys.add(&account, &passkey(1, false), 10).await.unwrap();
    passkeys.add(&account, &passkey(2, true), 10).await.unwrap();
    assert!(passkeys.find(&account, &[2; 16]).await.unwrap().unwrap().staff_enrolled);
    assert!(!passkeys.find(&account, &[1; 16]).await.unwrap().unwrap().staff_enrolled);
    assert!(passkeys.any_staff_enrolled().await.unwrap());

    // A grant: its own account only, once, before it expires.
    let grant = StaffGrant { grant_id: Uuid::now_v7(), account_id: account, issued_by: None, issued_at: now, expires_at: now + Duration::hours(24) };
    let hash = format!("it-{}", Uuid::now_v7());
    grants.issue(&grant, &hash).await.unwrap();
    let stranger = AccountId::from_uuid(Uuid::now_v7());
    assert_eq!(grants.redeem(&stranger, &hash, now).await.unwrap(), None, "another account's");
    assert_eq!(grants.redeem(&account, &hash, now + Duration::hours(25)).await.unwrap(), None, "expired");
    assert_eq!(grants.redeem(&account, &hash, now).await.unwrap(), Some(grant.grant_id));
    assert_eq!(grants.redeem(&account, &hash, now).await.unwrap(), None, "used once");
}
