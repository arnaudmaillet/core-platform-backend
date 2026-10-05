//! #649 over real Postgres: auth enrolls MFA over the mesh (its ciphertext,
//! its code hashes); the holder's view says so; a backup code is spent once,
//! even by two concurrent sign-ins; a regenerated set replaces the old one;
//! revoking clears it all.

use cqrs::{CommandBus, Envelope, QueryBus};
use error::AppError;
use uuid::Uuid;

use account::application::command::{
    ConsumeRecoveryCodeCommand, EnrollMfaCommand, ReplaceRecoveryCodesCommand, RevokeMfaCommand,
};
use account::application::query::{GetMfaSecretQuery, MfaSecretView};

use crate::account_it::harness::{self, TestHarness, DEADLINE};

async fn active_account(h: &TestHarness) -> (String, String) {
    let (identity, email) = (harness::random_identity(), harness::random_email());
    h.create(&identity, &email).await;
    harness::await_until("account readable", DEADLINE, || async { h.get_by_identity(&identity).await.is_ok() }).await;
    let id = h.get_by_identity(&identity).await.unwrap().id;
    h.verify_email(&id).await.expect("activate");
    (identity, id)
}

fn codes(prefix: &str) -> Vec<String> {
    (0..10).map(|i| format!("{prefix}-{i}")).collect()
}

async fn secret(h: &TestHarness, id: &str) -> MfaSecretView {
    h.query_bus
        .dispatch(Envelope::new(Uuid::now_v7(), GetMfaSecretQuery { account_id: id.to_owned() }))
        .await
        .unwrap()
}

async fn spend(h: &TestHarness, id: &str, hash: &str) -> Result<(), cqrs::error::CqrsError> {
    h.command_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            ConsumeRecoveryCodeCommand { account_id: id.to_owned(), code_hash: hash.to_owned() },
        ))
        .await
}

#[tokio::test]
async fn mfa_is_enrolled_spent_regenerated_and_revoked_over_the_mesh() {
    let h = TestHarness::start().await;
    let (identity, id) = active_account(&h).await;
    assert!(!secret(&h, &id).await.enrolled);

    let enroll = |hashes: Vec<String>| {
        Envelope::new(
            Uuid::now_v7(),
            EnrollMfaCommand { account_id: id.clone(), totp_secret_ciphertext: vec![7; 40], recovery_code_hashes: hashes },
        )
    };
    let err = h.command_bus.dispatch(enroll(codes("c")[..3].to_vec())).await.unwrap_err();
    assert_eq!(err.error_code(), "ACC-9001", "too few backup codes");
    h.command_bus.dispatch(enroll(codes("c"))).await.expect("enroll");
    let err = h.command_bus.dispatch(enroll(codes("c"))).await.unwrap_err();
    assert_eq!(err.error_code(), "ACC-5001", "already on");

    let stored = secret(&h, &id).await;
    assert!(stored.enrolled && stored.totp_secret == vec![7; 40] && stored.recovery_codes_remaining == 10);
    let view = h.get_by_identity(&identity).await.unwrap();
    assert!(view.mfa_enrolled && view.mfa_recovery_codes_remaining == 10, "the holder's view says so");

    // Two sign-ins race on one code: exactly one spends it.
    let (a, b) = tokio::join!(spend(&h, &id, "c-3"), spend(&h, &id, "c-3"));
    assert_eq!([a.is_ok(), b.is_ok()].iter().filter(|ok| **ok).count(), 1, "{a:?} / {b:?}");
    assert_eq!(spend(&h, &id, "c-3").await.unwrap_err().error_code(), "ACC-5003", "spent");
    assert_eq!(secret(&h, &id).await.recovery_codes_remaining, 9);

    h.command_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            ReplaceRecoveryCodesCommand { account_id: id.clone(), recovery_code_hashes: codes("n") },
        ))
        .await
        .expect("regenerate");
    assert_eq!(spend(&h, &id, "c-4").await.unwrap_err().error_code(), "ACC-5003", "the old set is gone");
    spend(&h, &id, "n-4").await.expect("the new set");

    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), RevokeMfaCommand { account_id: id.clone() }))
        .await
        .expect("revoke");
    let cleared = secret(&h, &id).await;
    assert!(!cleared.enrolled && cleared.totp_secret.is_empty() && cleared.recovery_codes_remaining == 0);
    assert_eq!(spend(&h, &id, "n-5").await.unwrap_err().error_code(), "ACC-5002", "MFA is off");
}
