//! #649 over the real graph (Postgres, Redis, the AES seed cipher) through the
//! gRPC handler: with two-step sign-in on, Login issues nothing but a
//! challenge; CompleteLogin with the authenticator's code — or a backup code —
//! issues the session once; a code works once; five wrong codes lock the
//! account's codes for a while.

use tonic::{Code, Request};

use auth::application::port::MfaSeedCipher;
use auth::domain::value_object::{step_of, AccountId, TotpSecret};
use auth::infrastructure::grpc::handler::proto;

use crate::auth_it::harness::{random_user, Harness};

/// Signs `user` in once (no second factor yet), then turns two-step sign-in
/// on for its account. Returns its seed; backup codes `abcde-fghjk`,
/// `mnpqr-stuvw`.
async fn enrolled(h: &Harness, user: &str) -> TotpSecret {
    let first = h.login(user).await.expect("first sign-in");
    assert!(!first.mfa_required && first.tokens.is_some());
    let account = AccountId::try_from(first.account_id.as_str()).unwrap();
    let seed = TotpSecret::generate();
    let sealed = h.seed_cipher.seal(seed.as_bytes()).unwrap();
    let codes = ["abcdefghjk", "mnpqrstuvw"].map(|c| h.seed_cipher.code_hash(c).unwrap()).to_vec();
    h.directory.mfa.lock().unwrap().insert(account, (sealed, codes));
    seed
}

async fn challenge(h: &Harness, user: &str) -> String {
    let response = h.login(user).await.expect("sign-in");
    assert!(response.mfa_required, "a second factor is required");
    assert!(response.tokens.is_none(), "no session before the code");
    assert!(response.mfa_expires_in > 0);
    response.mfa_token
}

async fn complete(h: &Harness, mfa_token: &str, code: &str) -> Result<proto::LoginResponse, tonic::Status> {
    h.handler
        .complete_login(Request::new(proto::CompleteLoginRequest { mfa_token: mfa_token.to_owned(), code: code.to_owned(), passkey: None }))
        .await
        .map(|r| r.into_inner())
}

#[tokio::test]
async fn a_sign_in_waits_for_its_code_and_each_code_works_once() {
    let h = Harness::start().await;
    let user = random_user();
    let seed = enrolled(&h, &user).await;

    let token = challenge(&h, &user).await;
    let code = seed.code_at(step_of(chrono::Utc::now()));
    let signed_in = complete(&h, &token, &code).await.expect("the authenticator's code");
    assert!(signed_in.tokens.is_some() && !signed_in.mfa_required);

    let reused = complete(&h, &token, &code).await.unwrap_err();
    assert_eq!(reused.code(), Code::Unauthenticated, "the challenge is single use: {reused:?}");

    // The same TOTP code on a new sign-in: its step is spent.
    let token = challenge(&h, &user).await;
    let replay = complete(&h, &token, &code).await.unwrap_err();
    assert_eq!(replay.code(), Code::Unauthenticated, "a replayed code: {replay:?}");
    // A backup code (any case, with or without the dash) works, once.
    complete(&h, &token, "ABCDE-FGHJK").await.expect("a backup code");
    let token = challenge(&h, &user).await;
    assert_eq!(complete(&h, &token, "abcdefghjk").await.unwrap_err().code(), Code::Unauthenticated);
    complete(&h, &token, "mnpqr-stuvw").await.expect("the other backup code");
}

#[tokio::test]
async fn five_wrong_codes_lock_the_accounts_codes() {
    let h = Harness::start().await;
    let user = random_user();
    let seed = enrolled(&h, &user).await;
    let token = challenge(&h, &user).await;
    let right = seed.code_at(step_of(chrono::Utc::now()));

    let mut wrong = 0;
    for candidate in ["000000", "111111", "222222", "333333", "444444", "555555"] {
        if candidate == right || wrong == 5 {
            continue;
        }
        assert_eq!(complete(&h, &token, candidate).await.unwrap_err().code(), Code::Unauthenticated);
        wrong += 1;
    }
    let locked = complete(&h, &token, &right).await.unwrap_err();
    assert_eq!(locked.code(), Code::ResourceExhausted, "even the right code: {locked:?}");
    let retry_after: i64 = locked.metadata().get("retry-after-secs").unwrap().to_str().unwrap().parse().unwrap();
    assert!(retry_after > 0 && retry_after <= 15 * 60);
}

#[tokio::test]
async fn an_account_without_two_step_sign_in_signs_in_at_once() {
    let h = Harness::start().await;
    let response = h.login(&random_user()).await.unwrap();
    assert!(!response.mfa_required && response.mfa_token.is_empty() && response.tokens.is_some());
    let unknown = complete(&h, "not-a-challenge", "123456").await.unwrap_err();
    assert_eq!(unknown.code(), Code::Unauthenticated);
}

/// Parallel guesses on one challenge over the live Redis: each reserves its
/// attempt before its code is looked at, so at most five are evaluated.
#[tokio::test]
async fn parallel_guesses_are_counted_before_they_are_checked() {
    let h = std::sync::Arc::new(Harness::start().await);
    let user = random_user();
    let seed = enrolled(&h, &user).await;
    let token = challenge(&h, &user).await;
    let right = seed.code_at(step_of(chrono::Utc::now()));

    let guesses: Vec<_> = (0..20u32)
        .map(|i| format!("{:06}", (i * 104_729 + 7) % 1_000_000))
        .filter(|code| *code != right)
        .map(|code| {
            let (h, token) = (std::sync::Arc::clone(&h), token.clone());
            tokio::spawn(async move { complete(&h, &token, &code).await })
        })
        .collect();
    let (mut evaluated, mut locked) = (0, 0);
    for guess in guesses {
        match guess.await.unwrap().unwrap_err().code() {
            Code::Unauthenticated => evaluated += 1,
            Code::ResourceExhausted => locked += 1,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(evaluated, 5, "only the window's five attempts are evaluated");
    assert!(locked >= 14);
}
