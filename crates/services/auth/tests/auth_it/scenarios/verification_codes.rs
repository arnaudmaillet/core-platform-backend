//! One-time codes against a live Redis: a code proves its address once, wrong
//! codes burn the challenge, and sends are budgeted per address.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use auth::application::command::{StartVerificationCommand, VerificationCodes, VerificationPolicy};
use auth::application::port::{CodeSender, VerificationChannel};
use auth::error::AuthError;
use auth::infrastructure::cache::RedisVerificationStore;

use crate::auth_it::harness::Harness;

#[derive(Default)]
struct Outbox(Mutex<Vec<String>>);

#[async_trait]
impl CodeSender for Outbox {
    async fn send(&self, _: VerificationChannel, _: &str, code: &str, _: Option<&str>) -> Result<(), AuthError> {
        self.0.lock().unwrap().push(code.to_owned());
        Ok(())
    }
}

impl Outbox {
    fn last(&self) -> String {
        self.0.lock().unwrap().last().cloned().unwrap()
    }
}

fn start(to: &str) -> StartVerificationCommand {
    StartVerificationCommand { channel: VerificationChannel::Email, destination: to.into(), locale: None }
}

#[tokio::test]
async fn codes_prove_an_address_once_and_sends_are_budgeted() {
    let h = Harness::start().await;
    let outbox = Arc::new(Outbox::default());
    let codes = VerificationCodes::new(
        Arc::new(RedisVerificationStore::new(h.redis.clone())),
        Arc::clone(&outbox) as _,
        VerificationPolicy { per_hour: 3, resend: chrono::Duration::seconds(1), max_failures: 6, ..VerificationPolicy::default() },
    );
    let address = format!("it.{}@example.com", uuid::Uuid::now_v7().simple());

    // Right code once; then spent.
    let started = codes.start(start(&address)).await.unwrap();
    let code = outbox.last();
    let proven = codes.verify(&started.challenge_id, &code).await.unwrap();
    assert_eq!(proven.destination, address);
    assert!(matches!(codes.verify(&started.challenge_id, &code).await, Err(AuthError::VerificationCodeInvalid)));

    // Resend cooldown, then wrong codes burn the challenge (5 attempts).
    assert!(matches!(codes.start(start(&address)).await, Err(AuthError::VerificationRateLimited { .. })));
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let second = codes.start(start(&address)).await.unwrap();
    let code = outbox.last();
    let wrong = if code == "000000" { "111111" } else { "000000" };
    for _ in 0..5 {
        assert!(codes.verify(&second.challenge_id, wrong).await.is_err());
    }
    assert!(matches!(codes.verify(&second.challenge_id, &code).await, Err(AuthError::VerificationCodeInvalid)));

    // The hourly budget (3): the third send is the last one.
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    codes.start(start(&address)).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let refused = codes.start(start(&address)).await;
    assert!(matches!(refused, Err(AuthError::VerificationRateLimited { retry_after_secs }) if retry_after_secs > 1));

    // The failure ceiling (6) counts across challenges: lock another address
    // with 5 wrong codes on one challenge and a 6th on the next.
    let locked = format!("lock.{}@example.com", uuid::Uuid::now_v7().simple());
    let third = codes.start(start(&locked)).await.unwrap();
    let code = outbox.last();
    let wrong = if code == "000000" { "111111" } else { "000000" };
    for _ in 0..5 {
        let _ = codes.verify(&third.challenge_id, wrong).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let fourth = codes.start(start(&locked)).await.unwrap();
    let code = outbox.last();
    let wrong = if code == "000000" { "111111" } else { "000000" };
    let _ = codes.verify(&fourth.challenge_id, wrong).await; // the 6th failure
    assert!(matches!(codes.verify(&fourth.challenge_id, &code).await, Err(AuthError::VerificationCodeInvalid)), "a right code is refused once locked");
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    assert!(matches!(codes.start(start(&locked)).await, Err(AuthError::VerificationRateLimited { retry_after_secs }) if retry_after_secs > 3_600));
}
