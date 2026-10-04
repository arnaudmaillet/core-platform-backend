use std::sync::Arc;

use async_trait::async_trait;
use fred::interfaces::KeysInterface;
use fred::types::{Expiration, SetOptions};
use redis_storage::RedisClient;

use crate::application::port::ClassifierGateway;
use crate::domain::value_object::SubjectRef;
use crate::error::ModerationError;

use super::keys::classification_debounce_key;

/// Wraps a [`ClassifierGateway`] so one subject is sent to classification at most
/// once per window, however many reports arrive (a report flood from many guest
/// sessions must not multiply classifier work). One `SET NX EX` per request,
/// slot-local.
///
/// **Fails open**: if Redis does not answer, the request goes through (and is
/// logged) — a classifier run is cheaper than missing one.
pub struct DebouncedClassifierGateway {
    inner:       Arc<dyn ClassifierGateway>,
    client:      RedisClient,
    window_secs: i64,
}

impl DebouncedClassifierGateway {
    pub fn new(inner: Arc<dyn ClassifierGateway>, client: RedisClient, window_secs: u64) -> Self {
        Self { inner, client, window_secs: window_secs.max(1) as i64 }
    }
}

#[async_trait]
impl ClassifierGateway for DebouncedClassifierGateway {
    async fn request_classification(&self, subject: &SubjectRef) -> Result<(), ModerationError> {
        let key = classification_debounce_key(subject.entity_type().as_str(), subject.entity_id());
        let first: Result<Option<String>, _> = self
            .client
            .inner
            .set(&key, "1", Some(Expiration::EX(self.window_secs)), Some(SetOptions::NX), false)
            .await;
        match first {
            Ok(Some(_)) => self.inner.request_classification(subject).await,
            Ok(None) => Ok(()), // already requested within the window
            Err(e) => {
                tracing::warn!(error = %e, "classification debounce unavailable; requesting anyway (fail-open)");
                self.inner.request_classification(subject).await
            }
        }
    }
}
