//! In-process capturing fake for the event-publisher dependency.
//!
//! Post's [`EventPublisher`] port is the seam to the Kafka backbone. Rather than
//! boot a broker, the suite injects [`CapturingPublisher`]: it records a label for
//! every emitted [`DomainEvent`] so a scenario can assert that, e.g., publishing a
//! post emitted exactly one `PostPublished`.

use std::sync::Mutex;

use async_trait::async_trait;

use post::application::port::EventPublisher;
use post::domain::event::DomainEvent;
use post::error::PostError;

/// A capturing, never-failing stand-in for the Kafka event publisher.
#[derive(Default)]
pub struct CapturingPublisher {
    labels: Mutex<Vec<String>>,
}

impl CapturingPublisher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of the event labels emitted so far, in order.
    pub fn labels(&self) -> Vec<String> {
        self.labels.lock().unwrap().clone()
    }

    /// How many times `label` has been emitted.
    pub fn count(&self, label: &str) -> usize {
        self.labels.lock().unwrap().iter().filter(|l| l.as_str() == label).count()
    }
}

#[async_trait]
impl EventPublisher for CapturingPublisher {
    async fn publish(&self, event: &DomainEvent) -> Result<(), PostError> {
        let label = match event {
            DomainEvent::PostPublished(_) => "published",
            DomainEvent::PostUpdated(_) => "updated",
            DomainEvent::PostDeleted(_) => "deleted",
        };
        self.labels.lock().unwrap().push(label.to_owned());
        Ok(())
    }
}

/// A scriptable stand-in for social-graph's audience check: every author is
/// `Visible` unless scripted otherwise, and the gate can be made to fail.
#[derive(Default)]
pub struct ScriptedGate {
    access: Mutex<std::collections::HashMap<String, post::domain::value_object::ContentAccess>>,
    /// Profiles that take no mentions (#656).
    no_mentions: Mutex<std::collections::HashSet<String>>,
    down:   Mutex<bool>,
}

impl ScriptedGate {
    pub fn set(&self, author: &str, access: post::domain::value_object::ContentAccess) {
        self.access.lock().unwrap().insert(author.to_owned(), access);
    }

    /// `profile` takes no mentions.
    pub fn refuse_mentions(&self, profile: &str) {
        self.no_mentions.lock().unwrap().insert(profile.to_owned());
    }

    pub fn set_down(&self, down: bool) {
        *self.down.lock().unwrap() = down;
    }
}

#[async_trait]
impl post::application::port::AudienceGate for ScriptedGate {
    async fn access(
        &self,
        _viewers: &[post::domain::value_object::ProfileId],
        author: &post::domain::value_object::ProfileId,
    ) -> Result<post::domain::value_object::ContentAccess, PostError> {
        if *self.down.lock().unwrap() {
            return Err(PostError::AccessCheckUnavailable { reason: "scripted outage".into() });
        }
        Ok(self
            .access
            .lock()
            .unwrap()
            .get(&author.as_str())
            .copied()
            .unwrap_or(post::domain::value_object::ContentAccess::Visible))
    }

    async fn may_mention(
        &self,
        _author: &post::domain::value_object::ProfileId,
        mentioned: &post::domain::value_object::ProfileId,
    ) -> Result<bool, PostError> {
        if *self.down.lock().unwrap() {
            return Err(PostError::AccessCheckUnavailable { reason: "scripted outage".into() });
        }
        Ok(!self.no_mentions.lock().unwrap().contains(&mentioned.as_str()))
    }
}
