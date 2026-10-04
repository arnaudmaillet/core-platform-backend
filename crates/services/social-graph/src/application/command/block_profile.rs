use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, SocialGraphCache, SocialGraphRepository};
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

#[derive(Debug, Clone)]
pub struct BlockProfileCommand {
    pub actor_id:  String,
    pub target_id: String,
}

impl Command for BlockProfileCommand {}

impl Validate for BlockProfileCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.actor_id.trim().is_empty() {
            v.push(FieldViolation::new("actor_id", "VAL-4001", "actor_id must not be empty"));
        }
        if self.target_id.trim().is_empty() {
            v.push(FieldViolation::new("target_id", "VAL-4002", "target_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct BlockProfileHandler {
    repo:      Arc<dyn SocialGraphRepository>,
    cache:     Arc<dyn SocialGraphCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl BlockProfileHandler {
    pub fn new(
        repo:      Arc<dyn SocialGraphRepository>,
        cache:     Arc<dyn SocialGraphCache>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { repo, cache, publisher }
    }
}

impl CommandHandler<BlockProfileCommand> for BlockProfileHandler {
    type Error = SocialGraphError;

    async fn handle(
        &self,
        envelope: Envelope<BlockProfileCommand>,
    ) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;

        let actor_id  = ProfileId::try_from(cmd.actor_id.as_str())?;
        let target_id = ProfileId::try_from(cmd.target_id.as_str())?;

        if actor_id == target_id {
            return Err(SocialGraphError::SelfInteraction);
        }

        let mut relation = self.repo.load_relation(&actor_id, &target_id).await?;

        // `block()` returns which follows are severed (with their timestamps).
        let severed = relation.block()?;
        let now     = chrono::Utc::now();

        // Persist the block first, then sever any existing follows in parallel.
        self.repo.persist_block(&actor_id, &target_id, now).await?;

        let (r1, r2) = tokio::join!(
            async {
                if let Some(ts) = severed.actor_to_target {
                    self.repo.delete_follow(&actor_id, &target_id, ts).await?;
                    let _ = self.cache.remove_following(&actor_id, &target_id).await;
                    let _ = self.cache.decr_followers_count(&target_id).await;
                    let _ = self.cache.decr_following_count(&actor_id).await;
                }
                Ok::<(), SocialGraphError>(())
            },
            async {
                if let Some(ts) = severed.target_to_actor {
                    self.repo.delete_follow(&target_id, &actor_id, ts).await?;
                    let _ = self.cache.remove_following(&target_id, &actor_id).await;
                    let _ = self.cache.decr_followers_count(&actor_id).await;
                    let _ = self.cache.decr_following_count(&target_id).await;
                }
                Ok::<(), SocialGraphError>(())
            },
        );
        r1?;
        r2?;

        // Pending requests either way are dropped too (a block gates new ones).
        if let Some(at) = severed.actor_request {
            self.repo.delete_follow_request(&actor_id, &target_id, at).await?;
        }
        if let Some(at) = severed.target_request {
            self.repo.delete_follow_request(&target_id, &actor_id, at).await?;
        }

        let _ = self.cache.add_block(&actor_id, &target_id).await;

        for event in relation.take_events() {
            let _ = self.publisher.publish(&event).await;
        }

        Ok(())
    }
}
