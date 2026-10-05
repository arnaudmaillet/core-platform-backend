use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{ConversationRepository, MemberRepository, SubscriptionRepository};
use crate::domain::value_object::{ConversationId, ProfileId};
use crate::error::ChatError;

/// Subscribes a profile to the Audience Plane of a public conversation.
///
/// Audience subscription is a read-side concern: it never touches the aggregate
/// roster, so it carries no roster cap and emits no lifecycle event. It only
/// requires the conversation to be public: on a private one, a member gets the
/// precise [`ChatError::ConversationNotPublic`] while anyone else gets
/// [`ChatError::ConversationConcealed`] (indistinguishable from not-found).
pub struct SubscribeCommand {
    pub conversation_id: String,
    pub subscriber_id:   String,
}

impl Command for SubscribeCommand {}

impl Validate for SubscribeCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.conversation_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "conversation_id",
                "CHT-VAL-040",
                "conversation_id must not be empty",
            ));
        }
        if self.subscriber_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "subscriber_id",
                "CHT-VAL-041",
                "subscriber_id must not be empty",
            ));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct SubscribeHandler<CR, MR, SR> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
    pub subscription_repo: Arc<SR>,
}

impl<CR, MR, SR> CommandHandler<SubscribeCommand> for SubscribeHandler<CR, MR, SR>
where
    CR: ConversationRepository,
    MR: MemberRepository,
    SR: SubscriptionRepository,
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<SubscribeCommand>) -> Result<(), ChatError> {
        let cmd = &envelope.payload;

        let conversation_id = ConversationId::try_from(cmd.conversation_id.as_str())?;
        let subscriber_id   = ProfileId::try_from(cmd.subscriber_id.as_str())?;

        let conversation = self
            .conversation_repo
            .find(&conversation_id)
            .await?
            .ok_or_else(|| ChatError::ConversationNotFound {
                conversation_id: conversation_id.as_str(),
            })?;

        if !conversation.visibility().is_public() {
            // Only a member already knows this conversation exists.
            let is_member = self.member_repo.find(&conversation_id, &subscriber_id).await?.is_some();
            return Err(if is_member {
                ChatError::ConversationNotPublic { conversation_id: conversation_id.as_str() }
            } else {
                conversation.concealed()
            });
        }

        self.subscription_repo.subscribe(&conversation_id, &subscriber_id).await
    }
}

/// Removes an Audience-Plane subscription. Idempotent; requires no visibility
/// check (a profile may always unsubscribe).
pub struct UnsubscribeCommand {
    pub conversation_id: String,
    pub subscriber_id:   String,
}

impl Command for UnsubscribeCommand {}

impl Validate for UnsubscribeCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.conversation_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "conversation_id",
                "CHT-VAL-042",
                "conversation_id must not be empty",
            ));
        }
        if self.subscriber_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "subscriber_id",
                "CHT-VAL-043",
                "subscriber_id must not be empty",
            ));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct UnsubscribeHandler<SR> {
    pub subscription_repo: Arc<SR>,
}

impl<SR> CommandHandler<UnsubscribeCommand> for UnsubscribeHandler<SR>
where
    SR: SubscriptionRepository,
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<UnsubscribeCommand>) -> Result<(), ChatError> {
        let cmd = &envelope.payload;

        let conversation_id = ConversationId::try_from(cmd.conversation_id.as_str())?;
        let subscriber_id   = ProfileId::try_from(cmd.subscriber_id.as_str())?;

        self.subscription_repo.unsubscribe(&conversation_id, &subscriber_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::{
        FakeConversations, FakeMembers, FakeSubscriptions, Fixture,
    };
    use crate::domain::value_object::Role;

    async fn subscribe(
        f: &Fixture,
        subscriptions: &Arc<FakeSubscriptions>,
        subscriber: ProfileId,
    ) -> Result<(), ChatError> {
        let handler: SubscribeHandler<FakeConversations, FakeMembers, FakeSubscriptions> = SubscribeHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
            subscription_repo: Arc::clone(subscriptions),
        };
        handler
            .handle(Envelope::new(uuid::Uuid::now_v7(), SubscribeCommand {
                conversation_id: f.conversation_id.as_str(),
                subscriber_id:   subscriber.as_str(),
            }))
            .await
    }

    #[tokio::test]
    async fn outsider_subscribing_to_a_private_conversation_is_concealed() {
        let f = Fixture::private_group();
        let subscriptions = Arc::default();
        let outsider = Fixture::profile();

        let err = subscribe(&f, &subscriptions, outsider).await.unwrap_err();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
        assert!(!subscriptions.has(&f.conversation_id, &outsider));
    }

    #[tokio::test]
    async fn member_subscribing_to_a_private_conversation_gets_not_public() {
        let f = Fixture::private_group();
        let member = f.add_member(Role::Member);

        let err = subscribe(&f, &Arc::default(), member).await.unwrap_err();
        assert!(matches!(err, ChatError::ConversationNotPublic { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn anyone_may_subscribe_to_a_public_conversation() {
        let f = Fixture::public_group();
        let subscriptions = Arc::default();
        let outsider = Fixture::profile();

        subscribe(&f, &subscriptions, outsider).await.unwrap();
        assert!(subscriptions.has(&f.conversation_id, &outsider));
    }
}
