//! Existence-concealing access errors shared by the member-only operations.
//!
//! The hot member paths (`SendMessage`, `MarkRead`, `ListMembers`, the
//! Member-Plane stream and signals) authorize with a single roster read and do
//! not load the conversation. When that read misses, [`deny_non_member`] loads
//! the conversation **on the rejection path only** to pick the answer, so a
//! caller cannot tell a private conversation it is not in from one that does
//! not exist:
//!
//! | conversation | answer |
//! |---|---|
//! | missing | [`ChatError::ConversationNotFound`] |
//! | `Private` | [`ChatError::ConversationConcealed`] (same wire shape as not-found) |
//! | `Public` | [`ChatError::NotAMember`] (discoverable, so the precise error) |

use crate::application::port::ConversationRepository;
use crate::domain::value_object::{ConversationId, ProfileId};
use crate::error::ChatError;

/// The error for `profile_id`, whose roster lookup in `conversation_id` missed.
///
/// A storage fault while resolving the conversation is returned as-is (it is not
/// an access answer and must stay retryable).
pub async fn deny_non_member<CR>(
    conversation_repo: &CR,
    conversation_id:   &ConversationId,
    profile_id:        ProfileId,
) -> ChatError
where
    CR: ConversationRepository + ?Sized,
{
    match conversation_repo.find(conversation_id).await {
        Ok(Some(conversation)) => conversation.deny_outsider(profile_id),
        Ok(None) => ChatError::ConversationNotFound { conversation_id: conversation_id.as_str() },
        Err(e) => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::Fixture;

    #[tokio::test]
    async fn missing_private_and_public_conversations_answer_by_visibility() {
        let outsider = Fixture::profile();

        let missing = deny_non_member(&*Fixture::private_group().conversations, &ConversationId::new(), outsider).await;
        assert!(matches!(missing, ChatError::ConversationNotFound { .. }), "{missing:?}");

        let private = Fixture::private_group();
        let err = deny_non_member(&*private.conversations, &private.conversation_id, outsider).await;
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");

        let public = Fixture::public_group();
        let err = deny_non_member(&*public.conversations, &public.conversation_id, outsider).await;
        assert!(matches!(err, ChatError::NotAMember { .. }), "{err:?}");
    }
}
