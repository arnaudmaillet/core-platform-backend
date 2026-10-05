use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{ConversationRepository, EventPublisher, MemberRepository};
use crate::domain::aggregate::{Conversation, Participant};
use crate::domain::value_object::{ConversationId, ConversationKind, ProfileId, Role};
use crate::error::ChatError;

/// Creates a new conversation owned by `owner_id`.
///
/// The `conversation_id` is minted at the edge (gRPC handler) as a UUIDv7 and
/// carried in the command, keeping the [`CommandHandler`] return type `()` while
/// letting the caller learn the id it generated. A `Channel` is born public; a
/// `Group` is born private (see [`Conversation::create`]).
pub struct CreateConversationCommand {
    pub conversation_id: String,
    pub kind:            i32,
    pub owner_id:        String,
}

impl Command for CreateConversationCommand {}

impl Validate for CreateConversationCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.conversation_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "conversation_id",
                "CHT-VAL-001",
                "conversation_id must not be empty",
            ));
        }
        if self.owner_id.trim().is_empty() {
            v.push(FieldViolation::new("owner_id", "CHT-VAL-002", "owner_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct CreateConversationHandler<CR, MR, EP> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
    pub publisher:         Arc<EP>,
}

impl<CR, MR, EP> CommandHandler<CreateConversationCommand>
    for CreateConversationHandler<CR, MR, EP>
where
    CR: ConversationRepository,
    MR: MemberRepository,
    EP: EventPublisher,
{
    type Error = ChatError;

    async fn handle(
        &self,
        envelope: Envelope<CreateConversationCommand>,
    ) -> Result<(), ChatError> {
        let cmd = &envelope.payload;

        let id       = ConversationId::try_from(cmd.conversation_id.as_str())?;
        let owner_id = ProfileId::try_from(cmd.owner_id.as_str())?;
        let kind     = ConversationKind::try_from(cmd.kind as i8)?;
        if kind == ConversationKind::Direct {
            // Unique per pair, with its peer's consent: OpenDirectConversation.
            return Err(ChatError::DomainViolation {
                field:   "kind".to_owned(),
                message: "open a direct conversation with OpenDirectConversation".to_owned(),
            });
        }

        let conversation = Conversation::create(id, kind, owner_id);
        let owner        = Participant::new(owner_id, Role::Owner)?;

        // Durable state first: the aggregate row, then the owner roster row.
        self.conversation_repo.insert(&conversation).await?;
        self.member_repo.insert(&id, &owner).await?;

        let mut conversation = conversation;
        for event in conversation.take_events() {
            self.publisher.publish_conversation(&event).await?;
        }

        Ok(())
    }
}
