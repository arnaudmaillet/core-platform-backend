//! A member mutes a conversation's pushes (#654): for a while, or until they
//! unmute. Only a member's own setting; the conversation is unchanged.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::access::deny_non_member;
use crate::application::port::{ConversationRepository, MemberRepository};
use crate::domain::aggregate::participant::muted_forever;
use crate::domain::value_object::{ConversationId, ProfileId};
use crate::error::ChatError;

/// Longest timed mute (longer: mute until unmuted).
pub const MAX_MUTE: Duration = Duration::days(366);

/// What the member asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mute {
    Off,
    Until(DateTime<Utc>),
    Forever,
}

pub struct MuteConversationCommand {
    pub conversation_id: String,
    pub member_id:       String,
    pub mute:            Mute,
}

impl Command for MuteConversationCommand {}

impl Validate for MuteConversationCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.conversation_id.trim().is_empty() {
            v.push(FieldViolation::new("conversation_id", "CHT-VAL-060", "conversation_id must not be empty"));
        }
        if self.member_id.trim().is_empty() {
            v.push(FieldViolation::new("member_id", "CHT-VAL-061", "member_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct MuteConversationHandler<CR, MR> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
}

impl<CR, MR> CommandHandler<MuteConversationCommand> for MuteConversationHandler<CR, MR>
where
    CR: ConversationRepository,
    MR: MemberRepository,
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<MuteConversationCommand>) -> Result<(), ChatError> {
        let cmd = &envelope.payload;
        let conversation_id = ConversationId::try_from(cmd.conversation_id.as_str())?;
        let member_id = ProfileId::try_from(cmd.member_id.as_str())?;
        let now = Utc::now();
        let muted_until = match cmd.mute {
            Mute::Off => None,
            Mute::Forever => Some(muted_forever()),
            Mute::Until(until) if until > now && until <= now + MAX_MUTE => Some(until),
            Mute::Until(_) => {
                return Err(ChatError::DomainViolation {
                    field:   "until".into(),
                    message: "a mute ends in the future, within a year (else: until unmuted)".into(),
                });
            }
        };
        if !self.member_repo.set_muted_until(&conversation_id, &member_id, muted_until).await? {
            return Err(deny_non_member(&*self.conversation_repo, &conversation_id, member_id).await);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::{FakeConversations, FakeMembers, Fixture};
    use crate::application::port::MemberRepository;

    async fn mute(f: &Fixture, member: ProfileId, mute: Mute) -> Result<(), ChatError> {
        let handler: MuteConversationHandler<FakeConversations, FakeMembers> = MuteConversationHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
        };
        let cmd = MuteConversationCommand {
            conversation_id: f.conversation_id.as_str(),
            member_id:       member.as_str(),
            mute,
        };
        handler.handle(Envelope::new(uuid::Uuid::now_v7(), cmd)).await
    }

    async fn muted_until(f: &Fixture, member: ProfileId) -> Option<DateTime<Utc>> {
        f.members.find(&f.conversation_id, &member).await.unwrap().unwrap().muted_until()
    }

    #[tokio::test]
    async fn a_member_mutes_for_a_while_or_for_good_then_unmutes() {
        let f = Fixture::private_group();
        let member = f.owner;
        let later = Utc::now() + Duration::hours(8);
        mute(&f, member, Mute::Until(later)).await.unwrap();
        assert_eq!(muted_until(&f, member).await, Some(later));
        mute(&f, member, Mute::Forever).await.unwrap();
        assert_eq!(muted_until(&f, member).await, Some(muted_forever()));
        mute(&f, member, Mute::Off).await.unwrap();
        assert_eq!(muted_until(&f, member).await, None);
    }

    #[tokio::test]
    async fn a_past_or_too_long_mute_is_refused_and_outsiders_cannot_mute() {
        let f = Fixture::private_group();
        for until in [Utc::now() - Duration::minutes(1), Utc::now() + Duration::days(400)] {
            assert!(matches!(mute(&f, f.owner, Mute::Until(until)).await, Err(ChatError::DomainViolation { .. })));
        }
        let outsider = Fixture::profile();
        assert!(mute(&f, outsider, Mute::Forever).await.is_err());
    }
}
