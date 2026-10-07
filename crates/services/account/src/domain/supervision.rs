//! Family supervision (#670): a parent pairs with a teen's account. Either
//! side creates an invite (a short code the other side enters or scans);
//! the other side accepts it. Both see the link; either ends it; it ends by
//! itself when the teen turns 18.

use chrono::{DateTime, Duration, Utc};
use rand::Rng;

use crate::domain::value_object::{AccountId, AgeBracket};
use crate::error::AccountError;

/// How long an invite can be accepted.
pub const INVITE_TTL: Duration = Duration::hours(24);
/// A teen has at most this many supervisors (two parents).
pub const MAX_SUPERVISORS: usize = 2;
/// Characters of an invite code (Crockford base32: no I, L, O, U).
const CODE_LEN: usize = 10;
const ALPHABET: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Which side of a supervision an account is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisionRole {
    Supervisor,
    Teen,
}

impl SupervisionRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Supervisor => "supervisor",
            Self::Teen => "teen",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "supervisor" => Some(Self::Supervisor),
            "teen" => Some(Self::Teen),
            _ => None,
        }
    }

    /// A supervisor is a known adult (18+ by date of birth); a teen is 13–17.
    /// An unknown age fits neither.
    pub fn fits(self, age: Option<AgeBracket>) -> bool {
        matches!(
            (self, age),
            (Self::Supervisor, Some(AgeBracket::Adult))
                | (Self::Teen, Some(AgeBracket::Teen13To15 | AgeBracket::Teen16To17))
        )
    }

    pub fn other(self) -> Self {
        match self {
            Self::Supervisor => Self::Teen,
            Self::Teen => Self::Supervisor,
        }
    }
}

/// An invite's code, as shown and entered: 10 characters of Crockford base32
/// (~50 bits), case-insensitive, separators ignored.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InviteCode(String);

impl InviteCode {
    pub fn generate() -> Self {
        let mut rng = rand::rng();
        Self((0..CODE_LEN).map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char).collect())
    }

    /// The code as typed: upper-cased, spaces and dashes dropped, and the
    /// look-alikes read as Crockford does (O → 0, I/L → 1).
    pub fn parse(typed: &str) -> Option<Self> {
        let code: String = typed
            .chars()
            .filter(|c| !matches!(c, ' ' | '-'))
            .map(|c| match c.to_ascii_uppercase() {
                'O' => '0',
                'I' | 'L' => '1',
                c => c,
            })
            .collect();
        (code.len() == CODE_LEN && code.bytes().all(|b| ALPHABET.contains(&b))).then_some(Self(code))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An invite, waiting for the other side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionInvite {
    pub code:       InviteCode,
    pub creator:    AccountId,
    /// The creator's side; the acceptor takes the other.
    pub role:       SupervisionRole,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl SupervisionInvite {
    /// `creator` (of age `age`) invites the other side.
    pub fn create(creator: AccountId, role: SupervisionRole, age: Option<AgeBracket>, now: DateTime<Utc>) -> Result<Self, AccountError> {
        if !role.fits(age) {
            return Err(AccountError::SupervisionRoleNotAllowed { role: role.as_str().to_owned() });
        }
        Ok(Self { code: InviteCode::generate(), creator, role, created_at: now, expires_at: now + INVITE_TTL })
    }

    /// `acceptor` (of age `age`) accepts: the pairing it makes. The invite
    /// must be live, from someone else, and the acceptor must fit the other
    /// side.
    pub fn accept(&self, acceptor: AccountId, age: Option<AgeBracket>, now: DateTime<Utc>) -> Result<Supervision, AccountError> {
        if now >= self.expires_at {
            return Err(AccountError::SupervisionInviteInvalid);
        }
        if acceptor == self.creator {
            return Err(AccountError::SelfSupervision);
        }
        let side = self.role.other();
        if !side.fits(age) {
            return Err(AccountError::SupervisionRoleNotAllowed { role: side.as_str().to_owned() });
        }
        let (teen, supervisor) = match self.role {
            SupervisionRole::Supervisor => (acceptor, self.creator),
            SupervisionRole::Teen => (self.creator, acceptor),
        };
        Ok(Supervision { teen, supervisor, since: now })
    }
}

/// A live supervision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Supervision {
    pub teen:       AccountId,
    pub supervisor: AccountId,
    pub since:      DateTime<Utc>,
}

impl Supervision {
    /// The other side of the link, seen from `account` (`None`: not theirs).
    pub fn counterpart(&self, account: &AccountId) -> Option<(SupervisionRole, AccountId)> {
        if *account == self.teen {
            Some((SupervisionRole::Supervisor, self.supervisor))
        } else if *account == self.supervisor {
            Some((SupervisionRole::Teen, self.teen))
        } else {
            None
        }
    }
}

/// Why a supervision ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisionEnd {
    /// The teen ended it (their supervisor is told).
    ByTeen,
    /// The supervisor ended it (the teen is told).
    BySupervisor,
    /// The teen turned 18.
    CameOfAge,
    /// One side's account was erased.
    AccountDeleted,
}

impl SupervisionEnd {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ByTeen => "by_teen",
            Self::BySupervisor => "by_supervisor",
            Self::CameOfAge => "came_of_age",
            Self::AccountDeleted => "account_deleted",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id() -> AccountId {
        AccountId::new()
    }

    #[test]
    fn codes_are_ten_crockford_characters_and_typing_is_forgiving() {
        let code = InviteCode::generate();
        assert_eq!(code.as_str().len(), 10);
        assert_eq!(InviteCode::parse(&code.as_str().to_lowercase()), Some(code.clone()));
        assert_eq!(InviteCode::parse("abcd-efgh 12").map(|c| c.0), Some("ABCDEFGH12".into()));
        assert_eq!(InviteCode::parse("0O1IL23456").map(|c| c.0), Some("0011123456".into()), "look-alikes");
        assert!(InviteCode::parse("ABCU123456").is_none(), "no U");
        assert!(InviteCode::parse("ABC").is_none());
    }

    #[test]
    fn a_parent_invites_a_teen_who_accepts() {
        let (parent, teen) = (id(), id());
        let now = Utc::now();
        let invite = SupervisionInvite::create(parent, SupervisionRole::Supervisor, Some(AgeBracket::Adult), now).unwrap();
        let link = invite.accept(teen, Some(AgeBracket::Teen16To17), now).unwrap();
        assert_eq!((link.teen, link.supervisor), (teen, parent));
        assert_eq!(link.counterpart(&teen), Some((SupervisionRole::Supervisor, parent)));
        assert_eq!(link.counterpart(&parent), Some((SupervisionRole::Teen, teen)));
    }

    #[test]
    fn a_teen_invites_a_parent_too() {
        let (parent, teen) = (id(), id());
        let now = Utc::now();
        let invite = SupervisionInvite::create(teen, SupervisionRole::Teen, Some(AgeBracket::Teen13To15), now).unwrap();
        assert_eq!(invite.accept(parent, Some(AgeBracket::Adult), now).unwrap().supervisor, parent);
    }

    #[test]
    fn ages_must_fit_and_an_invite_is_for_someone_else_while_it_lives() {
        let now = Utc::now();
        // A teen cannot supervise; an unknown age is neither side.
        for (role, age) in [(SupervisionRole::Supervisor, Some(AgeBracket::Teen16To17)), (SupervisionRole::Supervisor, None), (SupervisionRole::Teen, Some(AgeBracket::Adult)), (SupervisionRole::Teen, None)] {
            assert!(matches!(SupervisionInvite::create(id(), role, age, now), Err(AccountError::SupervisionRoleNotAllowed { .. })), "{role:?} {age:?}");
        }
        let parent = id();
        let invite = SupervisionInvite::create(parent, SupervisionRole::Supervisor, Some(AgeBracket::Adult), now).unwrap();
        assert!(matches!(invite.accept(id(), Some(AgeBracket::Adult), now), Err(AccountError::SupervisionRoleNotAllowed { .. })), "two adults");
        assert!(matches!(invite.accept(parent, Some(AgeBracket::Teen13To15), now), Err(AccountError::SelfSupervision)));
        assert!(matches!(invite.accept(id(), Some(AgeBracket::Teen13To15), now + INVITE_TTL), Err(AccountError::SupervisionInviteInvalid)), "expired");
    }
}
