use crate::domain::value_object::{AccountId, SessionId};

/// The account's revocation-generation counter. The layout is shared with the
/// client edge of every service (`auth_context::edge::revocation`, #884).
pub fn generation_key(account_id: &AccountId) -> String {
    auth_context::edge::revocation::generation_key(&account_id.as_str())
}

/// Per-session blacklist marker. Set with a TTL equal to the access-token window
/// so it self-evicts once no live edge token could reference the session.
pub fn blacklist_key(session_id: &SessionId) -> String {
    auth_context::edge::revocation::blacklist_key(&session_id.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn keys_carry_hash_tags() {
        let acct = AccountId::from_uuid(Uuid::nil());
        let sess = SessionId::from_uuid(Uuid::nil());
        assert!(generation_key(&acct).contains("{acct:"));
        assert!(blacklist_key(&sess).contains("{sess:"));
        assert!(generation_key(&acct).ends_with(":gen"));
        assert!(blacklist_key(&sess).ends_with(":revoked"));
    }
}
