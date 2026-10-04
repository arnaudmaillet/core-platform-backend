/// Who is reading a profile, taken from how the request arrived
/// (`transport::grpc::edge::viewer`), never from a request field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Viewer {
    /// A trusted in-cluster caller (the mesh): sees every profile as stored.
    Internal,
    /// A client with no identity.
    Anonymous,
    /// A client signed in to `account_id`.
    Account(String),
}

impl Viewer {
    /// `true` when the viewer sees everything about a profile owned by
    /// `account_id`: its owner, or a trusted internal caller.
    pub fn sees_everything_of(&self, account_id: &str) -> bool {
        match self {
            Self::Internal => true,
            Self::Account(own) => own == account_id,
            Self::Anonymous => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_owner_and_the_mesh_see_everything() {
        assert!(Viewer::Internal.sees_everything_of("acct-1"));
        assert!(Viewer::Account("acct-1".into()).sees_everything_of("acct-1"));
        assert!(!Viewer::Account("acct-2".into()).sees_everything_of("acct-1"));
        assert!(!Viewer::Anonymous.sees_everything_of("acct-1"));
    }
}
