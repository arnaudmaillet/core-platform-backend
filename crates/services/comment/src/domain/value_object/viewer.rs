use super::ProfileId;

/// Who is reading comments, taken from how the request arrived
/// (`transport::grpc::edge::viewer`), never from a request field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Viewer {
    /// A trusted in-cluster caller (the mesh): sees everything, unchecked.
    Internal,
    /// A client and the profiles its account owns (empty when anonymous).
    Profiles(Vec<ProfileId>),
}
