/// Who is reading the map, taken from how the request arrived
/// (`transport::grpc::edge::viewer`), never from a request field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Viewer {
    /// A trusted in-cluster caller (the mesh): unfiltered.
    Internal,
    /// A client and the profile ids its account owns (empty when anonymous).
    Profiles(Vec<String>),
}

/// social-graph `CheckAccess`'s answer for one author.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentAccess {
    Visible,
    HeaderOnly,
    Hidden,
}

/// One author, as the reader stands to it: the content access, and whether
/// the reader follows it / is mutual with it (an author's location
/// audience, #657).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorAccess {
    pub content: ContentAccess,
    pub follows: bool,
    pub mutual:  bool,
}

impl AuthorAccess {
    pub fn visible(follows: bool, mutual: bool) -> Self {
        Self { content: ContentAccess::Visible, follows, mutual }
    }
}
