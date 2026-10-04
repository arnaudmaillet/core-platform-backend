//! Who is searching, and what social-graph's access check says they may see
//! of an author. Search filters its hits with it; it does not own the rule.

/// The reader, taken from how the request arrived (`edge::viewer`).
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
    /// A private author the reader does not follow: their profile is still
    /// findable, their posts are not.
    HeaderOnly,
    /// A block either way, or a hidden profile: nothing.
    Hidden,
}
