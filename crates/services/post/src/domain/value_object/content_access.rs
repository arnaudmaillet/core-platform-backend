/// What a reader may see of an author's content, as social-graph's access
/// check decides it (private profiles, blocks, hidden profiles).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentAccess {
    Visible,
    /// A private author the reader does not follow: no posts.
    HeaderOnly,
    /// A block either way, or a hidden author: nothing.
    Hidden,
}
