//! What a post's owner hides from the comments on their posts (#660): their
//! hidden words and, unless turned off, the offensive-term list. The matching
//! is shared with chat's message requests (#810): see [`text_filter`].

pub use text_filter::{ContentFilter as CommentFilter, TermList};
