//! What a push says (#654): the preference category that gates it, and an alert
//! the app localizes (`loc-key` + `loc-args`), so the text follows the
//! device's language.

use crate::domain::preferences::PushCategory;
use crate::domain::value_object::NotificationKind;

/// Longest sender name put in an alert (a display name is longer only when
/// malformed; it is cut, not refused).
pub const MAX_NAME_CHARS: usize = 64;

impl NotificationKind {
    /// The category whose preference gates a push of this kind; `None`: never
    /// pushed, the feed only (appeal outcomes, until their recipient is decided).
    pub fn push_category(self) -> Option<PushCategory> {
        match self {
            Self::Reaction => Some(PushCategory::Likes),
            Self::Comment | Self::Reply => Some(PushCategory::Comments),
            Self::Mention => Some(PushCategory::Mentions),
            Self::Follow => Some(PushCategory::NewFollowers),
            Self::FollowRequest | Self::FollowAccepted => Some(PushCategory::FollowRequests),
            Self::AppealUpheld | Self::AppealOverturned => None,
            // Feed only: no preference category covers supervision notices.
            Self::SupervisionStarted | Self::SupervisionEnded | Self::SupervisionCameOfAge => None,
        }
    }

    /// The alert's localization key stem (`NTF_PUSH_REACTION`, …).
    fn loc_stem(self) -> &'static str {
        match self {
            Self::Reaction => "NTF_PUSH_REACTION",
            Self::Comment => "NTF_PUSH_COMMENT",
            Self::Reply => "NTF_PUSH_REPLY",
            Self::Mention => "NTF_PUSH_MENTION",
            Self::Follow => "NTF_PUSH_FOLLOW",
            Self::FollowRequest => "NTF_PUSH_FOLLOW_REQUEST",
            Self::FollowAccepted => "NTF_PUSH_FOLLOW_ACCEPTED",
            Self::AppealUpheld => "NTF_PUSH_APPEAL_UPHELD",
            Self::AppealOverturned => "NTF_PUSH_APPEAL_OVERTURNED",
            Self::SupervisionStarted => "NTF_PUSH_SUPERVISION_STARTED",
            Self::SupervisionEnded => "NTF_PUSH_SUPERVISION_ENDED",
            Self::SupervisionCameOfAge => "NTF_PUSH_SUPERVISION_CAME_OF_AGE",
        }
    }
}

/// One push, the same for every device of the recipient.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PushMessage {
    /// A plain title (a chat message: its sender's name).
    pub title:           Option<String>,
    /// A localized title, when there is no plain one.
    pub title_loc_key:   Option<&'static str>,
    /// A plain body (a chat message: its text).
    pub body:            Option<String>,
    /// A localized body: `NTF_PUSH_<KIND>` (args: the sender's name),
    /// `…_OTHERS` (args: the name, how many others) or `…_ANON` (no args: the
    /// name is unknown); `NTF_PUSH_MESSAGE_MEDIA` for an attachment.
    pub loc_key:         Option<String>,
    pub loc_args:        Vec<String>,
    /// The unread count, for the app icon.
    pub badge:           Option<u32>,
    /// Groups a subject's pushes on the lock screen.
    pub thread_id:       String,
    /// A newer push about the same notification replaces the older one.
    pub collapse_id:     String,
    /// The notification's id; a chat message's id for `kind = message`.
    pub notification_id: String,
    pub kind:            &'static str,
    pub subject_kind:    &'static str,
    pub subject_id:      String,
}

/// What a push is built from.
#[derive(Debug, Clone)]
pub struct PushSubject<'a> {
    pub notification_id: String,
    pub kind:            NotificationKind,
    pub subject_kind:    &'static str,
    pub subject_id:      String,
    pub sender_count:    i32,
    pub sender_name:     Option<&'a str>,
    pub badge:           Option<u32>,
}

impl PushMessage {
    pub fn new(subject: PushSubject<'_>) -> Self {
        let stem = subject.kind.loc_stem();
        let name = clean_name(subject.sender_name);
        let others = subject.sender_count.saturating_sub(1);
        let (loc_key, loc_args) = match name {
            None => (format!("{stem}_ANON"), Vec::new()),
            Some(name) if others > 0 => (format!("{stem}_OTHERS"), vec![name, others.to_string()]),
            Some(name) => (stem.to_owned(), vec![name]),
        };
        Self {
            loc_key: Some(loc_key),
            loc_args,
            badge: subject.badge,
            thread_id: subject.subject_id.clone(),
            collapse_id: subject.notification_id.clone(),
            notification_id: subject.notification_id,
            kind: subject.kind.as_str(),
            subject_kind: subject.subject_kind,
            subject_id: subject.subject_id,
            ..Self::default()
        }
    }

    /// A chat message's push (#654): its sender as the title, its text as the
    /// body (an attachment: `NTF_PUSH_MESSAGE_MEDIA`). No badge: the unread
    /// count is the activity feed's, which a message does not change.
    pub fn chat_message(message: ChatMessage<'_>) -> Self {
        let name = clean_name(message.sender_name);
        let text = message.preview.trim();
        let (body, loc_key) = if message.media || text.is_empty() {
            (None, Some("NTF_PUSH_MESSAGE_MEDIA".to_owned()))
        } else {
            (Some(text.to_owned()), None)
        };
        Self {
            title_loc_key: name.is_none().then_some("NTF_PUSH_MESSAGE_ANON"),
            title: name,
            body,
            loc_key,
            thread_id: message.conversation_id.to_owned(),
            collapse_id: message.message_id.to_owned(),
            notification_id: message.message_id.to_owned(),
            kind: "message",
            subject_kind: "conversation",
            subject_id: message.conversation_id.to_owned(),
            ..Self::default()
        }
    }
}

/// What a chat message's push is built from.
#[derive(Debug, Clone, Copy)]
pub struct ChatMessage<'a> {
    pub message_id:      &'a str,
    pub conversation_id: &'a str,
    pub media:           bool,
    pub preview:         &'a str,
    pub sender_name:     Option<&'a str>,
}

fn clean_name(name: Option<&str>) -> Option<String> {
    name.map(|n| n.trim().chars().take(MAX_NAME_CHARS).collect::<String>()).filter(|n| !n.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subject(kind: NotificationKind, name: Option<&str>, count: i32) -> PushSubject<'_> {
        PushSubject {
            notification_id: "n".into(),
            kind,
            subject_kind: "post",
            subject_id: "s".into(),
            sender_count: count,
            sender_name: name,
            badge: Some(3),
        }
    }

    #[test]
    fn the_alert_names_the_sender_and_counts_the_others() {
        let one = PushMessage::new(subject(NotificationKind::Reaction, Some("Alice"), 1));
        assert_eq!((one.loc_key.as_deref(), one.loc_args.clone()), (Some("NTF_PUSH_REACTION"), vec!["Alice".to_owned()]));
        let many = PushMessage::new(subject(NotificationKind::Reaction, Some("Alice"), 13));
        assert_eq!(many.loc_key.as_deref(), Some("NTF_PUSH_REACTION_OTHERS"));
        assert_eq!(many.loc_args, vec!["Alice".to_owned(), "12".to_owned()]);
        let anon = PushMessage::new(subject(NotificationKind::FollowRequest, Some("  "), 1));
        assert_eq!((anon.loc_key.as_deref(), anon.loc_args.len()), (Some("NTF_PUSH_FOLLOW_REQUEST_ANON"), 0));
        let long = "x".repeat(200);
        assert_eq!(PushMessage::new(subject(NotificationKind::Follow, Some(&long), 1)).loc_args[0].len(), MAX_NAME_CHARS);
        assert_eq!((one.thread_id.as_str(), one.collapse_id.as_str(), one.badge), ("s", "n", Some(3)));
    }

    #[test]
    fn each_kind_has_its_category() {
        assert_eq!(NotificationKind::Reaction.push_category(), Some(PushCategory::Likes));
        assert_eq!(NotificationKind::Reply.push_category(), Some(PushCategory::Comments));
        assert_eq!(NotificationKind::FollowAccepted.push_category(), Some(PushCategory::FollowRequests));
        assert_eq!(NotificationKind::AppealUpheld.push_category(), None, "feed only");
    }

    #[test]
    fn a_chat_message_shows_its_sender_and_text() {
        let chat = |media: bool, preview: &'static str, name: Option<&'static str>| {
            PushMessage::chat_message(ChatMessage {
                message_id: "m",
                conversation_id: "c",
                media,
                preview,
                sender_name: name,
            })
        };
        let text = chat(false, "on se voit ce soir ?", Some("Alice"));
        assert_eq!((text.title.as_deref(), text.body.as_deref()), (Some("Alice"), Some("on se voit ce soir ?")));
        assert_eq!((text.loc_key, text.title_loc_key, text.badge), (None, None, None));
        assert_eq!((text.kind, text.subject_kind, text.subject_id.as_str(), text.thread_id.as_str()), ("message", "conversation", "c", "c"));
        assert_eq!((text.notification_id.as_str(), text.collapse_id.as_str()), ("m", "m"));

        let media = chat(true, "", Some("Alice"));
        assert_eq!((media.body, media.loc_key.as_deref()), (None, Some("NTF_PUSH_MESSAGE_MEDIA")));
        let anon = chat(false, "salut", None);
        assert_eq!((anon.title, anon.title_loc_key), (None, Some("NTF_PUSH_MESSAGE_ANON")));
    }
}
