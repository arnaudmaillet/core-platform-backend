//! The archive's files from the other services (#653): per profile, its
//! profile, posts, comments, social graph and conversations; for the account,
//! its likes (#665) and media. Conversations follow the holder's choice: a
//! direct (one-to-one) conversation is exported in full (it is between the
//! two of them); in a group or channel, only the holder's own messages — the
//! others' are placeholders (what they wrote is another member's data). Being
//! direct is the conversation's kind, never its current roster size. A group
//! the holder left (#656) is in too: its own messages up to the departure.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::json;

use crate::application::command::EXPORT_LINK_TTL_DAYS;
use crate::application::port::{ExportFile, ExportPeers, ExportSources, MessageExport};
use crate::domain::value_object::AccountId;
use crate::error::AccountError;

/// [`ExportSources`] over the other services.
pub struct PeerExportSources {
    peers: Arc<dyn ExportPeers>,
}

impl PeerExportSources {
    pub fn new(peers: Arc<dyn ExportPeers>) -> Self {
        Self { peers }
    }
}

#[async_trait]
impl ExportSources for PeerExportSources {
    async fn gather(&self, account_id: &AccountId, _now: DateTime<Utc>) -> Result<Vec<ExportFile>, AccountError> {
        let mut files = Vec::new();
        for (profile_id, profile) in self.peers.profiles(account_id).await? {
            let dir = format!("profiles/{profile_id}");
            files.push(ExportFile::json(format!("{dir}/profile.json"), &profile));
            files.push(ExportFile::json(format!("{dir}/posts.json"), &json!(self.peers.posts(&profile_id).await?)));
            files.push(ExportFile::json(format!("{dir}/comments.json"), &json!(self.peers.comments(&profile_id).await?)));
            files.push(ExportFile::json(
                format!("{dir}/recent_searches.json"),
                &json!(self.peers.recent_searches(&profile_id).await?),
            ));
            files.push(ExportFile::json(format!("{dir}/social.json"), &self.peers.social(&profile_id).await?));
            for conversation in self.peers.conversations(&profile_id).await? {
                // A left conversation's roster is no longer the holder's to read.
                let members = if conversation.left {
                    None
                } else {
                    Some(self.peers.members(&conversation.conversation_id, &profile_id).await?.len())
                };
                let messages = self.peers.messages(&conversation, &profile_id).await?;
                files.push(ExportFile::json(
                    format!("{dir}/conversations/{}.json", conversation.conversation_id),
                    &json!({
                        "conversation_id": conversation.conversation_id,
                        "membership": conversation.membership,
                        "direct": conversation.direct,
                        "left": conversation.left,
                        "members": members,
                        "messages": shown(messages, &profile_id, conversation.direct),
                    }),
                ));
            }
        }
        files.push(ExportFile::json("likes.json", &json!(self.peers.likes(account_id).await?)));
        let media = self.peers.media(account_id, Duration::days(EXPORT_LINK_TTL_DAYS)).await?;
        files.push(ExportFile::json("media.json", &json!(media)));
        Ok(files)
    }
}

/// The messages as the archive shows them: all of a direct conversation;
/// elsewhere the holder's own, the others as placeholders.
fn shown(messages: Vec<MessageExport>, holder: &str, direct: bool) -> Vec<serde_json::Value> {
    messages
        .into_iter()
        .map(|m| {
            if direct || m.sender_id == holder {
                m.message
            } else {
                json!({ "from": "another member", "created_at_ms": m.created_at_ms })
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::port::ConversationExport;

    /// One profile in a direct conversation, a group, a group that shrank to
    /// two and a two-member channel.
    struct Peers;

    fn message(sender: &str, at: i64, body: &str) -> MessageExport {
        MessageExport { sender_id: sender.into(), created_at_ms: at, message: json!({ "sender_id": sender, "body": body }) }
    }

    #[async_trait]
    impl ExportPeers for Peers {
        async fn profiles(&self, _: &AccountId) -> Result<Vec<(String, serde_json::Value)>, AccountError> {
            Ok(vec![("me".into(), json!({ "handle": "me" }))])
        }
        async fn posts(&self, _: &str) -> Result<Vec<serde_json::Value>, AccountError> {
            Ok(vec![json!({ "caption": "hello" })])
        }
        async fn comments(&self, _: &str) -> Result<Vec<serde_json::Value>, AccountError> {
            Ok(vec![])
        }
        async fn recent_searches(&self, _: &str) -> Result<Vec<serde_json::Value>, AccountError> {
            Ok(vec![json!({ "query": "paris food" })])
        }
        async fn social(&self, _: &str) -> Result<serde_json::Value, AccountError> {
            Ok(json!({ "following": [], "followers": [], "blocks": [] }))
        }
        async fn conversations(&self, _: &str) -> Result<Vec<ConversationExport>, AccountError> {
            Ok([("dm", true), ("group", false), ("shrunk", false), ("channel", false), ("left", false)]
                .map(|(id, direct)| ConversationExport {
                    conversation_id: id.into(),
                    membership: json!({ "role": "MEMBER" }),
                    direct,
                    left: id == "left",
                })
                .to_vec())
        }
        async fn members(&self, conversation: &str, _: &str) -> Result<Vec<String>, AccountError> {
            assert_ne!(conversation, "left", "a left conversation's roster is never read");
            Ok(match conversation {
                "dm" | "shrunk" | "channel" => vec!["me".into(), "friend".into()],
                _ => vec!["me".into(), "a".into(), "b".into()],
            })
        }
        async fn messages(&self, conversation: &ConversationExport, _: &str) -> Result<Vec<MessageExport>, AccountError> {
            if conversation.left {
                // As chat's GetFormerMemberHistory gives it: others reduced.
                return Ok(vec![message("me", 1, "before I left"), message("", 2, "")]);
            }
            Ok(vec![message("me", 1, "hi"), message("friend", 2, "their secret"), message("gone", 3, "departed")])
        }
        async fn likes(&self, _: &AccountId) -> Result<Vec<serde_json::Value>, AccountError> {
            Ok(vec![json!({ "target": { "post_id": "p1" }, "total": 12 })])
        }
        async fn media(&self, _: &AccountId, ttl: Duration) -> Result<Vec<serde_json::Value>, AccountError> {
            Ok(vec![json!({ "ttl_days": ttl.num_days() })])
        }
    }

    fn file(files: &[ExportFile], path: &str) -> serde_json::Value {
        let file = files.iter().find(|f| f.path == path).unwrap_or_else(|| panic!("{path} missing"));
        serde_json::from_slice(&file.content).unwrap()
    }

    #[tokio::test]
    async fn the_archive_lays_out_each_profile_and_shields_group_members() {
        let files = PeerExportSources::new(Arc::new(Peers))
            .gather(&AccountId::new(), Utc::now())
            .await
            .unwrap();
        assert_eq!(file(&files, "profiles/me/posts.json")[0]["caption"], "hello");
        assert_eq!(file(&files, "likes.json")[0]["total"], 12, "the account's likes, once");
        assert!(files.iter().all(|f| !f.path.ends_with("reactions.json")));
        assert_eq!(file(&files, "profiles/me/recent_searches.json")[0]["query"], "paris food");
        assert!(file(&files, "profiles/me/social.json")["blocks"].is_array());
        assert_eq!(file(&files, "media.json")[0]["ttl_days"], 7, "links valid 7 days");

        let dm = file(&files, "profiles/me/conversations/dm.json");
        assert_eq!(dm["direct"], true);
        assert_eq!(dm["messages"][1]["body"], "their secret", "a direct conversation in full");

        // Two members left is not one-to-one: a shrunk group keeps the departed
        // member's words, a channel is a channel.
        for shielded in ["group", "shrunk", "channel"] {
            let conversation = file(&files, &format!("profiles/me/conversations/{shielded}.json"));
            assert_eq!(conversation["direct"], false, "{shielded}");
            assert_eq!(conversation["messages"][0]["body"], "hi", "the holder's own");
            assert_eq!(conversation["messages"][1], json!({ "from": "another member", "created_at_ms": 2 }));
            assert_eq!(conversation["messages"][2], json!({ "from": "another member", "created_at_ms": 3 }));
            let text = conversation.to_string();
            assert!(!text.contains("their secret") && !text.contains("departed"), "{shielded}: others' words never leave");
        }

        // A group the holder left: its own messages, others' as placeholders,
        // no roster read.
        let left = file(&files, "profiles/me/conversations/left.json");
        assert_eq!(left["left"], true);
        assert!(left["members"].is_null());
        assert_eq!(left["messages"][0]["body"], "before I left");
        assert_eq!(left["messages"][1], json!({ "from": "another member", "created_at_ms": 2 }));
    }
}
