//! The archive's files from the other services (#653): per profile, its
//! profile, posts, comments, reactions, social graph and conversations; for
//! the account, its media. Conversations follow the holder's choice: a
//! one-to-one conversation is exported in full (it is between the two of
//! them); in a group or channel, only the holder's own messages — the
//! others' are placeholders (who wrote them is another member's data).

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
            files.push(ExportFile::json(format!("{dir}/reactions.json"), &json!(self.peers.reactions(&profile_id).await?)));
            files.push(ExportFile::json(format!("{dir}/social.json"), &self.peers.social(&profile_id).await?));
            for conversation in self.peers.conversations(&profile_id).await? {
                let members = self.peers.members(&conversation.conversation_id, &profile_id).await?;
                let messages = self.peers.messages(&conversation.conversation_id, &profile_id).await?;
                let one_to_one = members.len() == 2;
                files.push(ExportFile::json(
                    format!("{dir}/conversations/{}.json", conversation.conversation_id),
                    &json!({
                        "conversation_id": conversation.conversation_id,
                        "membership": conversation.membership,
                        "one_to_one": one_to_one,
                        "members": members.len(),
                        "messages": shown(messages, &profile_id, one_to_one),
                    }),
                ));
            }
        }
        let media = self.peers.media(account_id, Duration::days(EXPORT_LINK_TTL_DAYS)).await?;
        files.push(ExportFile::json("media.json", &json!(media)));
        Ok(files)
    }
}

/// The messages as the archive shows them: all of a one-to-one
/// conversation; elsewhere the holder's own, the others as placeholders.
fn shown(messages: Vec<MessageExport>, holder: &str, one_to_one: bool) -> Vec<serde_json::Value> {
    messages
        .into_iter()
        .map(|m| {
            if one_to_one || m.sender_id == holder {
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

    /// One profile in a one-to-one and in a group conversation.
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
        async fn reactions(&self, _: &str) -> Result<Vec<serde_json::Value>, AccountError> {
            Ok(vec![json!({ "kind": "HEART" })])
        }
        async fn social(&self, _: &str) -> Result<serde_json::Value, AccountError> {
            Ok(json!({ "following": [], "followers": [], "blocks": [] }))
        }
        async fn conversations(&self, _: &str) -> Result<Vec<ConversationExport>, AccountError> {
            Ok(["dm", "group"]
                .map(|id| ConversationExport { conversation_id: id.into(), membership: json!({ "role": "MEMBER" }) })
                .to_vec())
        }
        async fn members(&self, conversation: &str, _: &str) -> Result<Vec<String>, AccountError> {
            Ok(match conversation {
                "dm" => vec!["me".into(), "friend".into()],
                _ => vec!["me".into(), "a".into(), "b".into()],
            })
        }
        async fn messages(&self, _: &str, _: &str) -> Result<Vec<MessageExport>, AccountError> {
            Ok(vec![message("me", 1, "hi"), message("friend", 2, "their secret")])
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
        assert_eq!(file(&files, "profiles/me/reactions.json")[0]["kind"], "HEART");
        assert!(file(&files, "profiles/me/social.json")["blocks"].is_array());
        assert_eq!(file(&files, "media.json")[0]["ttl_days"], 7, "links valid 7 days");

        let dm = file(&files, "profiles/me/conversations/dm.json");
        assert_eq!(dm["one_to_one"], true);
        assert_eq!(dm["messages"][1]["body"], "their secret", "a one-to-one in full");

        let group = file(&files, "profiles/me/conversations/group.json");
        assert_eq!(group["messages"][0]["body"], "hi", "the holder's own");
        assert_eq!(group["messages"][1], json!({ "from": "another member", "created_at_ms": 2 }));
        assert!(!group.to_string().contains("their secret"), "another member's words never leave");
    }
}
