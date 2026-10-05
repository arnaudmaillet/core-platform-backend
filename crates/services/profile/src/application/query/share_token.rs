//! A profile's QR code / share link (#661): a random token, revocable,
//! independent of the handle. The owner gets it (issued on first ask) and
//! rotates it — the old one stops resolving at once. Anyone scanning it
//! resolves it to the profile, as they may see it, while the owner keeps
//! "reachable by QR code / shared link" (`by_qr`) on.

use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope, Query, QueryHandler};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{
    is_share_token, new_share_token, ProfileCache, ProfileRepository, ProfileView, ShareTokenStore,
};
use crate::domain::value_object::{ProfileId, ProfileStatus, Viewer};
use crate::error::ProfileError;

/// The owner's current token (issued on first ask).
#[derive(Debug, Clone)]
pub struct GetShareTokenQuery {
    pub profile_id: String,
}

impl Query for GetShareTokenQuery {
    type Response = String;
}

/// The owner revokes their token for a new one.
#[derive(Debug, Clone)]
pub struct RotateShareTokenCommand {
    pub profile_id: String,
}

impl Command for RotateShareTokenCommand {}

impl Validate for RotateShareTokenCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "VAL-3061", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

/// A scanned token, read as `viewer`.
#[derive(Debug, Clone)]
pub struct ResolveShareTokenQuery {
    pub token:  String,
    pub viewer: Viewer,
}

impl Query for ResolveShareTokenQuery {
    type Response = Option<ProfileView>;
}

/// Issues, rotates and resolves share tokens.
pub struct ShareTokenHandler {
    repo:   Arc<dyn ProfileRepository>,
    cache:  Arc<dyn ProfileCache>,
    tokens: Arc<dyn ShareTokenStore>,
}

impl ShareTokenHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>, tokens: Arc<dyn ShareTokenStore>) -> Self {
        Self { repo, cache, tokens }
    }

    async fn existing(&self, id: &ProfileId) -> Result<(), ProfileError> {
        self.repo
            .find_by_id(id)
            .await?
            .map(|_| ())
            .ok_or_else(|| ProfileError::ProfileNotFound { id: id.as_str() })
    }

    /// The whole view (owner-only fields included), from the cache or store.
    async fn full_view(&self, id: &ProfileId) -> Result<Option<ProfileView>, ProfileError> {
        if let Some(view) = self.cache.get_by_id(id).await? {
            return Ok(Some(view));
        }
        let Some(profile) = self.repo.find_by_id(id).await? else { return Ok(None) };
        let view = ProfileView::from(&profile);
        let _ = self.cache.set_by_id(&view).await;
        Ok(Some(view))
    }
}

impl QueryHandler<GetShareTokenQuery> for ShareTokenHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<GetShareTokenQuery>) -> Result<String, ProfileError> {
        let id = ProfileId::try_from(envelope.payload.profile_id.as_str())?;
        self.existing(&id).await?;
        match self.tokens.current(&id).await? {
            Some(token) => Ok(token),
            None => self.tokens.issue(&id, &new_share_token()).await,
        }
    }
}

impl CommandHandler<RotateShareTokenCommand> for ShareTokenHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<RotateShareTokenCommand>) -> Result<(), ProfileError> {
        let id = ProfileId::try_from(envelope.payload.profile_id.as_str())?;
        self.existing(&id).await?;
        // Twice at most: a concurrent rotation moved the token on; one more
        // try from where it now stands.
        for _ in 0..2 {
            let Some(current) = self.tokens.current(&id).await? else {
                self.tokens.issue(&id, &new_share_token()).await?;
                return Ok(());
            };
            if self.tokens.replace(&id, &current, &new_share_token()).await? {
                return Ok(());
            }
        }
        Ok(())
    }
}

impl QueryHandler<ResolveShareTokenQuery> for ShareTokenHandler {
    type Error = ProfileError;

    /// `None` — answered like a missing profile — for a malformed or revoked
    /// token, a profile that is not active, or one whose owner turned
    /// `by_qr` off (the owner still resolves their own).
    async fn handle(&self, envelope: Envelope<ResolveShareTokenQuery>) -> Result<Option<ProfileView>, ProfileError> {
        let q = &envelope.payload;
        if !is_share_token(&q.token) {
            return Ok(None);
        }
        let Some(id) = self.tokens.resolve(&q.token).await? else { return Ok(None) };
        let Some(view) = self.full_view(&id).await? else { return Ok(None) };
        let owner = q.viewer.sees_everything_of(&view.account_id) && q.viewer != Viewer::Internal;
        let reachable = view.discovery.is_none_or(|d| d.by_qr);
        if !owner && (view.status != ProfileStatus::Active.as_str() || !reachable) {
            return Ok(None);
        }
        Ok(view.for_viewer(&q.viewer))
    }
}
