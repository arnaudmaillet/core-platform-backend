use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::ProfileRepository;
use crate::domain::value_object::Handle;
use crate::error::ProfileError;

/// Whether a handle can be taken right now (sign-up's live check).
#[derive(Debug, Clone)]
pub struct CheckHandleAvailabilityQuery {
    pub handle: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleAvailability {
    /// Normalized handle.
    Available(String),
    /// Normalized handle.
    Taken(String),
    /// Why it fails the handle rules.
    Invalid(String),
}

impl Query for CheckHandleAvailabilityQuery {
    type Response = HandleAvailability;
}

pub struct CheckHandleAvailabilityHandler {
    repo: Arc<dyn ProfileRepository>,
}

impl CheckHandleAvailabilityHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<CheckHandleAvailabilityQuery> for CheckHandleAvailabilityHandler {
    type Error = ProfileError;

    /// The same rules and the same availability read as `CreateProfile`'s
    /// pre-check, so an AVAILABLE answer is what CreateProfile would accept
    /// (barring a race, which its claim settles).
    async fn handle(&self, envelope: Envelope<CheckHandleAvailabilityQuery>) -> Result<HandleAvailability, ProfileError> {
        let handle = match Handle::new(&envelope.payload.handle) {
            Ok(handle) => handle,
            Err(e) => return Ok(HandleAvailability::Invalid(e.to_string())),
        };
        let normalized = handle.as_str().to_owned();
        Ok(if self.repo.handle_is_available(&handle).await? {
            HandleAvailability::Available(normalized)
        } else {
            HandleAvailability::Taken(normalized)
        })
    }
}
