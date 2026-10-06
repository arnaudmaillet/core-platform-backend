//! Outbound ports — the only contracts the application layer holds against the
//! outside world. Concrete adapters (Keycloak, Postgres, Redis, the `account`
//! gRPC client, the token minter) live in `infrastructure` (Phase 4) and are
//! injected at the composition root. Each is an `async_trait` so it can be held
//! as `Arc<dyn …>`.

pub mod account_directory;
pub mod account_eraser;
pub mod credential_admin;
pub mod event_publisher;
pub mod federated_nonce_store;
pub mod federated_token_verifier;
pub mod guest_registry;
pub mod identity_provider;
pub mod mfa;
pub mod passkey_repository;
pub mod profile_directory;
pub mod refresh_token_repository;
pub mod session_cache;
pub mod session_repository;
pub mod subject_link_repository;
pub mod token_minter;
pub mod verification;

pub use account_directory::{
    AccountActivation, AccountDirectory, AccountSnapshot, ContactDetails, EmailHolder, NewAccount, SignUpConsent,
};
pub use account_eraser::{AccountEraser, ErasedAccount};
pub use credential_admin::CredentialAdmin;
pub use event_publisher::EventPublisher;
pub use federated_nonce_store::FederatedNonceStore;
pub use federated_token_verifier::{FederatedIdentity, FederatedTokenVerifier};
pub use guest_registry::{GuestRecord, GuestRegistry};
pub use identity_provider::{AuthnGrant, IdentityProvider, NormalizedClaims};
pub use mfa::{MfaChange, MfaSecret, MfaSeedCipher, MfaStore, PendingLogin};
pub use passkey_repository::{PasskeyRepository, StoredPasskey};
pub use profile_directory::{profile_ids_or_empty, ProfileDirectory};
pub use refresh_token_repository::RefreshTokenRepository;
pub use session_cache::SessionCache;
pub use session_repository::{DeviceHistory, RECENT_SESSIONS, SessionRepository};
pub use subject_link_repository::SubjectLinkRepository;
pub use token_minter::{GeneratedRefresh, TokenMinter};
pub use verification::{
    CodeSender, ConsumeOutcome, PendingChallenge, SendAdmission, SendLimits, SmsBudget, SmsReservation,
    VerificationChannel,
    VerificationStore, VerifiedDestination,
};
