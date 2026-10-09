pub mod config;
pub mod context;
pub mod decoder;
pub mod edge;
pub mod error;
pub mod extractor;
pub mod jwks;
pub mod mesh;
pub mod principal;

pub use config::AuthContextConfig;
pub use context::{AnyPrincipal, current_principal, inject_into_span, with_principal};
pub use decoder::JwtDecoder;
pub use edge::{
    edge_decoder, spawn_edge_decoder, EdgeDecoder, EDGE_ALGORITHMS, EDGE_PROFILES_CLAIM,
    EDGE_SESSION_CLAIM,
};
pub use error::AuthError;
pub use extractor::{
    ClaimsExtractor, OidcClaims, OidcClaimsExtractor, OidcExtractorConfig, RealmAccess,
    RoleSource, EDGE_PERMISSIONS_CLAIM,
};
pub use jwks::{JwksCache, JwksClient, JwksRefresher};
pub use principal::{CurrentPrincipal, Permission, PrincipalId};

#[cfg(feature = "cqrs-integration")]
pub use context::inject_into_envelope;
