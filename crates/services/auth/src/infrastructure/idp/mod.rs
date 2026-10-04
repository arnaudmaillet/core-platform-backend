//! Identity-provider adapter. Keycloak today; the OIDC/OAuth2 specifics live
//! entirely here, behind the [`IdentityProvider`](crate::application::port::IdentityProvider)
//! port. Swapping to Cognito/Okta/custom is a sibling module — no change above
//! `infrastructure`.

pub mod keycloak_credential_admin;
pub mod keycloak_identity_provider;

pub use keycloak_credential_admin::{
    KeycloakAdminConfig, KeycloakCredentialAdmin, UnconfiguredCredentialAdmin,
};
pub use keycloak_identity_provider::{KeycloakConfig, KeycloakIdentityProvider};
