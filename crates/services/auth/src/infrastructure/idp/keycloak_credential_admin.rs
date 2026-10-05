use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::application::port::CredentialAdmin;
use crate::domain::value_object::IdpSubject;
use crate::error::AuthError;

/// The Keycloak Admin API client auth uses to change a password: a confidential
/// client with a service account holding `realm-management` `view-users` +
/// `manage-users` (`client_credentials` grant).
#[derive(Debug, Clone, Default)]
pub struct KeycloakAdminConfig {
    /// The realm's admin base, e.g. `https://idp/admin/realms/core-platform`.
    /// Empty ⇒ credential management is not configured.
    pub admin_url: String,
    /// The realm's token endpoint (the one `Login` uses).
    pub token_endpoint: String,
    pub client_id: String,
    pub client_secret: String,
}

impl KeycloakAdminConfig {
    pub fn is_configured(&self) -> bool {
        !self.admin_url.is_empty() && !self.client_id.is_empty() && !self.client_secret.is_empty()
    }
}

/// Keycloak implementation of [`CredentialAdmin`]. The service-account token is
/// cached until shortly before it expires.
pub struct KeycloakCredentialAdmin {
    http: reqwest::Client,
    config: KeycloakAdminConfig,
    token: Mutex<Option<(String, Instant)>>,
}

#[derive(Deserialize)]
struct ServiceToken {
    access_token: String,
    #[serde(default = "default_expires_in")]
    expires_in: u64,
}

fn default_expires_in() -> u64 {
    60
}

#[derive(Deserialize)]
struct UserRepresentation {
    username: String,
    #[serde(default)]
    email: Option<String>,
}

/// Keycloak's error body (`{"error": "...", "error_description": "..."}`).
#[derive(Deserialize, Default)]
struct KeycloakError {
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_description: String,
}

impl KeycloakCredentialAdmin {
    pub fn new(http: reqwest::Client, config: KeycloakAdminConfig) -> Self {
        Self { http, config, token: Mutex::new(None) }
    }

    /// The service-account bearer, from cache or a fresh `client_credentials`
    /// grant. A refused grant means the admin client is misconfigured.
    async fn service_token(&self) -> Result<String, AuthError> {
        if let Some((token, until)) = self.token.lock().unwrap().as_ref()
            && Instant::now() < *until
        {
            return Ok(token.clone());
        }
        let response = self
            .http
            .post(&self.config.token_endpoint)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", self.config.client_id.as_str()),
                ("client_secret", self.config.client_secret.as_str()),
            ])
            .send()
            .await
            .map_err(|_| AuthError::IdpUnavailable)?;
        if !response.status().is_success() {
            tracing::error!(status = %response.status(), "keycloak admin client_credentials grant refused");
            return Err(if response.status().is_server_error() {
                AuthError::IdpUnavailable
            } else {
                AuthError::CredentialManagementUnavailable
            });
        }
        let token: ServiceToken =
            response.json().await.map_err(|_| AuthError::CredentialManagementUnavailable)?;
        // Renew 30 s early so a call never presents an expiring token.
        let until = Instant::now() + Duration::from_secs(token.expires_in.saturating_sub(30));
        *self.token.lock().unwrap() = Some((token.access_token.clone(), until));
        Ok(token.access_token)
    }

    fn user_url(&self, subject: &IdpSubject) -> String {
        format!("{}/users/{}", self.config.admin_url.trim_end_matches('/'), subject.subject())
    }

    /// Maps an admin-call failure: 401/403 ⇒ the admin client lost its rights
    /// (drop the cached token); 5xx ⇒ IdP trouble.
    fn admin_failure(&self, status: StatusCode) -> AuthError {
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            *self.token.lock().unwrap() = None;
            tracing::error!(%status, "keycloak admin call refused; check the admin client's roles");
            return AuthError::CredentialManagementUnavailable;
        }
        if status.is_server_error() {
            return AuthError::IdpUnavailable;
        }
        AuthError::CredentialManagementUnavailable
    }
}

#[async_trait]
impl CredentialAdmin for KeycloakCredentialAdmin {
    async fn login_name(&self, subject: &IdpSubject) -> Result<String, AuthError> {
        let token = self.service_token().await?;
        let response = self
            .http
            .get(self.user_url(subject))
            .bearer_auth(token)
            .send()
            .await
            .map_err(|_| AuthError::IdpUnavailable)?;
        match response.status() {
            s if s.is_success() => response
                .json::<UserRepresentation>()
                .await
                .map(|u| u.username)
                .map_err(|e| AuthError::ClaimsNormalizationFailed(format!("admin user: {e}"))),
            // The subject is gone from the IdP: nothing to prove a password for.
            StatusCode::NOT_FOUND => Err(AuthError::IdpAuthenticationFailed),
            s => Err(self.admin_failure(s)),
        }
    }

    async fn set_password(&self, subject: &IdpSubject, new_password: &str) -> Result<(), AuthError> {
        let token = self.service_token().await?;
        let response = self
            .http
            .put(format!("{}/reset-password", self.user_url(subject)))
            .bearer_auth(token)
            .json(&serde_json::json!({
                "type": "password",
                "value": new_password,
                "temporary": false,
            }))
            .send()
            .await
            .map_err(|_| AuthError::IdpUnavailable)?;
        match response.status() {
            s if s.is_success() => Ok(()),
            // The realm's password policy refused it; its description names the
            // rule ("Invalid password: minimum length 12.").
            StatusCode::BAD_REQUEST => {
                let body: KeycloakError = response.json().await.unwrap_or_default();
                let reason = if body.error_description.is_empty() { body.error } else { body.error_description };
                Err(AuthError::PasswordRejected { reason })
            }
            StatusCode::NOT_FOUND => Err(AuthError::IdpAuthenticationFailed),
            s => Err(self.admin_failure(s)),
        }
    }

    async fn delete_user(&self, subject: &IdpSubject) -> Result<(), AuthError> {
        let token = self.service_token().await?;
        let response = self
            .http
            .delete(self.user_url(subject))
            .bearer_auth(token)
            .send()
            .await
            .map_err(|_| AuthError::IdpUnavailable)?;
        match response.status() {
            // Deleted now, or already gone (a replayed erasure).
            s if s.is_success() || s == StatusCode::NOT_FOUND => Ok(()),
            s => Err(self.admin_failure(s)),
        }
    }

    async fn set_email(&self, subject: &IdpSubject, email: &str) -> Result<(), AuthError> {
        let token = self.service_token().await?;
        let response = self
            .http
            .get(self.user_url(subject))
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|_| AuthError::IdpUnavailable)?;
        let user = match response.status() {
            s if s.is_success() => response
                .json::<UserRepresentation>()
                .await
                .map_err(|e| AuthError::ClaimsNormalizationFailed(format!("admin user: {e}")))?,
            // Gone from the IdP: no password sign-in to keep in step.
            StatusCode::NOT_FOUND => return Ok(()),
            s => return Err(self.admin_failure(s)),
        };
        // The login name follows when it was the email (a realm that signs in
        // by email address).
        let mut update = serde_json::json!({ "email": email, "emailVerified": true });
        if user.email.as_deref().is_some_and(|old| old.eq_ignore_ascii_case(&user.username)) {
            update["username"] = serde_json::json!(email);
        }
        let response = self
            .http
            .put(self.user_url(subject))
            .bearer_auth(token)
            .json(&update)
            .send()
            .await
            .map_err(|_| AuthError::IdpUnavailable)?;
        match response.status() {
            s if s.is_success() => Ok(()),
            // Another IdP user has that email (or login name).
            StatusCode::CONFLICT => Err(AuthError::EmailAlreadyRegistered),
            StatusCode::NOT_FOUND => Ok(()),
            s => Err(self.admin_failure(s)),
        }
    }
}

/// [`CredentialAdmin`] when no admin client is configured: changing a password
/// is unavailable, everything else in auth works.
pub struct UnconfiguredCredentialAdmin;

#[async_trait]
impl CredentialAdmin for UnconfiguredCredentialAdmin {
    async fn login_name(&self, _subject: &IdpSubject) -> Result<String, AuthError> {
        Err(AuthError::CredentialManagementUnavailable)
    }

    async fn set_password(&self, _subject: &IdpSubject, _new_password: &str) -> Result<(), AuthError> {
        Err(AuthError::CredentialManagementUnavailable)
    }

    async fn delete_user(&self, _subject: &IdpSubject) -> Result<(), AuthError> {
        Err(AuthError::CredentialManagementUnavailable)
    }

    async fn set_email(&self, _subject: &IdpSubject, _email: &str) -> Result<(), AuthError> {
        Err(AuthError::CredentialManagementUnavailable)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::extract::{Path, State};
    use axum::http::StatusCode as AxumStatus;
    use axum::routing::{get, post, put};
    use axum::{Json, Router};

    use super::*;

    /// A fake Keycloak: one token endpoint, one user `u-1` named `alice`, and a
    /// reset-password that refuses anything shorter than 12 characters.
    #[derive(Clone, Default)]
    struct Fake {
        grants: Arc<AtomicUsize>,
        last_password: Arc<Mutex<Option<String>>>,
        deleted: Arc<Mutex<Vec<String>>>,
        last_update: Arc<Mutex<Option<serde_json::Value>>>,
    }

    async fn delete(State(fake): State<Fake>, Path(id): Path<String>) -> AxumStatus {
        let mut deleted = fake.deleted.lock().unwrap();
        if id == "u-1" && !deleted.contains(&id) {
            deleted.push(id);
            AxumStatus::NO_CONTENT
        } else {
            AxumStatus::NOT_FOUND
        }
    }

    async fn token(State(fake): State<Fake>) -> Json<serde_json::Value> {
        fake.grants.fetch_add(1, Ordering::SeqCst);
        Json(serde_json::json!({ "access_token": "svc", "expires_in": 300 }))
    }

    async fn user(Path(id): Path<String>) -> Result<Json<serde_json::Value>, AxumStatus> {
        match id.as_str() {
            "u-1" => Ok(Json(serde_json::json!({ "id": "u-1", "username": "alice", "email": "alice@old.example" }))),
            // A realm that signs in by email: the login name is the address.
            "u-2" => Ok(Json(serde_json::json!({ "id": "u-2", "username": "bob@old.example", "email": "bob@old.example" }))),
            _ => Err(AxumStatus::NOT_FOUND),
        }
    }

    async fn update_user(
        State(fake): State<Fake>,
        Path(_id): Path<String>,
        Json(body): Json<serde_json::Value>,
    ) -> AxumStatus {
        if body["email"] == "taken@example.com" {
            return AxumStatus::CONFLICT;
        }
        *fake.last_update.lock().unwrap() = Some(body);
        AxumStatus::NO_CONTENT
    }

    async fn reset(
        State(fake): State<Fake>,
        Json(body): Json<serde_json::Value>,
    ) -> (AxumStatus, Json<serde_json::Value>) {
        let value = body["value"].as_str().unwrap_or_default().to_owned();
        if value.chars().count() < 12 {
            return (
                AxumStatus::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "invalidPasswordMinLengthMessage",
                    "error_description": "Invalid password: minimum length 12."
                })),
            );
        }
        assert_eq!(body["temporary"], false);
        *fake.last_password.lock().unwrap() = Some(value);
        (AxumStatus::NO_CONTENT, Json(serde_json::json!({})))
    }

    async fn serve(fake: Fake) -> String {
        let app = Router::new()
            .route("/realms/r/protocol/openid-connect/token", post(token))
            .route("/admin/realms/r/users/{id}", get(user).put(update_user).delete(delete))
            .route("/admin/realms/r/users/{id}/reset-password", put(reset))
            .with_state(fake);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn admin(base: &str) -> KeycloakCredentialAdmin {
        KeycloakCredentialAdmin::new(
            reqwest::Client::new(),
            KeycloakAdminConfig {
                admin_url: format!("{base}/admin/realms/r"),
                token_endpoint: format!("{base}/realms/r/protocol/openid-connect/token"),
                client_id: "core-platform-auth-admin".into(),
                client_secret: "s".into(),
            },
        )
    }

    fn subject(id: &str) -> IdpSubject {
        IdpSubject::new("https://idp/realms/r", id).unwrap()
    }

    #[tokio::test]
    async fn deleting_a_user_is_idempotent() {
        let fake = Fake::default();
        let admin = admin(&serve(fake.clone()).await);
        admin.delete_user(&subject("u-1")).await.unwrap();
        // Already gone (a replayed erasure), and never existed: both done.
        admin.delete_user(&subject("u-1")).await.unwrap();
        admin.delete_user(&subject("nobody")).await.unwrap();
        assert_eq!(*fake.deleted.lock().unwrap(), vec!["u-1".to_owned()]);
    }

    #[tokio::test]
    async fn reads_the_login_name_and_sets_the_password_under_one_cached_token() {
        let fake = Fake::default();
        let base = serve(fake.clone()).await;
        let admin = admin(&base);

        assert_eq!(admin.login_name(&subject("u-1")).await.unwrap(), "alice");
        admin.set_password(&subject("u-1"), "correct horse battery").await.unwrap();

        assert_eq!(fake.last_password.lock().unwrap().as_deref(), Some("correct horse battery"));
        assert_eq!(fake.grants.load(Ordering::SeqCst), 1, "the service token is cached");
    }

    #[tokio::test]
    async fn the_email_follows_and_the_login_name_too_when_it_was_the_email() {
        let fake = Fake::default();
        let base = serve(fake.clone()).await;
        let admin = admin(&base);

        admin.set_email(&subject("u-1"), "alice@new.example").await.unwrap();
        let update = fake.last_update.lock().unwrap().clone().unwrap();
        assert_eq!(update["email"], "alice@new.example");
        assert_eq!(update["emailVerified"], true);
        assert!(update.get("username").is_none(), "a chosen login name stays");

        admin.set_email(&subject("u-2"), "bob@new.example").await.unwrap();
        let update = fake.last_update.lock().unwrap().clone().unwrap();
        assert_eq!(update["username"], "bob@new.example", "signs in by the new address");

        assert!(matches!(
            admin.set_email(&subject("u-1"), "taken@example.com").await.unwrap_err(),
            AuthError::EmailAlreadyRegistered
        ));
        admin.set_email(&subject("ghost"), "x@example.com").await.expect("gone: nothing to keep in step");
    }

    #[tokio::test]
    async fn the_realm_policy_refusal_names_its_rule() {
        let base = serve(Fake::default()).await;
        let err = admin(&base).set_password(&subject("u-1"), "short").await.unwrap_err();
        match err {
            AuthError::PasswordRejected { reason } => {
                assert_eq!(reason, "Invalid password: minimum length 12.")
            }
            other => panic!("expected PasswordRejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unknown_subject_cannot_prove_a_password() {
        let base = serve(Fake::default()).await;
        assert!(matches!(
            admin(&base).login_name(&subject("ghost")).await.unwrap_err(),
            AuthError::IdpAuthenticationFailed
        ));
    }

    #[tokio::test]
    async fn an_unreachable_idp_is_unavailable_and_an_unconfigured_one_too() {
        let admin = admin("http://127.0.0.1:9");
        assert!(matches!(admin.login_name(&subject("u-1")).await.unwrap_err(), AuthError::IdpUnavailable));
        assert!(matches!(
            UnconfiguredCredentialAdmin.set_password(&subject("u-1"), "whatever123").await.unwrap_err(),
            AuthError::CredentialManagementUnavailable
        ));
    }

    /// End to end against a real Keycloak with local-dev's realm imported
    /// (`local-dev/keycloak/realm-core-platform.json`). Every credential comes
    /// from the environment — the realm file has the local-dev values:
    ///
    /// ```text
    /// docker run --rm -d --name kc-it -p 18085:8080 \
    ///   -e KC_BOOTSTRAP_ADMIN_USERNAME=admin -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
    ///   -v "$PWD/local-dev/keycloak:/opt/keycloak/data/import:ro" \
    ///   quay.io/keycloak/keycloak:26.0 start-dev --import-realm
    /// KEYCLOAK_IT_BASE=http://localhost:18085 KEYCLOAK_IT_ADMIN_SECRET=… \
    ///   KEYCLOAK_IT_USER=… KEYCLOAK_IT_PASSWORD=… \
    ///   cargo test -p auth --lib real_keycloak -- --ignored
    /// ```
    #[tokio::test]
    #[ignore = "needs a running Keycloak (KEYCLOAK_IT_*)"]
    async fn real_keycloak_changes_the_password_a_user_signs_in_with() {
        use crate::application::port::{AuthnGrant, IdentityProvider};
        use crate::infrastructure::idp::{KeycloakConfig, KeycloakIdentityProvider};

        let env = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
        let base = env("KEYCLOAK_IT_BASE");
        let user = env("KEYCLOAK_IT_USER");
        let original = env("KEYCLOAK_IT_PASSWORD");
        let token_endpoint = format!("{base}/realms/core-platform/protocol/openid-connect/token");
        let idp = KeycloakIdentityProvider::new(
            reqwest::Client::new(),
            KeycloakConfig {
                token_endpoint: token_endpoint.clone(),
                client_id: "core-platform-auth".into(),
                ..KeycloakConfig::default()
            },
        );
        let admin = KeycloakCredentialAdmin::new(
            reqwest::Client::new(),
            KeycloakAdminConfig {
                admin_url: format!("{base}/admin/realms/core-platform"),
                token_endpoint,
                client_id: "core-platform-auth-admin".into(),
                client_secret: env("KEYCLOAK_IT_ADMIN_SECRET"),
            },
        );
        let login = |secret: String| {
            idp.authenticate(AuthnGrant::Password { username: user.clone(), password: secret })
        };
        let changed = format!("{original}-changed-by-it");

        let claims = login(original.clone()).await.expect("the user signs in");
        let subject = IdpSubject::new(claims.issuer, claims.subject).unwrap();
        assert_eq!(admin.login_name(&subject).await.unwrap(), user);

        admin.set_password(&subject, &changed).await.expect("set");
        assert!(matches!(login(original.clone()).await.unwrap_err(), AuthError::IdpAuthenticationFailed));
        login(changed).await.expect("the new password works");

        // Leave the realm as found.
        admin.set_password(&subject, &original).await.expect("restore");
    }
}
