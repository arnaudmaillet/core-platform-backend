use std::marker::PhantomData;

use jsonwebtoken::{decode, decode_header, Algorithm, Validation};
use serde::de::DeserializeOwned;

use crate::{AuthContextConfig, AuthError, ClaimsExtractor, CurrentPrincipal, JwksCache};

/// Stateless JWT verifier that delegates cryptographic key lookup to [`JwksCache`]
/// and claim transformation to a [`ClaimsExtractor`].
///
/// ## Verification pipeline
///
/// ```text
/// raw token string
///   │
///   ▼
/// decode_header()          ← extract kid (no signature check yet)
///   │
///   ▼
/// JwksCache::get(kid)      ← O(1) in-memory read under a shared RwLock
///   │
///   ▼
/// jsonwebtoken::decode()   ← verify RS256/ES256 signature + exp/nbf/iss/aud
///   │
///   ▼
/// ClaimsExtractor::extract ← map raw claims → CurrentPrincipal<C>
/// ```
///
/// ## Thread safety
///
/// `JwtDecoder` is `Clone + Send + Sync`. Wrap in `Arc` and share across all
/// request handlers in the process.
///
/// ## Generic parameters
///
/// - `C` — the concrete claim struct (e.g. [`crate::OidcClaims`]). Must be
///   `DeserializeOwned + Send + Sync + 'static`.
/// - `E` — the [`ClaimsExtractor`] implementation.
pub struct JwtDecoder<C, E>
where
    C: DeserializeOwned + Send + Sync + 'static,
    E: ClaimsExtractor<C>,
{
    cache: JwksCache,
    extractor: E,
    validation: Validation,
    _marker: PhantomData<fn() -> C>,
}

impl<C, E> JwtDecoder<C, E>
where
    C: DeserializeOwned + Send + Sync + 'static,
    E: ClaimsExtractor<C>,
{
    /// Constructs a decoder from an [`AuthContextConfig`], a pre-seeded (or
    /// empty) [`JwksCache`], and a [`ClaimsExtractor`].
    ///
    /// Pass the *same* `JwksCache` instance to both [`JwksRefresher::spawn`]
    /// and this constructor so key rotations are picked up transparently.
    ///
    /// [`JwksRefresher::spawn`]: crate::JwksRefresher::spawn
    pub fn new(config: &AuthContextConfig, cache: JwksCache, extractor: E) -> Self {
        let mut validation = Validation::new(Algorithm::RS256);

        match &config.expected_audience {
            Some(aud) => validation.set_audience(&[aud.as_str()]),
            None => validation.validate_aud = false,
        }

        if let Some(ref iss) = config.expected_issuer {
            validation.set_issuer(&[iss.as_str()]);
        }

        validation.leeway = config.clock_skew.as_secs();

        Self {
            cache,
            extractor,
            validation,
            _marker: PhantomData,
        }
    }

    /// Constructs a decoder that also accepts ES256 tokens.
    ///
    /// Use this when the JWKS may contain both RSA and EC keys.
    pub fn with_algorithms(
        config: &AuthContextConfig,
        cache: JwksCache,
        extractor: E,
        algorithms: Vec<Algorithm>,
    ) -> Self {
        let mut decoder = Self::new(config, cache, extractor);
        decoder.validation = {
            let mut v = Validation::new(algorithms[0]);
            v.algorithms = algorithms;
            if let Some(ref aud) = config.expected_audience {
                v.set_audience(&[aud.as_str()]);
            } else {
                v.validate_aud = false;
            }
            if let Some(ref iss) = config.expected_issuer {
                v.set_issuer(&[iss.as_str()]);
            }
            v.leeway = config.clock_skew.as_secs();
            v
        };
        decoder
    }

    /// Verifies `token` and returns the extracted [`CurrentPrincipal`].
    ///
    /// This method is `async` because the cache lookup acquires a
    /// `tokio::sync::RwLock`. The actual cryptographic work is synchronous
    /// (CPU-bound on the calling thread), so avoid spawning this inside a
    /// `spawn_blocking` — the work is short and not I/O-bound.
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---------|-----------|
    /// | [`AuthError::MissingKid`]           | No `kid` in JWT header        |
    /// | [`AuthError::UnknownKid`]           | `kid` not found in cache      |
    /// | [`AuthError::InvalidSignature`]     | Signature mismatch            |
    /// | [`AuthError::TokenExpired`]         | `exp` exceeded (+ leeway)     |
    /// | [`AuthError::TokenNotYetValid`]     | `nbf` in the future           |
    /// | [`AuthError::InvalidAudience`]      | `aud` mismatch                |
    /// | [`AuthError::InvalidIssuer`]        | `iss` mismatch                |
    /// | [`AuthError::MalformedToken`]       | Structural / algorithm error  |
    /// | [`AuthError::ClaimsExtractionFailed`] | Extractor rejected claims   |
    pub async fn decode(&self, token: &str) -> Result<CurrentPrincipal<C>, AuthError> {
        let header =
            decode_header(token).map_err(|e| AuthError::MalformedToken(e.to_string()))?;

        let kid = header.kid.ok_or(AuthError::MissingKid)?;

        let decoding_key = self
            .cache
            .get(&kid)
            .await
            .ok_or_else(|| AuthError::UnknownKid(kid.clone()))?;

        let token_data = decode::<C>(token, &decoding_key, &self.validation)
            .map_err(map_jwt_error)?;

        self.extractor.extract(token_data.claims)
    }
}

// ── Error mapping ─────────────────────────────────────────────────────────────

pub(crate) fn map_jwt_error(e: jsonwebtoken::errors::Error) -> AuthError {
    use jsonwebtoken::errors::ErrorKind;
    match e.kind() {
        ErrorKind::ExpiredSignature => AuthError::TokenExpired,
        ErrorKind::ImmatureSignature => AuthError::TokenNotYetValid,
        ErrorKind::InvalidSignature => AuthError::InvalidSignature,
        ErrorKind::InvalidAudience => AuthError::InvalidAudience,
        ErrorKind::InvalidIssuer => AuthError::InvalidIssuer,
        _ => AuthError::MalformedToken(e.to_string()),
    }
}
