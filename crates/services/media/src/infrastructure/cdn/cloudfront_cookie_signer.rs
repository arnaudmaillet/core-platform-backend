//! CloudFront signed cookies (#921): how a SIGNED video plays. A player
//! resolves the master playlist's relative URIs (rung playlists, segments)
//! against the master's URL and drops its query string, so a per-object
//! presigned URL cannot cover an HLS tree. Signed cookies can: one custom
//! policy grants every object under the asset's prefix until it expires, and
//! the player sends the cookies with each request to the CDN host.
//!
//! CloudFront's scheme: the policy JSON (no whitespace), its RSA-SHA1
//! PKCS#1 v1.5 signature with the key pair registered in the distribution's
//! trusted key group, both base64-encoded with CloudFront's URL-safe
//! substitutions (`+`→`-`, `=`→`_`, `/`→`~`), plus the key pair id.

use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer};
use rsa::RsaPrivateKey;
use sha1::Sha1;

use crate::application::port::SignedCookies;
use crate::error::MediaError;

pub struct CloudFrontCookieSigner {
    key_pair_id: String,
    key:         SigningKey<Sha1>,
    /// The CDN base URL, e.g. `https://cdn.example.com` (no trailing `/`).
    base_url:    String,
    domain:      String,
    /// The base URL's own path (`/media`, or empty), under which every tree
    /// sits on the CDN host.
    base_path:   String,
    ttl:         Duration,
}

impl CloudFrontCookieSigner {
    /// `private_key_pem`: the key pair's private key, PKCS#1 (`BEGIN RSA
    /// PRIVATE KEY`, what `openssl genrsa` writes) or PKCS#8.
    pub fn new(key_pair_id: String, private_key_pem: &str, base_url: &str, ttl: Duration) -> Result<Self, MediaError> {
        let key = RsaPrivateKey::from_pkcs1_pem(private_key_pem)
            .or_else(|_| RsaPrivateKey::from_pkcs8_pem(private_key_pem))
            .map_err(|e| MediaError::DeliverySigningFailed { reason: format!("CloudFront cookie key: {e}") })?;
        let base_url = base_url.trim_end_matches('/').to_owned();
        let parsed = url::Url::parse(&base_url)
            .ok()
            .filter(|u| u.host_str().is_some())
            .ok_or_else(|| MediaError::DeliverySigningFailed { reason: format!("CDN base URL {base_url:?} has no host") })?;
        let domain = parsed.host_str().unwrap_or_default().to_owned();
        let base_path = parsed.path().trim_end_matches('/').to_owned();
        Ok(Self { key_pair_id, key: SigningKey::<Sha1>::new(key), base_url, domain, base_path, ttl })
    }

    /// Cookies granting every CDN object under `prefix` (an asset's tree,
    /// trailing `/`) for the signer's TTL from `now`. Their path is the tree's
    /// own, so one video's cookies never overwrite another's in a shared
    /// cookie jar (all are named `CloudFront-*`), and each is only sent for
    /// its own tree.
    pub fn sign(&self, prefix: &str, now: DateTime<Utc>) -> SignedCookies {
        let expires_at = now + self.ttl;
        let resource = format!("{}/{}*", self.base_url, prefix.trim_start_matches('/'));
        let policy = policy(&resource, expires_at.timestamp());
        let signature = self.key.sign(policy.as_bytes()).to_vec();
        SignedCookies {
            cookies: vec![
                ("CloudFront-Policy".to_owned(), encode(policy.as_bytes())),
                ("CloudFront-Signature".to_owned(), encode(&signature)),
                ("CloudFront-Key-Pair-Id".to_owned(), self.key_pair_id.clone()),
            ],
            domain: self.domain.clone(),
            path: format!("{}/{}", self.base_path, prefix.trim_start_matches('/')),
            expires_at,
        }
    }
}

/// A custom policy: `resource` (a URL, `*` wildcards allowed) until `epoch`.
fn policy(resource: &str, epoch: i64) -> String {
    format!(r#"{{"Statement":[{{"Resource":"{resource}","Condition":{{"DateLessThan":{{"AWS:EpochTime":{epoch}}}}}}}]}}"#)
}

/// Base64 with CloudFront's substitutions for cookie / query values.
fn encode(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD
        .encode(bytes)
        .chars()
        .map(|c| match c {
            '+' => '-',
            '=' => '_',
            '/' => '~',
            c => c,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::signature::Verifier;

    use super::*;

    fn decode(value: &str) -> Vec<u8> {
        let standard: String = value
            .chars()
            .map(|c| match c {
                '-' => '+',
                '_' => '=',
                '~' => '/',
                c => c,
            })
            .collect();
        base64::engine::general_purpose::STANDARD.decode(standard).unwrap()
    }

    #[test]
    fn the_cookies_carry_a_verifiable_policy_on_the_assets_tree() {
        let key = RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 1024).unwrap();
        let pem = key.to_pkcs1_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let signer = CloudFrontCookieSigner::new("K2JCJMDEHXQW5F".into(), &pem, "https://cdn.example.com/", Duration::hours(1)).unwrap();
        let now = DateTime::from_timestamp(1_800_000_000, 0).unwrap();

        let signed = signer.sign("restricted/post-videos/0199/", now);

        assert_eq!(
            (signed.domain.as_str(), signed.path.as_str()),
            ("cdn.example.com", "/restricted/post-videos/0199/"),
            "scoped to its own tree"
        );
        assert_eq!(signed.expires_at, now + Duration::hours(1));
        let value = |name: &str| signed.cookies.iter().find(|(n, _)| n == name).unwrap().1.clone();
        assert_eq!(value("CloudFront-Key-Pair-Id"), "K2JCJMDEHXQW5F");
        let policy = String::from_utf8(decode(&value("CloudFront-Policy"))).unwrap();
        assert_eq!(
            policy,
            r#"{"Statement":[{"Resource":"https://cdn.example.com/restricted/post-videos/0199/*","Condition":{"DateLessThan":{"AWS:EpochTime":1800003600}}}]}"#
        );
        for (_, v) in &signed.cookies {
            assert!(!v.contains(['+', '=', '/']), "CloudFront's substitutions: {v}");
        }
        let signature = Signature::try_from(decode(&value("CloudFront-Signature")).as_slice()).unwrap();
        VerifyingKey::<Sha1>::new(key.to_public_key()).verify(policy.as_bytes(), &signature).expect("RSA-SHA1 over the policy");
    }

    /// A base URL with a path (a local MinIO bucket) keeps it in the cookie
    /// path.
    #[test]
    fn the_cookie_path_includes_the_base_urls_path() {
        let key = RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 1024).unwrap();
        let pem = key.to_pkcs1_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let signer = CloudFrontCookieSigner::new("K".into(), &pem, "http://localhost:9000/media/", Duration::hours(1)).unwrap();
        let signed = signer.sign("post-videos/abc/", DateTime::from_timestamp(0, 0).unwrap());
        assert_eq!((signed.domain.as_str(), signed.path.as_str()), ("localhost", "/media/post-videos/abc/"));
    }

    #[test]
    fn a_bad_key_or_base_url_is_refused() {
        assert!(CloudFrontCookieSigner::new("K".into(), "not a key", "https://cdn.example.com", Duration::hours(1)).is_err());
        let key = RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 1024).unwrap();
        let pem = key.to_pkcs1_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        assert!(CloudFrontCookieSigner::new("K".into(), &pem, "not a url", Duration::hours(1)).is_err());
    }
}
