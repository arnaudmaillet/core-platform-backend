//! WebAuthn (passkeys, #808): the two ceremonies auth verifies, pure.
//!
//! - **Registration** (`navigator.credentials.create` / `ASAuthorization…
//!   PlatformPublicKeyCredentialRegistration`): the client data must be a
//!   `webauthn.create` for our challenge from one of our origins, and the
//!   authenticator data must be for our RP id, with the user present **and
//!   verified** (device unlock: a passkey counts as two factors) and a new
//!   credential attached. No attestation is required (any authenticator, and
//!   synced passkeys bring none): the attestation statement is not read.
//! - **Assertion** (sign-in, second step, step-up): a `webauthn.get` for our
//!   challenge and origin, user present and verified, and an ECDSA signature
//!   by the stored key over `authenticatorData ‖ SHA-256(clientDataJSON)`.
//!
//! Keys are ES256 (P-256) only: what iCloud Keychain, Google Password Manager
//! and security keys make for a passkey. Anything else is refused.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ciborium::Value;
use p256::ecdsa::signature::Verifier;
use p256::ecdsa::{Signature, VerifyingKey};
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// The relying party: the RP id the passkeys are bound to (a domain the app
/// lists under `webcredentials`) and the origins a client may report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelyingParty {
    pub id:      String,
    pub origins: Vec<String>,
}

impl RelyingParty {
    /// `origins` empty ⇒ `https://<id>` (what a native app reports for its
    /// associated domain).
    pub fn new(id: impl Into<String>, origins: Vec<String>) -> Self {
        let id = id.into();
        let origins = if origins.is_empty() { vec![format!("https://{id}")] } else { origins };
        Self { id, origins }
    }
}

/// Why a ceremony was refused. Never shown in detail to the client.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WebAuthnError {
    #[error("malformed {0}")]
    Malformed(&'static str),
    #[error("client data type is not {0}")]
    WrongType(&'static str),
    #[error("challenge mismatch")]
    Challenge,
    #[error("origin not allowed")]
    Origin,
    #[error("RP id mismatch")]
    RpId,
    #[error("user not present or not verified")]
    UserVerification,
    #[error("no credential attached")]
    NoCredential,
    #[error("unsupported key (ES256 only)")]
    UnsupportedKey,
    #[error("bad signature")]
    Signature,
    #[error("signature counter went backwards (cloned authenticator?)")]
    Counter,
}

/// A credential created by a registration, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCredential {
    pub credential_id:  Vec<u8>,
    /// SEC1 uncompressed P-256 point (65 bytes).
    pub public_key:     Vec<u8>,
    pub sign_count:     u32,
    /// The authenticator model (all zero for most synced passkeys).
    pub aaguid:         [u8; 16],
    /// Syncable (a passkey) rather than bound to one device.
    pub backup_eligible: bool,
    pub backed_up:      bool,
}

/// What a verified assertion says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssertionOutcome {
    pub sign_count: u32,
    pub backed_up:  bool,
}

const FLAG_UP: u8 = 0x01;
const FLAG_UV: u8 = 0x04;
const FLAG_BE: u8 = 0x08;
const FLAG_BS: u8 = 0x10;
const FLAG_AT: u8 = 0x40;

/// The longest credential id the spec allows.
const MAX_CREDENTIAL_ID: usize = 1023;

#[derive(Deserialize)]
struct ClientData {
    #[serde(rename = "type")]
    kind:      String,
    challenge: String,
    origin:    String,
}

fn check_client_data(
    rp: &RelyingParty,
    client_data_json: &[u8],
    kind: &'static str,
    challenge: &[u8],
) -> Result<(), WebAuthnError> {
    let data: ClientData =
        serde_json::from_slice(client_data_json).map_err(|_| WebAuthnError::Malformed("client data"))?;
    if data.kind != kind {
        return Err(WebAuthnError::WrongType(kind));
    }
    // Compared decoded: a client may or may not pad.
    let sent = URL_SAFE_NO_PAD
        .decode(data.challenge.trim_end_matches('='))
        .map_err(|_| WebAuthnError::Malformed("challenge"))?;
    if sent != challenge {
        return Err(WebAuthnError::Challenge);
    }
    if !rp.origins.contains(&data.origin) {
        return Err(WebAuthnError::Origin);
    }
    Ok(())
}

/// The fixed head of the authenticator data: RP id hash, flags, counter.
struct AuthData<'a> {
    rp_id_hash: &'a [u8],
    flags:      u8,
    sign_count: u32,
    rest:       &'a [u8],
}

fn parse_auth_data(bytes: &[u8]) -> Result<AuthData<'_>, WebAuthnError> {
    if bytes.len() < 37 {
        return Err(WebAuthnError::Malformed("authenticator data"));
    }
    Ok(AuthData {
        rp_id_hash: &bytes[..32],
        flags:      bytes[32],
        sign_count: u32::from_be_bytes([bytes[33], bytes[34], bytes[35], bytes[36]]),
        rest:       &bytes[37..],
    })
}

fn check_auth_data(rp: &RelyingParty, data: &AuthData<'_>) -> Result<(), WebAuthnError> {
    if data.rp_id_hash != Sha256::digest(rp.id.as_bytes()).as_slice() {
        return Err(WebAuthnError::RpId);
    }
    if data.flags & FLAG_UP == 0 || data.flags & FLAG_UV == 0 {
        return Err(WebAuthnError::UserVerification);
    }
    Ok(())
}

/// The COSE key of a new credential, as a SEC1 point: EC2 / ES256 / P-256.
fn cose_es256_to_sec1(key: &Value) -> Result<Vec<u8>, WebAuthnError> {
    let map = key.as_map().ok_or(WebAuthnError::Malformed("credential public key"))?;
    let get = |label: i64| {
        map.iter()
            .find(|(k, _)| k.as_integer().is_some_and(|i| i128::from(i) == i128::from(label)))
            .map(|(_, v)| v)
    };
    let int = |label: i64| get(label).and_then(Value::as_integer).map(i128::from);
    if int(1) != Some(2) || int(3) != Some(-7) || int(-1) != Some(1) {
        return Err(WebAuthnError::UnsupportedKey);
    }
    let (Some(x), Some(y)) = (get(-2).and_then(Value::as_bytes), get(-3).and_then(Value::as_bytes)) else {
        return Err(WebAuthnError::Malformed("credential public key"));
    };
    if x.len() != 32 || y.len() != 32 {
        return Err(WebAuthnError::UnsupportedKey);
    }
    let mut point = Vec::with_capacity(65);
    point.push(0x04);
    point.extend_from_slice(x);
    point.extend_from_slice(y);
    VerifyingKey::from_sec1_bytes(&point).map_err(|_| WebAuthnError::UnsupportedKey)?;
    Ok(point)
}

/// Verifies a registration for `challenge` and returns the credential to store.
pub fn verify_registration(
    rp: &RelyingParty,
    challenge: &[u8],
    client_data_json: &[u8],
    attestation_object: &[u8],
) -> Result<NewCredential, WebAuthnError> {
    check_client_data(rp, client_data_json, "webauthn.create", challenge)?;

    let object: Value =
        ciborium::from_reader(attestation_object).map_err(|_| WebAuthnError::Malformed("attestation object"))?;
    let entries = object.as_map().ok_or(WebAuthnError::Malformed("attestation object"))?;
    let auth_data = entries
        .iter()
        .find(|(k, _)| k.as_text() == Some("authData"))
        .and_then(|(_, v)| v.as_bytes())
        .ok_or(WebAuthnError::Malformed("authData"))?;

    let data = parse_auth_data(auth_data)?;
    check_auth_data(rp, &data)?;
    if data.flags & FLAG_AT == 0 {
        return Err(WebAuthnError::NoCredential);
    }
    let rest = data.rest;
    if rest.len() < 18 {
        return Err(WebAuthnError::Malformed("attested credential data"));
    }
    let mut aaguid = [0u8; 16];
    aaguid.copy_from_slice(&rest[..16]);
    let id_len = u16::from_be_bytes([rest[16], rest[17]]) as usize;
    if id_len == 0 || id_len > MAX_CREDENTIAL_ID || rest.len() < 18 + id_len {
        return Err(WebAuthnError::Malformed("credential id"));
    }
    let credential_id = rest[18..18 + id_len].to_vec();
    // The COSE key comes next; extensions may follow it and are not read.
    let key: Value =
        ciborium::from_reader(&rest[18 + id_len..]).map_err(|_| WebAuthnError::Malformed("credential public key"))?;
    let public_key = cose_es256_to_sec1(&key)?;

    Ok(NewCredential {
        credential_id,
        public_key,
        sign_count: data.sign_count,
        aaguid,
        backup_eligible: data.flags & FLAG_BE != 0,
        backed_up: data.flags & FLAG_BS != 0,
    })
}

/// Verifies an assertion for `challenge` by the stored credential (`public_key`
/// as SEC1, last `stored_count`). A counter that does not move forward is
/// refused, unless both are zero (synced passkeys keep no counter).
pub fn verify_assertion(
    rp: &RelyingParty,
    challenge: &[u8],
    public_key: &[u8],
    stored_count: u32,
    client_data_json: &[u8],
    authenticator_data: &[u8],
    signature: &[u8],
) -> Result<AssertionOutcome, WebAuthnError> {
    check_client_data(rp, client_data_json, "webauthn.get", challenge)?;
    let data = parse_auth_data(authenticator_data)?;
    check_auth_data(rp, &data)?;

    let key = VerifyingKey::from_sec1_bytes(public_key).map_err(|_| WebAuthnError::UnsupportedKey)?;
    let signature = Signature::from_der(signature).map_err(|_| WebAuthnError::Signature)?;
    let mut signed = authenticator_data.to_vec();
    signed.extend_from_slice(&Sha256::digest(client_data_json));
    key.verify(&signed, &signature).map_err(|_| WebAuthnError::Signature)?;

    if (data.sign_count != 0 || stored_count != 0) && data.sign_count <= stored_count {
        return Err(WebAuthnError::Counter);
    }
    Ok(AssertionOutcome { sign_count: data.sign_count, backed_up: data.flags & FLAG_BS != 0 })
}

#[cfg(any(test, feature = "integration-auth"))]
pub mod testing {
    //! A software authenticator, to build ceremonies in tests.
    use p256::ecdsa::signature::Signer;
    use p256::ecdsa::SigningKey;

    use super::*;

    pub struct SoftAuthenticator {
        pub key:           SigningKey,
        pub credential_id: Vec<u8>,
        pub count:         u32,
    }

    impl Default for SoftAuthenticator {
        fn default() -> Self {
            Self::new()
        }
    }

    impl SoftAuthenticator {
        pub fn new() -> Self {
            use rand::RngCore;
            let key = loop {
                let mut bytes = [0u8; 32];
                rand::rng().fill_bytes(&mut bytes);
                if let Ok(key) = SigningKey::from_slice(&bytes) {
                    break key;
                }
            };
            Self { key, credential_id: uuid::Uuid::now_v7().as_bytes().to_vec(), count: 0 }
        }

        pub fn client_data(kind: &str, challenge: &[u8], origin: &str) -> Vec<u8> {
            serde_json::to_vec(&serde_json::json!({
                "type": kind,
                "challenge": URL_SAFE_NO_PAD.encode(challenge),
                "origin": origin,
                "crossOrigin": false,
            }))
            .unwrap()
        }

        fn head(rp_id: &str, flags: u8, count: u32) -> Vec<u8> {
            let mut data = Sha256::digest(rp_id.as_bytes()).to_vec();
            data.push(flags);
            data.extend_from_slice(&count.to_be_bytes());
            data
        }

        /// An `attestationObject` (format `none`) for this credential.
        pub fn attestation(&self, rp_id: &str, flags: u8) -> Vec<u8> {
            let point = self.key.verifying_key().to_encoded_point(false);
            let cose = Value::Map(vec![
                (Value::Integer(1.into()), Value::Integer(2.into())),
                (Value::Integer(3.into()), Value::Integer((-7).into())),
                (Value::Integer((-1).into()), Value::Integer(1.into())),
                (Value::Integer((-2).into()), Value::Bytes(point.x().unwrap().to_vec())),
                (Value::Integer((-3).into()), Value::Bytes(point.y().unwrap().to_vec())),
            ]);
            let mut data = Self::head(rp_id, flags | FLAG_AT, self.count);
            data.extend_from_slice(&[0u8; 16]);
            data.extend_from_slice(&(self.credential_id.len() as u16).to_be_bytes());
            data.extend_from_slice(&self.credential_id);
            ciborium::into_writer(&cose, &mut data).unwrap();
            let object = Value::Map(vec![
                (Value::Text("fmt".into()), Value::Text("none".into())),
                (Value::Text("attStmt".into()), Value::Map(vec![])),
                (Value::Text("authData".into()), Value::Bytes(data)),
            ]);
            let mut out = Vec::new();
            ciborium::into_writer(&object, &mut out).unwrap();
            out
        }

        /// `(authenticatorData, signature)` over `client_data`.
        pub fn assert(&mut self, rp_id: &str, flags: u8, client_data: &[u8], bump: bool) -> (Vec<u8>, Vec<u8>) {
            if bump {
                self.count += 1;
            }
            let data = Self::head(rp_id, flags, self.count);
            let mut signed = data.clone();
            signed.extend_from_slice(&Sha256::digest(client_data));
            let signature: Signature = self.key.sign(&signed);
            (data, signature.to_der().as_bytes().to_vec())
        }

        pub fn public_key(&self) -> Vec<u8> {
            self.key.verifying_key().to_encoded_point(false).as_bytes().to_vec()
        }
    }

    /// User present + verified (+ backup eligible and backed up: a passkey).
    pub const PASSKEY: u8 = FLAG_UP | FLAG_UV | FLAG_BE | FLAG_BS;
    /// User present, not verified (no device unlock).
    pub const PRESENT_ONLY: u8 = FLAG_UP;
}

#[cfg(test)]
mod tests {
    use super::testing::{SoftAuthenticator, PASSKEY};
    use super::*;

    const RP_ID: &str = "example.app";
    const ORIGIN: &str = "https://example.app";

    fn rp() -> RelyingParty {
        RelyingParty::new(RP_ID, vec![])
    }

    #[test]
    fn the_default_origin_is_the_rp_ids() {
        assert_eq!(rp().origins, vec![ORIGIN.to_owned()]);
        assert_eq!(RelyingParty::new(RP_ID, vec!["android:apk-key-hash:x".into()]).origins.len(), 1);
    }

    #[test]
    fn a_genuine_registration_yields_the_credential() {
        let auth = SoftAuthenticator::new();
        let challenge = [7u8; 32];
        let client = SoftAuthenticator::client_data("webauthn.create", &challenge, ORIGIN);
        let made = verify_registration(&rp(), &challenge, &client, &auth.attestation(RP_ID, PASSKEY)).unwrap();
        assert_eq!(made.credential_id, auth.credential_id);
        assert_eq!(made.public_key, auth.public_key());
        assert!(made.backup_eligible && made.backed_up);
    }

    #[test]
    fn a_registration_that_is_off_in_any_way_is_refused() {
        let auth = SoftAuthenticator::new();
        let challenge = [7u8; 32];
        let good = SoftAuthenticator::client_data("webauthn.create", &challenge, ORIGIN);
        let object = auth.attestation(RP_ID, PASSKEY);
        let refused = |client: &[u8], object: &[u8]| verify_registration(&rp(), &challenge, client, object).unwrap_err();

        let other_challenge = SoftAuthenticator::client_data("webauthn.create", &[8u8; 32], ORIGIN);
        assert_eq!(refused(&other_challenge, &object), WebAuthnError::Challenge);
        let get = SoftAuthenticator::client_data("webauthn.get", &challenge, ORIGIN);
        assert_eq!(refused(&get, &object), WebAuthnError::WrongType("webauthn.create"));
        let phishing = SoftAuthenticator::client_data("webauthn.create", &challenge, "https://examp1e.app");
        assert_eq!(refused(&phishing, &object), WebAuthnError::Origin);
        assert_eq!(refused(&good, &auth.attestation("evil.app", PASSKEY)), WebAuthnError::RpId);
        assert_eq!(refused(&good, &auth.attestation(RP_ID, FLAG_UP)), WebAuthnError::UserVerification);
        assert!(matches!(refused(&good, b"garbage"), WebAuthnError::Malformed(_)));
        assert!(matches!(refused(b"{}", &object), WebAuthnError::Malformed(_)));
    }

    #[test]
    fn an_assertion_by_the_stored_key_verifies_and_anything_else_is_refused() {
        let mut auth = SoftAuthenticator::new();
        let key = auth.public_key();
        let challenge = [9u8; 32];
        let client = SoftAuthenticator::client_data("webauthn.get", &challenge, ORIGIN);

        // A synced passkey keeps its counter at zero.
        let (data, sig) = auth.assert(RP_ID, PASSKEY, &client, false);
        let ok = verify_assertion(&rp(), &challenge, &key, 0, &client, &data, &sig).unwrap();
        assert_eq!(ok, AssertionOutcome { sign_count: 0, backed_up: true });

        // A counting authenticator must move forward.
        let (data, sig) = auth.assert(RP_ID, PASSKEY, &client, true);
        assert_eq!(verify_assertion(&rp(), &challenge, &key, 0, &client, &data, &sig).unwrap().sign_count, 1);
        assert_eq!(verify_assertion(&rp(), &challenge, &key, 1, &client, &data, &sig), Err(WebAuthnError::Counter));

        // Another key, a tampered client data, another challenge, no UV.
        let other = SoftAuthenticator::new().public_key();
        assert_eq!(verify_assertion(&rp(), &challenge, &other, 0, &client, &data, &sig), Err(WebAuthnError::Signature));
        let tampered = SoftAuthenticator::client_data("webauthn.get", &challenge, ORIGIN)
            .into_iter()
            .chain(b" ".iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(verify_assertion(&rp(), &challenge, &key, 0, &tampered, &data, &sig), Err(WebAuthnError::Signature));
        assert_eq!(verify_assertion(&rp(), &[1u8; 32], &key, 0, &client, &data, &sig), Err(WebAuthnError::Challenge));
        let (weak, weak_sig) = auth.assert(RP_ID, FLAG_UP, &client, true);
        assert_eq!(
            verify_assertion(&rp(), &challenge, &key, 0, &client, &weak, &weak_sig),
            Err(WebAuthnError::UserVerification)
        );
    }
}
