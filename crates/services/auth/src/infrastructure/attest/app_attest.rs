//! Apple App Attest: verifying a key attestation (guest mode B5b).
//!
//! The app generates a key in the Secure Enclave and asks Apple to attest it for
//! a one-time challenge of ours; the attestation object (CBOR, `apple-appattest`)
//! proves the key belongs to a genuine install of **our** app on a real Apple
//! device. Checked, per Apple's "Validating apps that connect to your server":
//!
//! 1. the certificate chain `x5c` (leaf, intermediate) up to Apple's App
//!    Attestation Root CA, each certificate within its validity;
//! 2. the nonce in the leaf (extension `1.2.840.113635.100.8.2`) equals
//!    `SHA-256(authData ‖ SHA-256(challenge))`;
//! 3. the key id equals `SHA-256` of the leaf's public key;
//! 4. `authData`'s RP id hash equals `SHA-256` of an accepted app id
//!    (`<team id>.<bundle id>`, from configuration — never in code);
//! 5. its sign counter is 0, its AAGUID names an accepted environment
//!    (`appattestdevelop` / `appattest` + 7 zero bytes), and its credential id is
//!    the key id.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use x509_cert::der::{Decode, Encode};
use p256::ecdsa::signature::hazmat::PrehashVerifier;
use sha2::{Digest, Sha256, Sha384};
use x509_cert::Certificate;

/// Apple's App Attestation Root CA (public; https://www.apple.com/certificateauthority/).
pub const APPLE_APP_ATTESTATION_ROOT_CA: &str = include_str!("apple_app_attestation_root_ca.pem");

const NONCE_EXTENSION: &str = "1.2.840.113635.100.8.2";
const ECDSA_SHA256: &str = "1.2.840.10045.4.3.2";
const ECDSA_SHA384: &str = "1.2.840.10045.4.3.3";
const SECP256R1: &str = "1.2.840.10045.3.1.7";
const SECP384R1: &str = "1.3.132.0.34";

/// Which App Attest environment issued a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttestEnvironment {
    /// Builds signed for development (run from Xcode).
    Development,
    /// TestFlight and App Store builds.
    Production,
}

impl AttestEnvironment {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "development" => Some(Self::Development),
            "production" => Some(Self::Production),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Production => "production",
        }
    }

    fn aaguid(self) -> [u8; 16] {
        match self {
            Self::Development => *b"appattestdevelop",
            Self::Production => *b"appattest\0\0\0\0\0\0\0",
        }
    }
}

/// A key Apple attested for one of our apps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttestedKey {
    /// base64 of `SHA-256(public key)`, as the app knows it.
    pub key_id:      String,
    pub app_id:      String,
    pub environment: AttestEnvironment,
    /// The key's public point (SEC1, uncompressed), for later assertions.
    pub public_key:  Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AttestError {
    #[error("malformed attestation: {0}")]
    Malformed(String),
    #[error("certificate chain not trusted: {0}")]
    UntrustedChain(String),
    #[error("attestation does not match: {0}")]
    Mismatch(&'static str),
}

/// Verifies attestations for the configured apps and environments.
#[derive(Debug, Clone)]
pub struct AppAttestVerifier {
    root: Certificate,
    /// `(app id, SHA-256(app id))`.
    apps: Vec<(String, [u8; 32])>,
    environments: Vec<AttestEnvironment>,
}

impl AppAttestVerifier {
    /// `app_ids` are `<team id>.<bundle id>`; the trust anchor is Apple's root.
    pub fn new(app_ids: &[String], environments: &[AttestEnvironment]) -> Result<Self, AttestError> {
        Self::with_root(APPLE_APP_ATTESTATION_ROOT_CA, app_ids, environments)
    }

    /// The same with another trust anchor (tests).
    pub fn with_root(
        root_pem: &str,
        app_ids: &[String],
        environments: &[AttestEnvironment],
    ) -> Result<Self, AttestError> {
        let root = pem_to_cert(root_pem)?;
        let apps = app_ids
            .iter()
            .map(|id| (id.clone(), Sha256::digest(id.as_bytes()).into()))
            .collect();
        Ok(Self { root, apps, environments: environments.to_vec() })
    }

    /// Verifies `attestation` (base64 of the CBOR object) for `key_id` (base64)
    /// and our one-time `challenge` (the app signs `SHA-256(challenge)`).
    pub fn verify(
        &self,
        key_id: &str,
        attestation: &str,
        challenge: &str,
        now: SystemTime,
    ) -> Result<AttestedKey, AttestError> {
        let b64 = |s: &str| {
            base64::engine::general_purpose::STANDARD
                .decode(s.trim())
                .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s.trim()))
                .map_err(|_| AttestError::Malformed("not base64".into()))
        };
        let key_id_bytes = b64(key_id)?;
        let object = AttestationObject::parse(&b64(attestation)?)?;

        // 1. The chain: leaf ← intermediate ← Apple's root, all valid now.
        let [leaf_der, intermediate_der] = object.x5c.as_slice() else {
            return Err(AttestError::Malformed("x5c must hold the leaf and the intermediate".into()));
        };
        let leaf = Certificate::from_der(leaf_der).map_err(|e| AttestError::Malformed(format!("leaf: {e}")))?;
        let intermediate = Certificate::from_der(intermediate_der)
            .map_err(|e| AttestError::Malformed(format!("intermediate: {e}")))?;
        for cert in [&leaf, &intermediate, &self.root] {
            within_validity(cert, now)?;
        }
        verify_signed_by(&intermediate, &self.root)?;
        verify_signed_by(&leaf, &intermediate)?;

        // 2. The nonce binds the attestation to our challenge.
        let client_data_hash = Sha256::digest(challenge.as_bytes());
        let mut hasher = Sha256::new();
        hasher.update(&object.auth_data);
        hasher.update(client_data_hash);
        let nonce: [u8; 32] = hasher.finalize().into();
        if leaf_nonce(&leaf)? != nonce {
            return Err(AttestError::Mismatch("nonce"));
        }

        // 3. The key id is the hash of the attested public key.
        let public_key = leaf.tbs_certificate.subject_public_key_info.subject_public_key.raw_bytes().to_vec();
        if Sha256::digest(&public_key).as_slice() != key_id_bytes.as_slice() {
            return Err(AttestError::Mismatch("key id"));
        }

        // 4–5. authData: our app, a fresh key, an accepted environment.
        let auth = AuthData::parse(&object.auth_data)?;
        let Some((app_id, _)) = self.apps.iter().find(|(_, hash)| *hash == auth.rp_id_hash) else {
            return Err(AttestError::Mismatch("app id"));
        };
        if auth.sign_count != 0 {
            return Err(AttestError::Mismatch("sign counter"));
        }
        let Some(environment) = self.environments.iter().copied().find(|env| env.aaguid() == auth.aaguid) else {
            return Err(AttestError::Mismatch("environment"));
        };
        if auth.credential_id != key_id_bytes {
            return Err(AttestError::Mismatch("credential id"));
        }

        Ok(AttestedKey {
            key_id: base64::engine::general_purpose::STANDARD.encode(&key_id_bytes),
            app_id: app_id.clone(),
            environment,
            public_key,
        })
    }
}

impl crate::application::command::DeviceAttestationVerifier for AppAttestVerifier {
    fn verify(
        &self,
        proof: &crate::application::command::AttestationProof,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<crate::application::command::AttestedDevice, String> {
        let now = UNIX_EPOCH + Duration::from_secs(now.timestamp().max(0) as u64);
        AppAttestVerifier::verify(self, &proof.key_id, &proof.attestation, &proof.challenge, now)
            .map(|key| crate::application::command::AttestedDevice {
                key_id: key.key_id,
                environment: key.environment.as_str().to_owned(),
            })
            .map_err(|e| e.to_string())
    }
}

fn pem_to_cert(pem: &str) -> Result<Certificate, AttestError> {
    let body: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(body.trim())
        .map_err(|_| AttestError::Malformed("root PEM".into()))?;
    Certificate::from_der(&der).map_err(|e| AttestError::Malformed(format!("root: {e}")))
}

fn within_validity(cert: &Certificate, now: SystemTime) -> Result<(), AttestError> {
    let now = now.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let validity = &cert.tbs_certificate.validity;
    if now < validity.not_before.to_unix_duration() || now > validity.not_after.to_unix_duration() {
        return Err(AttestError::UntrustedChain("a certificate is outside its validity".into()));
    }
    Ok(())
}

/// Checks `cert`'s signature with `issuer`'s key (ECDSA P-256 / P-384, SHA-256 / SHA-384).
fn verify_signed_by(cert: &Certificate, issuer: &Certificate) -> Result<(), AttestError> {
    if cert.tbs_certificate.issuer != issuer.tbs_certificate.subject {
        return Err(AttestError::UntrustedChain("issuer name mismatch".into()));
    }
    let tbs = cert.tbs_certificate.to_der().map_err(|e| AttestError::Malformed(format!("tbs: {e}")))?;
    let prehash: Vec<u8> = match cert.signature_algorithm.oid.to_string().as_str() {
        ECDSA_SHA256 => Sha256::digest(&tbs).to_vec(),
        ECDSA_SHA384 => Sha384::digest(&tbs).to_vec(),
        other => return Err(AttestError::UntrustedChain(format!("signature algorithm {other}"))),
    };
    let signature = cert.signature.raw_bytes();
    let spki = &issuer.tbs_certificate.subject_public_key_info;
    let point = spki.subject_public_key.raw_bytes();
    let curve = spki
        .algorithm
        .parameters
        .as_ref()
        .and_then(|p| p.decode_as::<x509_cert::der::asn1::ObjectIdentifier>().ok())
        .map(|oid| oid.to_string())
        .unwrap_or_default();
    let verified = match curve.as_str() {
        SECP256R1 => p256::ecdsa::VerifyingKey::from_sec1_bytes(point)
            .ok()
            .zip(p256::ecdsa::DerSignature::try_from(signature).ok())
            .is_some_and(|(key, sig)| key.verify_prehash(&prehash, &sig).is_ok()),
        SECP384R1 => p384::ecdsa::VerifyingKey::from_sec1_bytes(point)
            .ok()
            .zip(p384::ecdsa::DerSignature::try_from(signature).ok())
            .is_some_and(|(key, sig)| key.verify_prehash(&prehash, &sig).is_ok()),
        other => return Err(AttestError::UntrustedChain(format!("issuer curve {other}"))),
    };
    if verified { Ok(()) } else { Err(AttestError::UntrustedChain("bad certificate signature".into())) }
}

/// The 32-byte nonce in the leaf: `SEQUENCE { [1] EXPLICIT OCTET STRING }`.
fn leaf_nonce(leaf: &Certificate) -> Result<[u8; 32], AttestError> {
    let ext = leaf
        .tbs_certificate
        .extensions
        .as_ref()
        .and_then(|exts| exts.iter().find(|e| e.extn_id.to_string() == NONCE_EXTENSION))
        .ok_or(AttestError::Malformed("no nonce extension".into()))?;
    let bytes = ext.extn_value.as_bytes();
    // 30 L  A1 L  04 20 <32 bytes> (short-form lengths: the value is small).
    match bytes {
        [0x30, _, 0xA1, _, 0x04, 0x20, nonce @ ..] if nonce.len() == 32 => {
            let mut out = [0u8; 32];
            out.copy_from_slice(nonce);
            Ok(out)
        }
        _ => Err(AttestError::Malformed("nonce extension".into())),
    }
}

/// `{"fmt": "apple-appattest", "attStmt": {"x5c": [...], "receipt": ...}, "authData": ...}`.
struct AttestationObject {
    x5c:       Vec<Vec<u8>>,
    auth_data: Vec<u8>,
}

impl AttestationObject {
    fn parse(bytes: &[u8]) -> Result<Self, AttestError> {
        use ciborium::value::Value;
        let malformed = |what: &str| AttestError::Malformed(what.to_owned());
        let value: Value = ciborium::de::from_reader(bytes).map_err(|_| malformed("not CBOR"))?;
        let map = value.as_map().ok_or_else(|| malformed("not a map"))?;
        let get = |key: &str| map.iter().find(|(k, _)| k.as_text() == Some(key)).map(|(_, v)| v);
        if get("fmt").and_then(Value::as_text) != Some("apple-appattest") {
            return Err(malformed("fmt is not apple-appattest"));
        }
        let statement = get("attStmt").and_then(Value::as_map).ok_or_else(|| malformed("attStmt"))?;
        let x5c = statement
            .iter()
            .find(|(k, _)| k.as_text() == Some("x5c"))
            .and_then(|(_, v)| v.as_array())
            .ok_or_else(|| malformed("x5c"))?
            .iter()
            .map(|c| c.as_bytes().cloned().ok_or_else(|| malformed("x5c entry")))
            .collect::<Result<Vec<_>, _>>()?;
        let auth_data = get("authData").and_then(Value::as_bytes).cloned().ok_or_else(|| malformed("authData"))?;
        Ok(Self { x5c, auth_data })
    }
}

/// `rpIdHash(32) ‖ flags(1) ‖ signCount(4) ‖ aaguid(16) ‖ credIdLen(2) ‖ credId ‖ …`.
struct AuthData {
    rp_id_hash:    [u8; 32],
    sign_count:    u32,
    aaguid:        [u8; 16],
    credential_id: Vec<u8>,
}

impl AuthData {
    fn parse(bytes: &[u8]) -> Result<Self, AttestError> {
        let short = || AttestError::Malformed("authData too short".into());
        if bytes.len() < 55 {
            return Err(short());
        }
        let mut rp_id_hash = [0u8; 32];
        rp_id_hash.copy_from_slice(&bytes[..32]);
        let sign_count = u32::from_be_bytes([bytes[33], bytes[34], bytes[35], bytes[36]]);
        let mut aaguid = [0u8; 16];
        aaguid.copy_from_slice(&bytes[37..53]);
        let len = usize::from(u16::from_be_bytes([bytes[53], bytes[54]]));
        let credential_id = bytes.get(55..55 + len).ok_or_else(short)?.to_vec();
        Ok(Self { rp_id_hash, sign_count, aaguid, credential_id })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use x509_cert::der::asn1::ObjectIdentifier;
    use x509_cert::builder::{Builder, CertificateBuilder, Profile};
    use x509_cert::ext::{AsExtension, Extension};
    use x509_cert::name::Name;
    use x509_cert::serial_number::SerialNumber;
    use x509_cert::spki::SubjectPublicKeyInfoOwned;
    use x509_cert::time::Validity;

    use super::*;

    const APP_ID: &str = "TEAMID1234.com.example.app";

    #[test]
    fn the_embedded_root_is_apples() {
        let pem = APPLE_APP_ATTESTATION_ROOT_CA;
        let body: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
        let der = base64::engine::general_purpose::STANDARD.decode(body).unwrap();
        let fingerprint: String = Sha256::digest(&der).iter().map(|b| format!("{b:02X}")).collect();
        assert_eq!(fingerprint, "1CB9823BA28BA6AD2D33A006941DE2AE4F513EF1D4E831B9F7E0FA7B6242C932");
        assert!(AppAttestVerifier::new(&[APP_ID.into()], &[AttestEnvironment::Production]).is_ok());
    }

    /// The nonce extension as Apple encodes it.
    #[derive(Clone)]
    struct NonceExt([u8; 32]);

    impl x509_cert::der::Encode for NonceExt {
        fn encoded_len(&self) -> x509_cert::der::Result<x509_cert::der::Length> {
            x509_cert::der::Length::try_from(38u32)
        }
        fn encode(&self, writer: &mut impl x509_cert::der::Writer) -> x509_cert::der::Result<()> {
            writer.write(&[0x30, 0x24, 0xA1, 0x22, 0x04, 0x20])?;
            writer.write(&self.0)
        }
    }

    impl x509_cert::der::oid::AssociatedOid for NonceExt {
        const OID: ObjectIdentifier = ObjectIdentifier::new_unwrap(NONCE_EXTENSION);
    }

    impl AsExtension for NonceExt {
        fn critical(&self, _: &Name, _: &[Extension]) -> bool {
            false
        }
    }

    /// A synthetic App Attest flow: a test root standing in for Apple's, an
    /// intermediate, and a leaf for a fresh device key — all ephemeral.
    struct Flow {
        root_pem:  String,
        key_id:    String,
        challenge: String,
    }

    /// Valid from a minute ago to an hour from now.
    fn validity() -> Validity {
        let now = SystemTime::now();
        Validity {
            not_before: x509_cert::time::Time::try_from(now - Duration::from_secs(60)).unwrap(),
            not_after: x509_cert::time::Time::try_from(now + Duration::from_secs(3600)).unwrap(),
        }
    }

    fn attest(app_id: &str, aaguid: [u8; 16], sign_count: u32, tamper_nonce: bool) -> (Flow, String) {
        let root_key = p384::ecdsa::SigningKey::random(&mut rand_core::OsRng);
        let root_name = Name::from_str("CN=Test App Attestation Root CA").unwrap();
        let root_spki = SubjectPublicKeyInfoOwned::from_key(*root_key.verifying_key()).unwrap();
        let root = CertificateBuilder::new(Profile::Root, SerialNumber::from(1u32), validity(), root_name.clone(), root_spki, &root_key)
            .unwrap()
            .build::<p384::ecdsa::DerSignature>()
            .unwrap();

        let ca_key = p384::ecdsa::SigningKey::random(&mut rand_core::OsRng);
        let ca_name = Name::from_str("CN=Test App Attestation CA 1").unwrap();
        let ca_spki = SubjectPublicKeyInfoOwned::from_key(*ca_key.verifying_key()).unwrap();
        let ca = CertificateBuilder::new(
            Profile::SubCA { issuer: root_name, path_len_constraint: Some(0) },
            SerialNumber::from(2u32),
            validity(),
            ca_name.clone(),
            ca_spki,
            &root_key,
        )
        .unwrap()
        .build::<p384::ecdsa::DerSignature>()
        .unwrap();

        // The device key, and what the app does with our challenge.
        let device_key = p256::ecdsa::SigningKey::random(&mut rand_core::OsRng);
        let point = device_key.verifying_key().to_encoded_point(false).as_bytes().to_vec();
        let key_id_bytes = Sha256::digest(&point).to_vec();
        let challenge = "server-challenge-123".to_owned();
        let mut auth_data = Sha256::digest(app_id.as_bytes()).to_vec();
        auth_data.push(0x40);
        auth_data.extend_from_slice(&sign_count.to_be_bytes());
        auth_data.extend_from_slice(&aaguid);
        auth_data.extend_from_slice(&(key_id_bytes.len() as u16).to_be_bytes());
        auth_data.extend_from_slice(&key_id_bytes);
        let mut nonce_hasher = Sha256::new();
        nonce_hasher.update(&auth_data);
        nonce_hasher.update(Sha256::digest(challenge.as_bytes()));
        let mut nonce: [u8; 32] = nonce_hasher.finalize().into();
        if tamper_nonce {
            nonce[0] ^= 1;
        }

        let leaf_spki = SubjectPublicKeyInfoOwned::from_key(*device_key.verifying_key()).unwrap();
        let mut leaf = CertificateBuilder::new(
            Profile::Leaf { issuer: ca_name, enable_key_agreement: false, enable_key_encipherment: false },
            SerialNumber::from(3u32),
            validity(),
            Name::from_str("CN=device key").unwrap(),
            leaf_spki,
            &ca_key,
        )
        .unwrap();
        leaf.add_extension(&NonceExt(nonce)).unwrap();
        let leaf = leaf.build::<p384::ecdsa::DerSignature>().unwrap();

        use ciborium::value::Value;
        let object = Value::Map(vec![
            (Value::Text("fmt".into()), Value::Text("apple-appattest".into())),
            (
                Value::Text("attStmt".into()),
                Value::Map(vec![
                    (
                        Value::Text("x5c".into()),
                        Value::Array(vec![Value::Bytes(leaf.to_der().unwrap()), Value::Bytes(ca.to_der().unwrap())]),
                    ),
                    (Value::Text("receipt".into()), Value::Bytes(vec![1, 2, 3])),
                ]),
            ),
            (Value::Text("authData".into()), Value::Bytes(auth_data)),
        ]);
        let mut cbor = Vec::new();
        ciborium::ser::into_writer(&object, &mut cbor).unwrap();

        let root_der = root.to_der().unwrap();
        let root_pem = format!(
            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
            base64::engine::general_purpose::STANDARD.encode(root_der)
        );
        let b64 = |b: &[u8]| base64::engine::general_purpose::STANDARD.encode(b);
        (Flow { root_pem, key_id: b64(&key_id_bytes), challenge }, b64(&cbor))
    }

    fn verifier(flow: &Flow, environments: &[AttestEnvironment]) -> AppAttestVerifier {
        AppAttestVerifier::with_root(&flow.root_pem, &[APP_ID.into()], environments).unwrap()
    }

    #[test]
    fn a_genuine_attestation_for_our_app_verifies() {
        let (flow, attestation) = attest(APP_ID, AttestEnvironment::Production.aaguid(), 0, false);
        let key = verifier(&flow, &[AttestEnvironment::Production])
            .verify(&flow.key_id, &attestation, &flow.challenge, SystemTime::now())
            .unwrap();
        assert_eq!(key.app_id, APP_ID);
        assert_eq!(key.environment, AttestEnvironment::Production);
        assert_eq!(key.key_id, flow.key_id);
        assert_eq!(key.public_key.len(), 65);
    }

    #[test]
    fn anything_off_is_refused() {
        let prod = [AttestEnvironment::Production];
        let now = SystemTime::now();

        // Another team's / bundle's app.
        let (flow, att) = attest("OTHERTEAM.com.evil.app", AttestEnvironment::Production.aaguid(), 0, false);
        assert_eq!(verifier(&flow, &prod).verify(&flow.key_id, &att, &flow.challenge, now), Err(AttestError::Mismatch("app id")));

        // A development key where only production is accepted.
        let (flow, att) = attest(APP_ID, AttestEnvironment::Development.aaguid(), 0, false);
        assert_eq!(verifier(&flow, &prod).verify(&flow.key_id, &att, &flow.challenge, now), Err(AttestError::Mismatch("environment")));
        assert!(verifier(&flow, &[AttestEnvironment::Production, AttestEnvironment::Development])
            .verify(&flow.key_id, &att, &flow.challenge, now)
            .is_ok());

        // Another challenge (a replay), a tampered nonce, a used key.
        let (flow, att) = attest(APP_ID, AttestEnvironment::Production.aaguid(), 0, false);
        assert_eq!(verifier(&flow, &prod).verify(&flow.key_id, &att, "another-challenge", now), Err(AttestError::Mismatch("nonce")));
        let (flow, att) = attest(APP_ID, AttestEnvironment::Production.aaguid(), 0, true);
        assert_eq!(verifier(&flow, &prod).verify(&flow.key_id, &att, &flow.challenge, now), Err(AttestError::Mismatch("nonce")));
        let (flow, att) = attest(APP_ID, AttestEnvironment::Production.aaguid(), 3, false);
        assert_eq!(verifier(&flow, &prod).verify(&flow.key_id, &att, &flow.challenge, now), Err(AttestError::Mismatch("sign counter")));

        // Another key id than the attested one.
        let (flow, att) = attest(APP_ID, AttestEnvironment::Production.aaguid(), 0, false);
        let other = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        assert_eq!(verifier(&flow, &prod).verify(&other, &att, &flow.challenge, now), Err(AttestError::Mismatch("key id")));

        // A chain that does not lead to the trusted root (Apple's, here).
        let real = AppAttestVerifier::new(&[APP_ID.into()], &prod).unwrap();
        assert!(matches!(real.verify(&flow.key_id, &att, &flow.challenge, now), Err(AttestError::UntrustedChain(_))));

        // Expired certificates.
        let later = now + Duration::from_secs(2 * 3600);
        assert!(matches!(
            verifier(&flow, &prod).verify(&flow.key_id, &att, &flow.challenge, later),
            Err(AttestError::UntrustedChain(_))
        ));

        // Garbage.
        assert!(matches!(verifier(&flow, &prod).verify(&flow.key_id, "AAAA", &flow.challenge, now), Err(AttestError::Malformed(_))));
    }
}
