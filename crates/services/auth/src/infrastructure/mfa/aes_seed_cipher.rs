//! The two-step material's key (#649): `AUTH_MFA_SEED_KEY` (32 bytes,
//! standard base64) with its id `AUTH_MFA_SEED_KEY_ID` (default `k1`), and
//! the keys it replaced in `AUTH_MFA_SEED_KEYS_PREVIOUS` (`id:base64,…`).
//!
//! A sealed seed is `1 ‖ len(id) ‖ id ‖ nonce(12) ‖ AES-256-GCM(seed)`, the
//! header bound as associated data. A backup-code hash is
//! `<id>$<hex HMAC-SHA-256>` under a sub-key derived from the seed key.

use std::collections::HashMap;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2_hmac::Sha256;

use crate::application::port::MfaSeedCipher;
use crate::error::AuthError;

const VERSION: u8 = 1;
const NONCE_LEN: usize = 12;
const CODES_CONTEXT: &[u8] = b"core-platform/auth/mfa/backup-codes/v1";

struct Key {
    seal: Aes256Gcm,
    codes: [u8; 32],
}

impl Key {
    fn new(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != 32 {
            return Err(format!("a seed key is 32 bytes, got {}", bytes.len()));
        }
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(bytes).expect("HMAC takes any key length");
        mac.update(CODES_CONTEXT);
        Ok(Self {
            seal: Aes256Gcm::new_from_slice(bytes).map_err(|e| e.to_string())?,
            codes: mac.finalize().into_bytes().into(),
        })
    }

    fn code_hash(&self, id: &str, code: &str) -> String {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.codes).expect("HMAC takes any key length");
        mac.update(code.as_bytes());
        let digest = mac.finalize().into_bytes();
        format!("{id}${}", digest.iter().map(|b| format!("{b:02x}")).collect::<String>())
    }
}

/// AES-256-GCM over seeds, HMAC-SHA-256 over backup codes.
pub struct AesSeedCipher {
    current: String,
    keys: HashMap<String, Key>,
}

impl AesSeedCipher {
    /// `current` seals; `previous` (id, key) only open and match older codes.
    pub fn new(current_id: &str, current: &[u8], previous: &[(String, Vec<u8>)]) -> Result<Self, String> {
        let valid_id = |id: &str| {
            !id.is_empty() && id.len() <= 16 && id.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        };
        let mut keys = HashMap::new();
        for (id, bytes) in previous.iter().map(|(id, k)| (id.as_str(), k.as_slice())).chain([(current_id, current)]) {
            if !valid_id(id) {
                return Err(format!("bad seed key id {id:?} (≤ 16 of [a-z0-9-])"));
            }
            keys.insert(id.to_owned(), Key::new(bytes)?);
        }
        Ok(Self { current: current_id.to_owned(), keys })
    }

    /// From the environment; `Ok(None)` when no key is configured (two-step
    /// sign-in then fails closed).
    pub fn from_env() -> Result<Option<Self>, String> {
        let Some(key) = std::env::var("AUTH_MFA_SEED_KEY").ok().filter(|k| !k.trim().is_empty()) else {
            return Ok(None);
        };
        let decode = |s: &str| {
            base64::engine::general_purpose::STANDARD.decode(s.trim()).map_err(|e| format!("seed key is not base64: {e}"))
        };
        let id = std::env::var("AUTH_MFA_SEED_KEY_ID").ok().filter(|i| !i.trim().is_empty()).unwrap_or_else(|| "k1".into());
        let previous = std::env::var("AUTH_MFA_SEED_KEYS_PREVIOUS")
            .unwrap_or_default()
            .split(',')
            .filter(|entry| !entry.trim().is_empty())
            .map(|entry| {
                let (id, key) = entry.trim().split_once(':').ok_or("previous seed keys are id:base64")?;
                Ok((id.to_owned(), decode(key)?))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Self::new(id.trim(), &decode(&key)?, &previous).map(Some)
    }

    fn header(id: &str) -> Vec<u8> {
        let mut header = vec![VERSION, id.len() as u8];
        header.extend_from_slice(id.as_bytes());
        header
    }
}

impl MfaSeedCipher for AesSeedCipher {
    fn seal(&self, seed: &[u8]) -> Result<Vec<u8>, AuthError> {
        let key = &self.keys[&self.current];
        let header = Self::header(&self.current);
        let mut nonce = [0u8; NONCE_LEN];
        rand::rng().fill_bytes(&mut nonce);
        let sealed = key
            .seal
            .encrypt(Nonce::from_slice(&nonce), Payload { msg: seed, aad: &header })
            .map_err(|_| AuthError::MfaUnavailable)?;
        Ok([header.as_slice(), &nonce, &sealed].concat())
    }

    fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, AuthError> {
        let corrupt = || AuthError::DomainViolation { field: "totp_secret".into(), message: "unreadable sealed seed".into() };
        let (&version, rest) = sealed.split_first().ok_or_else(corrupt)?;
        let (&id_len, rest) = rest.split_first().ok_or_else(corrupt)?;
        if version != VERSION || rest.len() < id_len as usize + NONCE_LEN {
            return Err(corrupt());
        }
        let (id, rest) = rest.split_at(id_len as usize);
        let (nonce, ciphertext) = rest.split_at(NONCE_LEN);
        let id = std::str::from_utf8(id).map_err(|_| corrupt())?;
        // Sealed under a key this deployment no longer holds: fail closed.
        let key = self.keys.get(id).ok_or(AuthError::MfaUnavailable)?;
        key.seal
            .decrypt(Nonce::from_slice(nonce), Payload { msg: ciphertext, aad: &Self::header(id) })
            .map_err(|_| corrupt())
    }

    fn code_hash(&self, normalized_code: &str) -> Result<String, AuthError> {
        Ok(self.keys[&self.current].code_hash(&self.current, normalized_code))
    }

    fn code_hashes(&self, normalized_code: &str) -> Result<Vec<String>, AuthError> {
        let mut hashes = vec![self.code_hash(normalized_code)?];
        hashes.extend(
            self.keys.iter().filter(|(id, _)| **id != self.current).map(|(id, key)| key.code_hash(id, normalized_code)),
        );
        Ok(hashes)
    }
}

/// No seed key configured: two-step sign-in is unavailable, fail-closed.
pub struct UnconfiguredSeedCipher;

impl MfaSeedCipher for UnconfiguredSeedCipher {
    fn seal(&self, _seed: &[u8]) -> Result<Vec<u8>, AuthError> {
        Err(AuthError::MfaUnavailable)
    }

    fn open(&self, _sealed: &[u8]) -> Result<Vec<u8>, AuthError> {
        Err(AuthError::MfaUnavailable)
    }

    fn code_hash(&self, _normalized_code: &str) -> Result<String, AuthError> {
        Err(AuthError::MfaUnavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> Vec<u8> {
        vec![byte; 32]
    }

    #[test]
    fn a_seed_opens_under_its_key_only_and_tampering_is_caught() {
        let cipher = AesSeedCipher::new("k1", &key(1), &[]).unwrap();
        let sealed = cipher.seal(b"0123456789abcdefghij").unwrap();
        assert_ne!(&sealed[sealed.len() - 20..], b"0123456789abcdefghij");
        assert_eq!(cipher.open(&sealed).unwrap(), b"0123456789abcdefghij");
        assert_ne!(cipher.seal(b"0123456789abcdefghij").unwrap(), sealed, "a fresh nonce each time");

        let mut tampered = sealed.clone();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(matches!(cipher.open(&tampered), Err(AuthError::DomainViolation { .. })));
        // The header is bound: relabelling the key id is caught too.
        let other = AesSeedCipher::new("k2", &key(1), &[]).unwrap();
        let mut relabelled = sealed.clone();
        relabelled[3] = b'2';
        assert!(other.open(&relabelled).is_err());
        assert!(matches!(AesSeedCipher::new("k9", &key(9), &[]).unwrap().open(&sealed), Err(AuthError::MfaUnavailable)));
    }

    #[test]
    fn after_a_rotation_old_seeds_open_and_old_codes_match() {
        let old = AesSeedCipher::new("k1", &key(1), &[]).unwrap();
        let sealed = old.seal(b"seed-seed-seed-seed-").unwrap();
        let old_hash = old.code_hash("abcdefghjk").unwrap();
        assert!(old_hash.starts_with("k1$") && old_hash.len() == 3 + 64);
        assert_eq!(old.code_hash("abcdefghjk").unwrap(), old_hash, "deterministic");
        assert_ne!(old.code_hash("abcdefghjm").unwrap(), old_hash);

        let rotated = AesSeedCipher::new("k2", &key(2), &[("k1".into(), key(1))]).unwrap();
        assert_eq!(rotated.open(&sealed).unwrap(), b"seed-seed-seed-seed-");
        assert!(rotated.seal(b"x").unwrap()[2..4] == *b"k2", "new seeds under the new key");
        let hashes = rotated.code_hashes("abcdefghjk").unwrap();
        assert!(hashes[0].starts_with("k2$") && hashes.contains(&old_hash));
    }

    #[test]
    fn bad_keys_and_ids_are_refused() {
        assert!(AesSeedCipher::new("k1", &[1; 16], &[]).is_err(), "16 bytes");
        assert!(AesSeedCipher::new("K 1", &key(1), &[]).is_err(), "bad id");
        assert!(matches!(UnconfiguredSeedCipher.seal(b"x"), Err(AuthError::MfaUnavailable)));
    }
}
