//! Two-step sign-in material (#649): RFC 6238 TOTP — HMAC-SHA1, 6 digits,
//! 30-second steps, the authenticator-app standard — and one-time backup
//! codes. Pure: no I/O, the clock is passed in.

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rand::{Rng, RngCore};
use sha1::Sha1;

use crate::error::AuthError;

/// Digits of a TOTP code.
pub const TOTP_DIGITS: usize = 6;
/// Seconds per TOTP step.
pub const TOTP_PERIOD_SECS: i64 = 30;
/// Steps either side of now a code is still accepted on (clock drift).
pub const TOTP_SKEW_STEPS: i64 = 1;
/// Bytes of a fresh seed (160 bits, RFC 4226's recommendation).
pub const TOTP_SECRET_BYTES: usize = 20;
/// Backup codes handed out at enrolment (and on each regeneration).
pub const BACKUP_CODE_COUNT: usize = 10;
/// Characters of a backup code, without its separator.
pub const BACKUP_CODE_LEN: usize = 10;
/// Lowercase letters and digits, without look-alikes (0/o, 1/l/i): 31
/// symbols, so a code carries ~49.5 bits.
const BACKUP_ALPHABET: &[u8] = b"abcdefghjkmnpqrstuvwxyz23456789";

/// A TOTP seed. Its bytes never show in `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct TotpSecret(Vec<u8>);

impl std::fmt::Debug for TotpSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TotpSecret(<redacted>)")
    }
}

impl TotpSecret {
    /// A fresh random seed.
    pub fn generate() -> Self {
        let mut bytes = vec![0u8; TOTP_SECRET_BYTES];
        rand::rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// A stored seed (after it was opened). Shorter than 128 bits is refused.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, AuthError> {
        if bytes.len() < 16 {
            return Err(AuthError::DomainViolation {
                field: "totp_secret".into(),
                message: "a TOTP seed is at least 128 bits".into(),
            });
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// The seed in base32 (RFC 4648, no padding), as authenticator apps take
    /// it typed in.
    pub fn base32(&self) -> String {
        data_encoding::BASE32_NOPAD.encode(&self.0)
    }

    /// The `otpauth://` URI a QR code carries: `issuer` names the service,
    /// `account` the holder (e.g. their email) in the app's list.
    pub fn otpauth_uri(&self, issuer: &str, account: &str) -> String {
        let (issuer, account) = (percent_encode(issuer), percent_encode(account));
        format!(
            "otpauth://totp/{issuer}:{account}?secret={}&issuer={issuer}&algorithm=SHA1&digits={TOTP_DIGITS}&period={TOTP_PERIOD_SECS}",
            self.base32()
        )
    }

    /// The code for `step` (RFC 4226 HOTP over the step counter).
    pub fn code_at(&self, step: i64) -> String {
        let mut mac = Hmac::<Sha1>::new_from_slice(&self.0).expect("HMAC takes any key length");
        mac.update(&(step as u64).to_be_bytes());
        let digest = mac.finalize().into_bytes();
        let offset = (digest[19] & 0x0f) as usize;
        let binary = u32::from_be_bytes([digest[offset] & 0x7f, digest[offset + 1], digest[offset + 2], digest[offset + 3]]);
        format!("{:0width$}", binary % 10u32.pow(TOTP_DIGITS as u32), width = TOTP_DIGITS)
    }

    /// The step `code` is valid on at `now` (within [`TOTP_SKEW_STEPS`]), if
    /// any. Every candidate is compared in constant time.
    pub fn matching_step(&self, code: &str, now: DateTime<Utc>) -> Option<i64> {
        let code = totp_digits(code)?;
        let current = step_of(now);
        let mut found = None;
        for step in current - TOTP_SKEW_STEPS..=current + TOTP_SKEW_STEPS {
            if constant_time_eq(self.code_at(step).as_bytes(), code.as_bytes()) && found.is_none() {
                found = Some(step);
            }
        }
        found
    }
}

/// The TOTP step `now` falls in.
pub fn step_of(now: DateTime<Utc>) -> i64 {
    now.timestamp().div_euclid(TOTP_PERIOD_SECS)
}

/// `input` as a TOTP code (six digits, spaces allowed), if it is one.
pub fn totp_digits(input: &str) -> Option<String> {
    let digits: String = input.chars().filter(|c| !c.is_whitespace()).collect();
    (digits.len() == TOTP_DIGITS && digits.bytes().all(|b| b.is_ascii_digit())).then_some(digits)
}

/// [`BACKUP_CODE_COUNT`] fresh backup codes, shown as `xxxxx-xxxxx`.
pub fn generate_backup_codes() -> Vec<String> {
    let mut rng = rand::rng();
    (0..BACKUP_CODE_COUNT)
        .map(|_| {
            let raw: String = (0..BACKUP_CODE_LEN)
                .map(|_| BACKUP_ALPHABET[rng.random_range(0..BACKUP_ALPHABET.len())] as char)
                .collect();
            format!("{}-{}", &raw[..BACKUP_CODE_LEN / 2], &raw[BACKUP_CODE_LEN / 2..])
        })
        .collect()
}

/// `input` as a backup code in its canonical form (lowercase, no separator),
/// if it is one: what gets hashed, so `ABCDE-fghjk` and `abcdefghjk` match.
pub fn normalize_backup_code(input: &str) -> Option<String> {
    let code: String = input
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .map(|c| c.to_ascii_lowercase())
        .collect();
    (code.len() == BACKUP_CODE_LEN && code.bytes().all(|b| BACKUP_ALPHABET.contains(&b))).then_some(code)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// RFC 3986 percent-encoding of everything but the unreserved characters.
fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// RFC 6238 appendix B, SHA-1 seed "12345678901234567890": the 8-digit
    /// vectors' last six digits.
    #[test]
    fn rfc_6238_vectors() {
        let secret = TotpSecret::from_bytes(b"12345678901234567890".to_vec()).unwrap();
        for (t, expected) in [(59, "287082"), (1_111_111_109, "081804"), (1_234_567_890, "005924"), (2_000_000_000, "279037")] {
            let now = Utc.timestamp_opt(t, 0).unwrap();
            assert_eq!(secret.code_at(step_of(now)), expected, "T = {t}");
            assert_eq!(secret.matching_step(expected, now), Some(step_of(now)));
        }
    }

    #[test]
    fn a_code_holds_one_step_either_side_and_no_further() {
        let secret = TotpSecret::generate();
        let now = Utc.timestamp_opt(1_800_000_000, 0).unwrap();
        let step = step_of(now);
        for drift in [-1, 0, 1] {
            assert_eq!(secret.matching_step(&secret.code_at(step + drift), now), Some(step + drift));
        }
        for drift in [-2, 2] {
            let code = secret.code_at(step + drift);
            // A far step's code only matches by coincidence with a near one.
            if (-1..=1).all(|d| secret.code_at(step + d) != code) {
                assert_eq!(secret.matching_step(&code, now), None);
            }
        }
        let spaced = format!("{} {}", &secret.code_at(step)[..3], &secret.code_at(step)[3..]);
        assert_eq!(secret.matching_step(&spaced, now), Some(step), "spaces are fine");
        assert_eq!(secret.matching_step("12345", now), None);
        assert_eq!(secret.matching_step("abcdef", now), None);
    }

    #[test]
    fn the_uri_names_issuer_and_account_and_carries_the_base32_seed() {
        let secret = TotpSecret::from_bytes(b"12345678901234567890".to_vec()).unwrap();
        // Base32 without padding, upper case, decoding back to the seed (no
        // literal of the encoded seed here: secret scanners flag it).
        let encoded = secret.base32();
        assert!(!encoded.contains('=') && encoded.bytes().all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b)));
        assert_eq!(data_encoding::BASE32_NOPAD.decode(encoded.as_bytes()).unwrap(), b"12345678901234567890");
        let uri = secret.otpauth_uri("Core Platform", "me@example.com");
        assert_eq!(
            uri,
            format!(
                "otpauth://totp/Core%20Platform:me%40example.com?secret={encoded}\
                 &issuer=Core%20Platform&algorithm=SHA1&digits=6&period=30"
            )
        );
        assert_eq!(format!("{secret:?}"), "TotpSecret(<redacted>)");
        assert!(TotpSecret::from_bytes(vec![1; 10]).is_err(), "too short");
    }

    #[test]
    fn backup_codes_are_distinct_readable_and_normalize() {
        let codes = generate_backup_codes();
        assert_eq!(codes.len(), BACKUP_CODE_COUNT);
        let distinct: std::collections::HashSet<_> = codes.iter().collect();
        assert_eq!(distinct.len(), BACKUP_CODE_COUNT);
        for code in &codes {
            assert_eq!(code.len(), BACKUP_CODE_LEN + 1);
            let normal = normalize_backup_code(code).expect("its own format");
            assert_eq!(normalize_backup_code(&code.to_uppercase()), Some(normal.clone()));
            assert_eq!(normalize_backup_code(&normal), Some(normal));
            assert!(totp_digits(code).is_none(), "never mistaken for a TOTP code");
        }
        assert_eq!(normalize_backup_code("abcde-fgh0k"), None, "0 is not in the alphabet");
        assert_eq!(normalize_backup_code("abcde"), None);
    }
}
