//! Checking a second factor (#649): a TOTP code from the holder's
//! authenticator app, or one of their one-time backup codes. Used at sign-in
//! (the second step) and to step up before a sensitive action.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::application::port::{AccountDirectory, MfaSeedCipher, MfaStore};
use crate::domain::value_object::{
    normalize_backup_code, totp_digits, AccountId, TotpSecret, TOTP_PERIOD_SECS, TOTP_SKEW_STEPS,
};
use crate::error::AuthError;

/// Wrong-code limits for one account.
#[derive(Debug, Clone, Copy)]
pub struct MfaPolicy {
    /// Wrong codes allowed within the window before codes are refused.
    pub max_failures: u32,
    pub failure_window_secs: u64,
    /// How long a sign-in waits for its second factor.
    pub challenge_ttl_secs: u64,
}

impl Default for MfaPolicy {
    fn default() -> Self {
        Self { max_failures: 5, failure_window_secs: 15 * 60, challenge_ttl_secs: 5 * 60 }
    }
}

/// How long a spent TOTP step stays spent: past the last moment its code is
/// accepted (the skew either side), with a margin.
const STEP_CLAIM_TTL_SECS: u64 = ((2 * TOTP_SKEW_STEPS + 2) * TOTP_PERIOD_SECS) as u64;

/// Checks a holder's second factor.
pub struct MfaVerifier {
    directory: Arc<dyn AccountDirectory>,
    cipher: Arc<dyn MfaSeedCipher>,
    store: Arc<dyn MfaStore>,
    policy: MfaPolicy,
}

impl MfaVerifier {
    pub fn new(
        directory: Arc<dyn AccountDirectory>,
        cipher: Arc<dyn MfaSeedCipher>,
        store: Arc<dyn MfaStore>,
        policy: MfaPolicy,
    ) -> Self {
        Self { directory, cipher, store, policy }
    }

    pub fn policy(&self) -> MfaPolicy {
        self.policy
    }

    pub fn store(&self) -> &Arc<dyn MfaStore> {
        &self.store
    }

    /// Proves `code` for `account`: six digits are a TOTP code (each step
    /// works once), anything shaped like a backup code is one (spent on
    /// success). Each check first reserves an attempt, atomically: past
    /// [`MfaPolicy::max_failures`] attempts in the window the code is not
    /// even looked at ([`AuthError::MfaLocked`]) until it ends — so parallel
    /// guesses get no more tries than sequential ones. A right code clears
    /// the count.
    pub async fn check(&self, account: &AccountId, code: &str, now: DateTime<Utc>) -> Result<(), AuthError> {
        let (attempts, retry_after_secs) =
            self.store.reserve_attempt(account, self.policy.failure_window_secs).await?;
        if attempts > self.policy.max_failures {
            return Err(AuthError::MfaLocked { retry_after_secs: retry_after_secs.max(1) });
        }
        if self.proves(account, code, now).await? {
            self.store.clear_failures(account).await?;
            return Ok(());
        }
        Err(AuthError::MfaCodeInvalid)
    }

    /// Proves the first TOTP code of an enrolment against its new `seed`,
    /// under the same attempt limit and step claim as any code.
    pub async fn check_new_seed(
        &self,
        account: &AccountId,
        seed: &TotpSecret,
        code: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AuthError> {
        let (attempts, retry_after_secs) =
            self.store.reserve_attempt(account, self.policy.failure_window_secs).await?;
        if attempts > self.policy.max_failures {
            return Err(AuthError::MfaLocked { retry_after_secs: retry_after_secs.max(1) });
        }
        let proven = match seed.matching_step(code, now) {
            Some(step) => self.store.claim_step(account, step, STEP_CLAIM_TTL_SECS).await?,
            None => false,
        };
        if !proven {
            return Err(AuthError::MfaCodeInvalid);
        }
        self.store.clear_failures(account).await
    }

    pub fn cipher(&self) -> &Arc<dyn MfaSeedCipher> {
        &self.cipher
    }

    async fn proves(&self, account: &AccountId, code: &str, now: DateTime<Utc>) -> Result<bool, AuthError> {
        if let Some(totp) = totp_digits(code) {
            let secret = self.directory.mfa_secret(account).await?;
            if !secret.enrolled {
                return Err(AuthError::MfaNotEnabled);
            }
            let seed = TotpSecret::from_bytes(self.cipher.open(&secret.sealed_seed)?)?;
            return Ok(match seed.matching_step(&totp, now) {
                // A replayed code (its step already used) proves nothing.
                Some(step) => self.store.claim_step(account, step, STEP_CLAIM_TTL_SECS).await?,
                None => false,
            });
        }
        let Some(backup) = normalize_backup_code(code) else { return Ok(false) };
        for hash in self.cipher.code_hashes(&backup)? {
            if self.directory.consume_recovery_code(account, &hash).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{FakeSeedCipher, InMemoryMfaStore, StubAccountDirectory};
    use crate::application::port::MfaSeedCipher;
    use crate::domain::value_object::step_of;
    use uuid::Uuid;

    struct Setup {
        verifier: MfaVerifier,
        directory: Arc<StubAccountDirectory>,
        account: AccountId,
        seed: TotpSecret,
    }

    fn setup() -> Setup {
        let directory = Arc::new(StubAccountDirectory::new());
        let cipher = FakeSeedCipher::default();
        let account = AccountId::from_uuid(Uuid::now_v7());
        let seed = TotpSecret::generate();
        directory.with_mfa(
            account,
            cipher.seal(seed.as_bytes()).unwrap(),
            vec![cipher.code_hash("abcdefghjk").unwrap(), cipher.code_hash("mnpqrstuvw").unwrap()],
        );
        let verifier = MfaVerifier::new(
            Arc::clone(&directory) as _,
            Arc::new(cipher),
            Arc::new(InMemoryMfaStore::default()),
            MfaPolicy::default(),
        );
        Setup { verifier, directory, account, seed }
    }

    #[tokio::test]
    async fn a_totp_code_proves_once_per_step() {
        let s = setup();
        let now = Utc::now();
        let code = s.seed.code_at(step_of(now));
        s.verifier.check(&s.account, &code, now).await.expect("right code");
        let replay = s.verifier.check(&s.account, &code, now).await.unwrap_err();
        assert!(matches!(replay, AuthError::MfaCodeInvalid), "a replayed code: {replay:?}");
        // The next step's code works.
        let later = now + chrono::Duration::seconds(TOTP_PERIOD_SECS);
        s.verifier.check(&s.account, &s.seed.code_at(step_of(later)), later).await.expect("next step");
    }

    #[tokio::test]
    async fn a_backup_code_works_once_whatever_its_case_or_dashes() {
        let s = setup();
        s.verifier.check(&s.account, "ABCDE-fghjk", Utc::now()).await.expect("backup code");
        assert_eq!(s.directory.recovery_codes_left(&s.account), 1);
        let again = s.verifier.check(&s.account, "abcdefghjk", Utc::now()).await.unwrap_err();
        assert!(matches!(again, AuthError::MfaCodeInvalid));
    }

    #[tokio::test]
    async fn too_many_wrong_codes_lock_even_the_right_one_out() {
        let s = setup();
        let now = Utc::now();
        for wrong in ["000000", "zzzzz-zzzzz", "not a code", "111111", "222222"] {
            let code = s.seed.code_at(step_of(now));
            if wrong == code {
                continue;
            }
            assert!(matches!(s.verifier.check(&s.account, wrong, now).await, Err(AuthError::MfaCodeInvalid)));
        }
        let locked = s.verifier.check(&s.account, &s.seed.code_at(step_of(now)), now).await.unwrap_err();
        assert!(matches!(locked, AuthError::MfaLocked { .. }), "{locked:?}");
    }

    /// Parallel guesses are counted before they are checked: of 20 wrong
    /// codes fired at once, at most `max_failures` are even evaluated.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_guesses_get_no_more_tries_than_sequential_ones() {
        let s = setup();
        let verifier = Arc::new(s.verifier);
        let now = Utc::now();
        let right = s.seed.code_at(step_of(now));
        let guesses: Vec<_> = (0..20u32)
            .map(|i| format!("{:06}", (i * 7919 + 13) % 1_000_000))
            .filter(|code| *code != right)
            .map(|code| {
                let (verifier, account) = (Arc::clone(&verifier), s.account);
                tokio::spawn(async move { verifier.check(&account, &code, now).await })
            })
            .collect();
        let (mut evaluated, mut locked) = (0, 0);
        for guess in guesses {
            match guess.await.unwrap() {
                Err(AuthError::MfaCodeInvalid) => evaluated += 1,
                Err(AuthError::MfaLocked { .. }) => locked += 1,
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(evaluated, MfaPolicy::default().max_failures, "only the window's attempts are evaluated");
        assert!(locked > 0);
        assert!(matches!(verifier.check(&s.account, &right, now).await, Err(AuthError::MfaLocked { .. })));
    }

    #[tokio::test]
    async fn without_mfa_or_without_the_key_nothing_is_proven() {
        let s = setup();
        let stranger = AccountId::from_uuid(Uuid::now_v7());
        let off = s.verifier.check(&stranger, "123456", Utc::now()).await.unwrap_err();
        assert!(matches!(off, AuthError::MfaNotEnabled), "{off:?}");

        let unkeyed = MfaVerifier::new(
            Arc::clone(&s.directory) as _,
            Arc::new(FakeSeedCipher::unkeyed()),
            Arc::new(InMemoryMfaStore::default()),
            MfaPolicy::default(),
        );
        let err = unkeyed.check(&s.account, &s.seed.code_at(step_of(Utc::now())), Utc::now()).await.unwrap_err();
        assert!(matches!(err, AuthError::MfaUnavailable), "fail-closed: {err:?}");
    }
}
