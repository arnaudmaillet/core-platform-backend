//! Profile slots (#916): how many profiles an account may hold, and the
//! gems that buy more.

use async_trait::async_trait;

use crate::domain::value_object::AccountId;
use crate::error::ProfileError;

/// The slots each account bought (or was grandfathered), per account. No row:
/// the account never needed more than its free one.
#[async_trait]
pub trait ProfileSlotStore: Send + Sync + 'static {
    async fn get(&self, account: &AccountId) -> Result<Option<i32>, ProfileError>;

    /// Records `slots` unless the account has a row already (LWT). Returns
    /// the account's slots now.
    async fn init(&self, account: &AccountId, slots: i32) -> Result<i32, ProfileError>;

    /// `from` → `to` slots if the account holds `from` (LWT). Returns the
    /// account's slots now, applied or not.
    async fn grow(&self, account: &AccountId, from: i32, to: i32) -> Result<i32, ProfileError>;

    /// The account was deleted.
    async fn erase(&self, account: &AccountId) -> Result<(), ProfileError>;
}

/// What gems buy from profile: the wallet's ledger kind and `ref_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GemPurchase {
    /// A profile slot (#916): `PROFILE_SLOT`, `ref_id` = the slot number.
    ProfileSlot { slot: i32 },
    /// A paid handle change (#911): `HANDLE_CHANGE`, `ref_id` = the profile.
    HandleChange { profile: crate::domain::value_object::ProfileId },
}

/// What the wallet answered to a gem spend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GemSpend {
    pub spent: bool,
    /// Gems left.
    pub gems:  i64,
}

/// The account's gems in the wallet (mesh `SpendGems`, once per `key`).
/// Failures are `WalletUnavailable`: a purchase fails closed.
#[async_trait]
pub trait GemWallet: Send + Sync + 'static {
    /// `end_user`: the buyer's edge token, forwarded so the wallet checks the
    /// spender itself (#852).
    async fn spend(
        &self,
        account: &AccountId,
        purchase: GemPurchase,
        amount: i64,
        key: &str,
        end_user: Option<&str>,
    ) -> Result<GemSpend, ProfileError>;

    /// Whether `account` owns the catalogue item `item_id` (#913; wallet
    /// `ListOwnedItems`, free items included).
    async fn owns(&self, account: &AccountId, item_id: &str) -> Result<bool, ProfileError>;
}
