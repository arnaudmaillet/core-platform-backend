pub mod get_account_by_email;
pub mod get_account_by_id;
pub mod get_account_by_identity_id;
pub mod get_account_status;
pub mod get_gdpr_record;
pub mod list_accounts_by_status;

// ── View types ────────────────────────────────────────────────────────────────
pub use get_account_by_id::AccountView;
pub use get_account_status::AccountStatusView;
pub use get_gdpr_record::GdprRecordView;
pub use list_accounts_by_status::AccountListView;

// ── Query types ───────────────────────────────────────────────────────────────
pub use get_account_by_email::{GetAccountByEmailHandler, GetAccountByEmailQuery};
pub use get_account_by_id::{GetAccountByIdHandler, GetAccountByIdQuery};
pub use get_account_by_identity_id::{
    GetAccountByIdentityIdHandler, GetAccountByIdentityIdQuery,
};
pub use get_account_status::{GetAccountStatusHandler, GetAccountStatusQuery};
pub use get_gdpr_record::{GetGdprRecordHandler, GetGdprRecordQuery};
pub use list_accounts_by_status::{ListAccountsByStatusHandler, ListAccountsByStatusQuery};
