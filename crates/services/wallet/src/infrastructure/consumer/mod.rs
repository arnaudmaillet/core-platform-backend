//! Inbound Kafka consumers.

pub mod account_consumer;
pub mod auth_consumer;

pub use account_consumer::run_account_consumer;
pub use auth_consumer::run_auth_consumer;

/// A consumer retries what the error says is transient; the rest is
/// dead-lettered.
impl transport::kafka::consumer::ClassifyError for crate::error::WalletError {
    fn is_retryable(&self) -> bool {
        <Self as error::AppError>::is_retryable(self)
    }
}
