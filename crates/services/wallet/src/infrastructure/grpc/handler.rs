//! `wallet.v1` over [`Wallets`]. Every RPC is the caller's own wallet on the
//! edge (`edge::require_account`).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use error::AppError;
use tonic::{Request, Response, Status};
use transport::grpc::edge;

use crate::application::port::ClaimOutcome;
use crate::application::{Wallets, WalletView};
use crate::domain::{Currency, Transaction, TransactionKind};
use crate::error::WalletError;

pub use wallet_api as proto;

/// The stable error code (e.g. `WAL-9002`) on a failed call.
pub const ERROR_CODE_METADATA: &str = "x-error-code";

pub struct WalletServiceHandler {
    wallets: Arc<Wallets>,
}

impl WalletServiceHandler {
    pub fn new(wallets: Arc<Wallets>) -> Self {
        Self { wallets }
    }

    pub async fn get_wallet(&self, request: Request<proto::GetWalletRequest>) -> Result<Response<proto::Wallet>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let view = self.wallets.get(&request.get_ref().account_id, Utc::now()).await.map_err(to_status)?;
        Ok(Response::new(wallet_to_proto(&view)))
    }

    pub async fn claim_reward(
        &self,
        request: Request<proto::ClaimRewardRequest>,
    ) -> Result<Response<proto::ClaimRewardResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let reply = self.wallets.claim(&req.account_id, &req.idempotency_key, Utc::now()).await.map_err(to_status)?;
        Ok(Response::new(proto::ClaimRewardResponse {
            outcome: match reply.outcome {
                ClaimOutcome::Claimed => proto::ClaimOutcome::Claimed,
                ClaimOutcome::TooEarly => proto::ClaimOutcome::TooEarly,
                ClaimOutcome::DailyCapReached => proto::ClaimOutcome::DailyCapReached,
            } as i32,
            awarded: reply.awarded,
            wallet:  Some(wallet_to_proto(&reply.view)),
        }))
    }

    pub async fn list_wallet_transactions(
        &self,
        request: Request<proto::ListWalletTransactionsRequest>,
    ) -> Result<Response<proto::ListWalletTransactionsResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let currency = match proto::Currency::try_from(req.currency) {
            Ok(proto::Currency::Unspecified) => None,
            Ok(proto::Currency::Points) => Some(Currency::Points),
            Ok(proto::Currency::Gems) => Some(Currency::Gems),
            Err(_) => return Err(Status::invalid_argument("unknown currency")),
        };
        let page = self
            .wallets
            .history(&req.account_id, currency, usize::try_from(req.page_size).unwrap_or(0), &req.page_token)
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::ListWalletTransactionsResponse {
            transactions:    page.transactions.iter().map(transaction_to_proto).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
        }))
    }
}

fn wallet_to_proto(view: &WalletView) -> proto::Wallet {
    let (w, c) = (&view.wallet, &view.claim);
    proto::Wallet {
        account_id:      w.account.to_string(),
        points:          w.points,
        gems:            w.gems,
        points_earned:   w.points_earned,
        points_spent:    w.points_spent,
        gems_earned:     w.gems_earned,
        gems_spent:      w.gems_spent,
        claim_available: c.available,
        next_claim_at:   c.next_claim_at.map(timestamp),
        claim_amount:    c.claim_amount,
        claimed_today:   c.claimed_today,
        daily_claim_cap: c.daily_cap,
        streak_days:     c.streak_days,
        last_claim_at:   w.last_claim_at.map(timestamp),
        stake_shots:     w.stake_shots,
    }
}

fn transaction_to_proto(t: &Transaction) -> proto::WalletTransaction {
    proto::WalletTransaction {
        transaction_id: t.id.to_string(),
        currency:       match t.currency {
            Currency::Points => proto::Currency::Points,
            Currency::Gems => proto::Currency::Gems,
        } as i32,
        delta:          t.delta,
        balance_after:  t.balance_after,
        kind:           match t.kind {
            TransactionKind::Claim => proto::TransactionKind::Claim,
            TransactionKind::StarterGift => proto::TransactionKind::StarterGift,
            TransactionKind::Unknown => proto::TransactionKind::Unspecified,
        } as i32,
        created_at:     Some(timestamp(t.created_at)),
    }
}

fn timestamp(at: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds: at.timestamp(), nanos: i32::try_from(at.timestamp_subsec_nanos()).unwrap_or(0) }
}

/// The error as a gRPC status, with its stable code in `x-error-code`.
fn to_status(err: WalletError) -> Status {
    let message = err.to_string();
    let mut status = match err.http_status().as_u16() {
        400 | 422 => Status::invalid_argument(message),
        404 => Status::not_found(message),
        409 if err.is_retryable() => Status::aborted(message),
        409 => Status::failed_precondition(message),
        429 => Status::resource_exhausted(message),
        502..=504 => Status::unavailable(message),
        _ => Status::internal(message),
    };
    if let Ok(code) = err.error_code().parse() {
        status.metadata_mut().insert(ERROR_CODE_METADATA, code);
    }
    status
}

#[cfg(test)]
mod tests {
    use tonic::Code;
    use uuid::Uuid;

    use super::*;
    use crate::application::wallets::fakes::MemoryStore;
    use crate::config::WalletConfig;

    fn principal(sub: &str) -> edge::EdgePrincipal {
        let raw: auth_context::OidcClaims =
            serde_json::from_value(serde_json::json!({ "sub": sub, "exp": 4_102_444_800_i64 })).unwrap();
        edge::EdgePrincipal::new(Arc::new(auth_context::CurrentPrincipal {
            user_id: auth_context::PrincipalId::new(sub),
            tenant_id: None,
            permissions: vec![auth_context::Permission::new("read:public")],
            raw_claims: raw,
        }))
    }

    fn handler() -> WalletServiceHandler {
        WalletServiceHandler::new(Arc::new(Wallets::new(Arc::new(MemoryStore::default()), WalletConfig::default())))
    }

    fn as_caller<T>(sub: &str, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.extensions_mut().insert(principal(sub));
        request
    }

    #[tokio::test]
    async fn the_caller_reads_and_claims_its_own_wallet_only() {
        let h = handler();
        let me = Uuid::now_v7().to_string();
        let wallet = h.get_wallet(as_caller(&me, proto::GetWalletRequest { account_id: me.clone() })).await.unwrap().into_inner();
        assert_eq!((wallet.points, wallet.gems, wallet.claim_available, wallet.claim_amount), (0, 100, true, 25));

        let claim = h
            .claim_reward(as_caller(&me, proto::ClaimRewardRequest {
                account_id:      me.clone(),
                idempotency_key: Uuid::now_v7().to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!((claim.outcome, claim.awarded), (proto::ClaimOutcome::Claimed as i32, 25));
        assert!(!claim.wallet.unwrap().claim_available);

        let history = h
            .list_wallet_transactions(as_caller(&me, proto::ListWalletTransactionsRequest {
                account_id: me.clone(),
                currency:   proto::Currency::Points as i32,
                page_size:  0,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(history.transactions.len(), 1);
        assert_eq!(history.transactions[0].kind, proto::TransactionKind::Claim as i32);

        // Someone else's wallet: refused on the edge.
        let other = Uuid::now_v7().to_string();
        let denied = h.get_wallet(as_caller(&me, proto::GetWalletRequest { account_id: other })).await.unwrap_err();
        assert_eq!(denied.code(), Code::PermissionDenied);
    }

    #[tokio::test]
    async fn a_bad_key_is_invalid_argument_with_its_code() {
        let h = handler();
        let me = Uuid::now_v7().to_string();
        let status = h
            .claim_reward(as_caller(&me, proto::ClaimRewardRequest { account_id: me.clone(), idempotency_key: "x".into() }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(status.metadata().get(ERROR_CODE_METADATA).unwrap(), "WAL-9002");
    }
}
