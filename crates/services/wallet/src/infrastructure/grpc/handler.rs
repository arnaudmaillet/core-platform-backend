//! `wallet.v1` over [`Wallets`]. Every edge RPC is the caller's own wallet
//! (`edge::require_account`); `SpendGems` is mesh only.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use error::AppError;
use tonic::{Request, Response, Status};
use transport::grpc::edge;
use transport::grpc::mesh::{MeshCallerGate, MeshGateMode};

use crate::application::port::{
    ClaimOutcome, PackOutcome, PurchaseOutcome, SettlementRecord, SpendOutcome, StakeOutcome, StakePositionRecord, StakeStatus,
};
use crate::application::wallets::StakeBatch;
use crate::application::{MyStake, OwnedItemView, StakeVerdict, Wallets, WalletView};
use crate::domain::{Conviction, StakeAsk, StakeTarget};
use crate::config::WalletConfig;
use crate::domain::{Currency, ItemFamily, Transaction, TransactionKind};
use crate::error::WalletError;

pub use wallet_api as proto;

/// The stable error code (e.g. `WAL-9002`) on a failed call.
pub const ERROR_CODE_METADATA: &str = "x-error-code";

pub struct WalletServiceHandler {
    wallets: Arc<Wallets>,
    /// Which services may call its mesh-only RPCs (#852); off by default.
    mesh:    MeshCallerGate,
    /// Verifies the end user's edge token a caller forwards with
    /// `SpendGems` (#852): the wallet checks the spender itself.
    users:   edge::StaffGate,
}

/// `SpendGems`'s intended callers: geo-discovery (country unlocks and visas)
/// and profile (profile slots, #916; paid handle changes, #911).
const SPEND_GEMS_CALLERS: &[&str] = &["geo-discovery-server", "profile-server"];

/// `AwardBadge`'s callers (#912): the services whose rules earn badges.
const AWARD_CALLERS: &[&str] = &["geo-discovery-server"];

/// The wallet's export reads' only intended caller: account (the GDPR export).
const EXPORT_CALLERS: &[&str] = &["account-server"];

impl WalletServiceHandler {
    /// Checks its mesh callers with `mesh`, and `SpendGems`'s end user with
    /// `users` (#852).
    pub fn with_mesh_gate(mut self, mesh: MeshCallerGate, users: edge::StaffGate) -> Self {
        self.mesh = mesh;
        self.users = users;
        self
    }

    /// Off unless the mesh gate is enforced (`log`: logged, let through):
    /// the forwarded end-user token must name `account_id`'s owner, an
    /// adult.
    async fn check_spender<T>(&self, request: &Request<T>, account_id: &str) -> Result<(), Status> {
        let mode = self.mesh.mode();
        if mode == MeshGateMode::Off {
            return Ok(());
        }
        let refused = match self.users.verified(request).await {
            Ok(user) if user.account_id() != account_id => Some("the end user is not the spender".to_owned()),
            Ok(user) if !user.is_adult() => Some("the end user may not spend gems".to_owned()),
            Ok(_) => None,
            Err(status) => Some(format!("no valid end-user token: {}", status.message())),
        };
        match refused {
            None => Ok(()),
            Some(reason) if mode == MeshGateMode::Log => {
                tracing::warn!(%reason, "SpendGems end user refused (log mode: let through)");
                Ok(())
            }
            Some(reason) => {
                tracing::warn!(%reason, "SpendGems end user refused");
                Err(Status::permission_denied("the gems' owner must ask, and be an adult"))
            }
        }
    }

    pub fn new(wallets: Arc<Wallets>) -> Self {
        Self { wallets, mesh: MeshCallerGate::off(), users: edge::StaffGate::deny_all() }
    }

    pub async fn get_wallet(&self, request: Request<proto::GetWalletRequest>) -> Result<Response<proto::Wallet>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let restricted = spending_restricted(&request);
        let view = self.wallets.get(&request.get_ref().account_id, Utc::now()).await.map_err(to_status)?;
        Ok(Response::new(self.wallet_to_proto(&view, restricted)))
    }

    /// Buys a stake pack (#941). On the edge, a caller under 18 (or of
    /// unknown age, from the token) is refused: `WAL-3001`.
    pub async fn buy_stake_pack(
        &self,
        request: Request<proto::BuyStakePackRequest>,
    ) -> Result<Response<proto::BuyStakePackResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let restricted = spending_restricted(&request);
        let req = request.into_inner();
        let reply = self
            .wallets
            .buy_stake_pack(&req.account_id, &req.idempotency_key, &req.pack_id, restricted, Utc::now())
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::BuyStakePackResponse {
            outcome: match reply.outcome {
                PackOutcome::Bought => proto::StakePackOutcome::Bought,
                PackOutcome::StillActive => proto::StakePackOutcome::PackStillActive,
                PackOutcome::InsufficientGems => proto::StakePackOutcome::InsufficientGems,
            } as i32,
            wallet:  Some(self.wallet_to_proto(&reply.view, restricted)),
        }))
    }

    /// Commits a batch of likes (#665). Edge: the caller's account and one of
    /// its profiles; its own content (any of its profiles) cannot be liked.
    pub async fn stake(&self, request: Request<proto::StakeRequest>) -> Result<Response<proto::StakeResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let restricted = spending_restricted(&request);
        let own: Vec<String> = edge::principal(&request).map(|p| p.profile_ids().map(str::to_owned).collect()).unwrap_or_default();
        let req = request.into_inner();
        let target = match req.target {
            Some(proto::stake_request::Target::PostId(id)) => StakeTarget::Post(id),
            Some(proto::stake_request::Target::CommentId(id)) => StakeTarget::Comment(id),
            None => return Err(Status::invalid_argument("a stake names a post or a comment")),
        };
        let ask = if req.use_stake_shot { StakeAsk::Shot } else { StakeAsk::Points(i64::from(req.points)) };
        let first_tap_at = req
            .first_tap_at
            .and_then(|t| DateTime::from_timestamp(t.seconds, u32::try_from(t.nanos).unwrap_or(0)))
            .ok_or_else(|| Status::invalid_argument("first_tap_at is required"))?;
        let conviction = match proto::Conviction::try_from(req.conviction).unwrap_or_default() {
            proto::Conviction::Unspecified => None,
            proto::Conviction::C1 => Some(Conviction::C1),
            proto::Conviction::C2 => Some(Conviction::C2),
            proto::Conviction::C3 => Some(Conviction::C3),
            proto::Conviction::C4 => Some(Conviction::C4),
        };
        let batch = StakeBatch { profile_id: req.profile_id, target, ask, key: req.idempotency_key, first_tap_at, conviction };
        let reply = self.wallets.stake(&req.account_id, batch, &own, Utc::now()).await.map_err(to_status)?;
        Ok(Response::new(proto::StakeResponse {
            outcome:  match reply.outcome {
                StakeOutcome::Staked => proto::StakeOutcome::Staked,
                StakeOutcome::InsufficientBalance => proto::StakeOutcome::InsufficientBalance,
                StakeOutcome::TargetNotStakeable => proto::StakeOutcome::TargetNotStakeable,
                StakeOutcome::RateLimited => proto::StakeOutcome::RateLimited,
                StakeOutcome::TargetCapReached => proto::StakeOutcome::TargetCapReached,
                StakeOutcome::NoStakeShots => proto::StakeOutcome::NoStakeShots,
                StakeOutcome::ShotDoesNotFit => proto::StakeOutcome::ShotDoesNotFit,
                StakeOutcome::Expired => proto::StakeOutcome::Expired,
                StakeOutcome::OwnContent => proto::StakeOutcome::OwnContent,
            } as i32,
            spent:    i32::try_from(reply.spent).unwrap_or(i32::MAX),
            my_total: i32::try_from(reply.my_total).unwrap_or(i32::MAX),
            wallet:   Some(self.wallet_to_proto(&reply.view, restricted)),
        }))
    }

    /// Mesh only: another service spends gems (it checked the spender may).
    pub async fn spend_gems(
        &self,
        request: Request<proto::SpendGemsRequest>,
    ) -> Result<Response<proto::SpendGemsResponse>, Status> {
        self.mesh.require(&request, "SpendGems", SPEND_GEMS_CALLERS).await?;
        self.check_spender(&request, &request.get_ref().account_id).await?;
        let req = request.into_inner();
        let kind = match proto::TransactionKind::try_from(req.kind) {
            Ok(proto::TransactionKind::CountryUnlock) => TransactionKind::CountryUnlock,
            Ok(proto::TransactionKind::CountryVisa) => TransactionKind::CountryVisa,
            Ok(proto::TransactionKind::ProfileSlot) => TransactionKind::ProfileSlot,
            Ok(proto::TransactionKind::HandleChange) => TransactionKind::HandleChange,
            _ => {
                return Err(Status::invalid_argument("kind must be COUNTRY_UNLOCK, COUNTRY_VISA, PROFILE_SLOT or HANDLE_CHANGE"))
            }
        };
        let (outcome, gems) = self
            .wallets
            .spend_gems(&req.account_id, &req.idempotency_key, req.amount, kind, &req.ref_id, Utc::now())
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::SpendGemsResponse {
            outcome: match outcome {
                SpendOutcome::Spent => proto::SpendGemsOutcome::Spent,
                SpendOutcome::InsufficientGems => proto::SpendGemsOutcome::InsufficientGems,
            } as i32,
            gems,
        }))
    }

    /// Mesh only (#653, #665): the account's wallet for its GDPR export,
    /// never opened by it.
    pub async fn export_wallet(
        &self,
        request: Request<proto::ExportWalletRequest>,
    ) -> Result<Response<proto::ExportWalletResponse>, Status> {
        self.mesh.require(&request, "ExportWallet", EXPORT_CALLERS).await?;
        let req = request.into_inner();
        let view = self.wallets.export(&req.account_id, Utc::now()).await.map_err(to_status)?;
        let owned_items = self.wallets.bought_items(&req.account_id).await.map_err(to_status)?;
        Ok(Response::new(proto::ExportWalletResponse {
            wallet:      view.map(|v| self.wallet_to_proto(&v, false)),
            owned_items: owned_items.into_iter().map(owned_to_proto).collect(),
        }))
    }

    /// Mesh only (#912): awards a badge, once.
    pub async fn award_badge(
        &self,
        request: Request<proto::AwardBadgeRequest>,
    ) -> Result<Response<proto::AwardBadgeResponse>, Status> {
        self.mesh.require(&request, "AwardBadge", AWARD_CALLERS).await?;
        let req = request.into_inner();
        let (newly_awarded, awarded_at) = self
            .wallets
            .award_badge(&req.account_id, &req.badge_id, &req.profile_ids, Utc::now())
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::AwardBadgeResponse { newly_awarded, awarded_at: Some(timestamp(awarded_at)) }))
    }

    /// The gem catalogue (#892). Edge: any member.
    pub async fn list_catalog(
        &self,
        _request: Request<proto::ListCatalogRequest>,
    ) -> Result<Response<proto::ListCatalogResponse>, Status> {
        let items = self
            .wallets
            .catalog()
            .items
            .iter()
            .map(|item| proto::CatalogItem {
                item_id: item.id.clone(),
                family:  family_to_proto(item.family) as i32,
                price:   item.price_gems,
            })
            .collect();
        Ok(Response::new(proto::ListCatalogResponse { items }))
    }

    /// Buys a catalogue item (#892). On the edge, a caller under 18 (or of
    /// unknown age, from the token) is refused: `WAL-3001`.
    pub async fn purchase_item(
        &self,
        request: Request<proto::PurchaseItemRequest>,
    ) -> Result<Response<proto::PurchaseItemResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let restricted = spending_restricted(&request);
        let req = request.into_inner();
        let reply = self
            .wallets
            .purchase_item(&req.account_id, &req.idempotency_key, &req.item_id, req.expected_price, restricted, Utc::now())
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::PurchaseItemResponse {
            outcome: match reply.outcome {
                PurchaseOutcome::Purchased => proto::PurchaseOutcome::Purchased,
                PurchaseOutcome::AlreadyOwned => proto::PurchaseOutcome::AlreadyOwned,
                PurchaseOutcome::InsufficientGems => proto::PurchaseOutcome::InsufficientGems,
                PurchaseOutcome::PriceChanged => proto::PurchaseOutcome::PriceChanged,
                PurchaseOutcome::UnknownItem => proto::PurchaseOutcome::UnknownItem,
            } as i32,
            wallet:  Some(self.wallet_to_proto(&reply.view, restricted)),
        }))
    }

    /// The items the caller's account owns (#892). Edge: the caller's account.
    pub async fn list_owned_items(
        &self,
        request: Request<proto::ListOwnedItemsRequest>,
    ) -> Result<Response<proto::ListOwnedItemsResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let items = self.wallets.owned_items(&request.get_ref().account_id).await.map_err(to_status)?;
        Ok(Response::new(proto::ListOwnedItemsResponse { items: items.into_iter().map(owned_to_proto).collect() }))
    }

    /// Mesh only (#653, #665): the account's stake positions and their
    /// settlements, for its GDPR export.
    pub async fn list_stake_positions(
        &self,
        request: Request<proto::ListStakePositionsRequest>,
    ) -> Result<Response<proto::ListStakePositionsResponse>, Status> {
        self.mesh.require(&request, "ListStakePositions", EXPORT_CALLERS).await?;
        let req = request.into_inner();
        let page = self
            .wallets
            .stake_positions(&req.account_id, i64::from(req.page_size), &req.page_token)
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::ListStakePositionsResponse {
            positions:       page.positions.iter().map(position_to_proto).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
        }))
    }

    /// The caller's own stakes (#886). Edge: the caller's account.
    pub async fn list_my_stakes(
        &self,
        request: Request<proto::ListMyStakesRequest>,
    ) -> Result<Response<proto::ListMyStakesResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let status = match proto::StakeStatusFilter::try_from(req.status).unwrap_or_default() {
            proto::StakeStatusFilter::Unspecified => StakeStatus::All,
            proto::StakeStatusFilter::Active => StakeStatus::Active,
            proto::StakeStatusFilter::Settled => StakeStatus::Settled,
        };
        let page = self
            .wallets
            .my_stakes(&req.account_id, status, i64::from(req.page_size), &req.page_token)
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::ListMyStakesResponse {
            stakes:          page.stakes.iter().map(my_stake_to_proto).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
        }))
    }

    pub async fn claim_reward(
        &self,
        request: Request<proto::ClaimRewardRequest>,
    ) -> Result<Response<proto::ClaimRewardResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let restricted = spending_restricted(&request);
        let req = request.into_inner();
        let reply = self.wallets.claim(&req.account_id, &req.idempotency_key, Utc::now()).await.map_err(to_status)?;
        Ok(Response::new(proto::ClaimRewardResponse {
            outcome: match reply.outcome {
                ClaimOutcome::Claimed => proto::ClaimOutcome::Claimed,
                ClaimOutcome::TooEarly => proto::ClaimOutcome::TooEarly,
                ClaimOutcome::DailyCapReached => proto::ClaimOutcome::DailyCapReached,
                ClaimOutcome::BalanceFull => proto::ClaimOutcome::BalanceFull,
            } as i32,
            awarded: reply.awarded,
            wallet:  Some(self.wallet_to_proto(&reply.view, restricted)),
        }))
    }

    pub async fn list_wallet_transactions(
        &self,
        request: Request<proto::ListWalletTransactionsRequest>,
    ) -> Result<Response<proto::ListWalletTransactionsResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        // Over the mesh, only account reads another's history (the export).
        if matches!(edge::viewer(&request), edge::Viewer::Internal) {
            self.mesh.require(&request, "ListWalletTransactions", EXPORT_CALLERS).await?;
        }
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

impl WalletServiceHandler {
    fn wallet_to_proto(&self, view: &WalletView, restricted: bool) -> proto::Wallet {
        wallet_to_proto(view, self.wallets.config(), restricted)
    }
}

/// A caller under 18 or of unknown age may not spend gems (from the edge
/// token); a mesh caller is not restricted here.
fn spending_restricted<T>(request: &Request<T>) -> bool {
    edge::principal(request).is_some_and(|p| !p.is_adult())
}

fn family_to_proto(family: ItemFamily) -> proto::ItemFamily {
    match family {
        ItemFamily::AppIcon => proto::ItemFamily::AppIcon,
        ItemFamily::Theme => proto::ItemFamily::Theme,
        ItemFamily::AvatarFrame => proto::ItemFamily::AvatarFrame,
        ItemFamily::AnimatedBanner => proto::ItemFamily::AnimatedBanner,
        ItemFamily::CardStyle => proto::ItemFamily::CardStyle,
        ItemFamily::Badge => proto::ItemFamily::Badge,
        ItemFamily::AnimatedAvatar => proto::ItemFamily::AnimatedAvatar,
    }
}

fn owned_to_proto(item: OwnedItemView) -> proto::OwnedItem {
    proto::OwnedItem {
        item_id:     item.item_id,
        family:      family_to_proto(item.family) as i32,
        acquired_at: item.acquired_at.map(timestamp),
    }
}

fn wallet_to_proto(view: &WalletView, config: &WalletConfig, restricted: bool) -> proto::Wallet {
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
        points_cap:      c.points_cap,
        streak_days:     c.streak_days,
        last_claim_at:   w.last_claim_at.map(timestamp),
        stake_shots:     w.stake_shots,
        gem_spending_restricted: restricted,
        stake_pack_price: config.stake_pack.default_offer().price_gems,
        stake_pack_shots: config.stake_pack.default_offer().shots,
        points_per_shot: config.stake_pack.points_per_shot,
        stake_packs:     config
            .stake_pack
            .offers
            .iter()
            .map(|offer| proto::StakePackOffer { pack_id: offer.id(), shots: offer.shots, price: offer.price_gems })
            .collect(),
        stake_pack_id:   w.active_stake_pack(&config.stake_pack).unwrap_or_default(),
        conviction_probabilities: config.conviction_bps.iter().map(|bps| *bps as f64 / 10_000.0).collect(),
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
            TransactionKind::StakePack => proto::TransactionKind::StakePack,
            TransactionKind::CountryUnlock => proto::TransactionKind::CountryUnlock,
            TransactionKind::Stake => proto::TransactionKind::Stake,
            TransactionKind::WelcomeGift => proto::TransactionKind::WelcomeGift,
            TransactionKind::AppIcon => proto::TransactionKind::AppIcon,
            TransactionKind::Theme => proto::TransactionKind::Theme,
            TransactionKind::CountryVisa => proto::TransactionKind::CountryVisa,
            TransactionKind::ProfileSlot => proto::TransactionKind::ProfileSlot,
            TransactionKind::HandleChange => proto::TransactionKind::HandleChange,
            TransactionKind::AvatarFrame => proto::TransactionKind::AvatarFrame,
            TransactionKind::AnimatedBanner => proto::TransactionKind::AnimatedBanner,
            TransactionKind::CardStyle => proto::TransactionKind::CardStyle,
            TransactionKind::AnimatedAvatar => proto::TransactionKind::AnimatedAvatar,
            TransactionKind::Unknown => proto::TransactionKind::Unspecified,
        } as i32,
        created_at:     Some(timestamp(t.created_at)),
        ref_id:         t.ref_id.clone().unwrap_or_default(),
    }
}

fn my_stake_to_proto(s: &MyStake) -> proto::MyStake {
    use proto::my_stake::Target;
    let r = &s.record;
    proto::MyStake {
        target:     Some(match &r.target {
            StakeTarget::Post(id) => Target::PostId(id.clone()),
            StakeTarget::Comment(id) => Target::CommentId(id.clone()),
        }),
        points:     r.points,
        first_at:   Some(timestamp(r.first_at)),
        last_at:    Some(timestamp(r.last_at)),
        settles_at: Some(timestamp(s.settles_at)),
        settled_at: r.settled_at.map(timestamp),
        verdict:    match s.verdict {
            StakeVerdict::Unsettled => proto::StakeVerdict::Unspecified,
            StakeVerdict::Pending => proto::StakeVerdict::Pending,
            StakeVerdict::Rewarded => proto::StakeVerdict::Rewarded,
            StakeVerdict::NotRewarded => proto::StakeVerdict::NotRewarded,
        } as i32,
    }
}

fn position_to_proto(p: &StakePositionRecord) -> proto::StakePosition {
    use proto::stake_position::Target;
    proto::StakePosition {
        target:     Some(match &p.target {
            StakeTarget::Post(id) => Target::PostId(id.clone()),
            StakeTarget::Comment(id) => Target::CommentId(id.clone()),
        }),
        points:     p.points,
        first_at:   Some(timestamp(p.first_at)),
        last_at:    Some(timestamp(p.last_at)),
        settlement: p.settlement.as_ref().map(settlement_to_proto),
        first_conviction: p.first_conviction.map_or(proto::Conviction::Unspecified, |c| match c {
            Conviction::C1 => proto::Conviction::C1,
            Conviction::C2 => proto::Conviction::C2,
            Conviction::C3 => proto::Conviction::C3,
            Conviction::C4 => proto::Conviction::C4,
        }) as i32,
    }
}

fn settlement_to_proto(s: &SettlementRecord) -> proto::StakeSettlement {
    proto::StakeSettlement {
        settled_at:          Some(timestamp(s.settled_at)),
        points:              s.points,
        count_on_arrival:    s.count_on_arrival,
        count_at_settlement: s.count_at_settlement,
        earliness:           s.earliness,
        pre_score:           s.pre_score,
        model:               s.model.clone(),
        outcome:             s.outcome.map(i32::from),
        score:               s.score,
        provisional_gems:    s.provisional_gems,
        envelope_day:        s.envelope_day.map(|d| d.to_string()).unwrap_or_default(),
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
        403 => Status::permission_denied(message),
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

    /// An adult caller (the token's `age` claim).
    fn principal(sub: &str) -> edge::EdgePrincipal {
        principal_aged(sub, Some("18+"))
    }

    fn principal_aged(sub: &str, age: Option<&str>) -> edge::EdgePrincipal {
        let mut claims = serde_json::json!({ "sub": sub, "exp": 4_102_444_800_i64, "pids": ["my-profile"] });
        if let Some(age) = age {
            claims["age"] = serde_json::json!(age);
        }
        let raw: auth_context::OidcClaims = serde_json::from_value(claims).unwrap();
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

    /// #892, #946: the catalogue lists the themes; a member buys one for its
    /// own account only, a minor not at all; owned items start with the free
    /// pink, and the export carries what was bought.
    #[tokio::test]
    async fn the_catalogue_sells_themes_to_the_callers_own_account() {
        let h = handler();
        let me = Uuid::now_v7().to_string();
        let catalog = h.list_catalog(as_caller(&me, proto::ListCatalogRequest {})).await.unwrap().into_inner();
        let items: Vec<_> = catalog.items.iter().map(|i| (i.item_id.as_str(), i.family, i.price)).collect();
        let (theme, frame) = (proto::ItemFamily::Theme as i32, proto::ItemFamily::AvatarFrame as i32);
        assert_eq!(items, [
            ("theme.pink", theme, 0),
            ("theme.violet", theme, 50),
            ("theme.red", theme, 50),
            ("frame.gold", frame, 60),
            ("frame.neon", frame, 60),
            ("frame.aurora", frame, 60),
            ("banner.animated", proto::ItemFamily::AnimatedBanner as i32, 500),
            ("card.paper", proto::ItemFamily::CardStyle as i32, 80),
            ("card.night", proto::ItemFamily::CardStyle as i32, 80),
            ("card.mint", proto::ItemFamily::CardStyle as i32, 80),
            ("avatar.animated", proto::ItemFamily::AnimatedAvatar as i32, 500),
        ]);

        let purchase = |account: &str| proto::PurchaseItemRequest {
            account_id:      account.to_owned(),
            idempotency_key: Uuid::now_v7().to_string(),
            item_id:         "theme.violet".into(),
            expected_price:  50,
        };
        let other = Uuid::now_v7().to_string();
        assert_eq!(h.purchase_item(as_caller(&me, purchase(&other))).await.unwrap_err().code(), Code::PermissionDenied);
        let mut teen = Request::new(purchase(&me));
        teen.extensions_mut().insert(principal_aged(&me, Some("13-15")));
        let refused = h.purchase_item(teen).await.unwrap_err();
        assert_eq!(refused.metadata().get(ERROR_CODE_METADATA).unwrap(), "WAL-3001");

        let bought = h.purchase_item(as_caller(&me, purchase(&me))).await.unwrap().into_inner();
        assert_eq!((bought.outcome, bought.wallet.unwrap().gems), (proto::PurchaseOutcome::Purchased as i32, 50));
        let owned = h.list_owned_items(as_caller(&me, proto::ListOwnedItemsRequest { account_id: me.clone() })).await.unwrap().into_inner();
        let owned: Vec<_> = owned.items.iter().map(|o| (o.item_id.as_str(), o.acquired_at.is_some())).collect();
        assert_eq!(owned, [("theme.pink", false), ("theme.violet", true)]);
        assert!(h.list_owned_items(as_caller(&me, proto::ListOwnedItemsRequest { account_id: other })).await.is_err());

        let export = h.export_wallet(Request::new(proto::ExportWalletRequest { account_id: me.clone() })).await.unwrap().into_inner();
        assert_eq!(export.owned_items.iter().map(|o| o.item_id.as_str()).collect::<Vec<_>>(), ["theme.violet"], "bought ones only");
    }

    /// #665: gems are not spent under 18 or with an unknown age.
    #[tokio::test]
    async fn a_minor_cannot_buy_a_pack_and_the_wallet_says_so() {
        let h = handler();
        let teen = Uuid::now_v7().to_string();
        let as_teen = |message| {
            let mut request = Request::new(message);
            request.extensions_mut().insert(principal_aged(&teen, Some("13-15")));
            request
        };
        let status = h
            .buy_stake_pack(as_teen(proto::BuyStakePackRequest {
                account_id:      teen.clone(),
                idempotency_key: Uuid::now_v7().to_string(),
                pack_id:         String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(status.metadata().get(ERROR_CODE_METADATA).unwrap(), "WAL-3001");

        let mut get = Request::new(proto::GetWalletRequest { account_id: teen.clone() });
        get.extensions_mut().insert(principal_aged(&teen, None));
        assert!(h.get_wallet(get).await.unwrap().into_inner().gem_spending_restricted, "age unknown: restricted");

        let adult = Uuid::now_v7().to_string();
        let bought = h
            .buy_stake_pack(as_caller(&adult, proto::BuyStakePackRequest {
                account_id:      adult.clone(),
                idempotency_key: Uuid::now_v7().to_string(),
                pack_id:         String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        let wallet = bought.wallet.unwrap();
        assert_eq!(bought.outcome, proto::StakePackOutcome::Bought as i32);
        assert_eq!((wallet.gems, wallet.stake_shots, wallet.stake_pack_price, wallet.gem_spending_restricted), (50, 3, 50, false));
        assert_eq!(wallet.stake_pack_id, "shots_3");
        let packs: Vec<_> = wallet.stake_packs.iter().map(|p| (p.pack_id.as_str(), p.shots, p.price)).collect();
        assert_eq!(packs, [("shots_3", 3, 50), ("shots_10", 10, 150), ("shots_25", 25, 325)]);
    }

    #[tokio::test]
    async fn spend_gems_takes_country_unlocks_only() {
        let h = handler();
        let account = Uuid::now_v7().to_string();
        let spend = |kind: proto::TransactionKind| {
            Request::new(proto::SpendGemsRequest {
                account_id:      account.clone(),
                amount:          15,
                kind:            kind as i32,
                ref_id:          "IT".into(),
                idempotency_key: "country-IT".into(),
            })
        };
        let spent = h.spend_gems(spend(proto::TransactionKind::CountryUnlock)).await.unwrap().into_inner();
        assert_eq!((spent.outcome, spent.gems), (proto::SpendGemsOutcome::Spent as i32, 85));
        assert_eq!(h.spend_gems(spend(proto::TransactionKind::Claim)).await.unwrap_err().code(), Code::InvalidArgument);
    }

    /// Mesh tokens `ns/name`; end-user tokens `<sub>|<age>`.
    struct Tokens;

    impl transport::grpc::mesh::MeshVerifier for Tokens {
        fn verify<'a>(&'a self, token: &'a str) -> transport::grpc::mesh::MeshVerification<'a> {
            Box::pin(async move {
                let (namespace, name) = token.split_once('/').ok_or_else(|| "bad".to_owned())?;
                Ok(auth_context::mesh::MeshCaller { namespace: namespace.into(), service_account: name.into() })
            })
        }
    }

    impl edge::StaffVerifier for Tokens {
        fn verify<'a>(&'a self, token: &'a str) -> edge::StaffVerification<'a> {
            Box::pin(async move {
                let (sub, age) = token.split_once('|')?;
                Some(principal_aged(sub, Some(age).filter(|a| !a.is_empty())).inner().clone())
            })
        }
    }

    fn gated(mode: MeshGateMode) -> WalletServiceHandler {
        handler().with_mesh_gate(
            MeshCallerGate::new(mode, Some(Arc::new(Tokens)), None),
            edge::StaffGate::new(Arc::new(Tokens)),
        )
    }

    fn mesh_call<T>(message: T, caller: Option<&str>, end_user: Option<&str>) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(caller) = caller {
            request.metadata_mut().insert(transport::grpc::mesh::MESH_TOKEN_HEADER, caller.parse().unwrap());
        }
        if let Some(user) = end_user {
            request.metadata_mut().insert("authorization", format!("Bearer {user}").parse().unwrap());
        }
        request
    }

    /// #852: gems are spent by geo-discovery only, for the account's own
    /// adult owner; the export reads are account's.
    #[tokio::test]
    async fn enforced_mesh_rpcs_take_their_intended_callers_only() {
        let h = gated(MeshGateMode::Enforce);
        let me = Uuid::now_v7().to_string();
        let spend = || proto::SpendGemsRequest {
            account_id:      me.clone(),
            amount:          15,
            kind:            proto::TransactionKind::CountryUnlock as i32,
            ref_id:          "IT".into(),
            idempotency_key: "country-IT".into(),
        };
        let adult = format!("{me}|18+");
        let refused = [
            (None, Some(adult.as_str())),
            (Some("core/staging-account-server"), Some(adult.as_str())),
            (Some("core/staging-geo-discovery-server"), None),
            (Some("core/staging-geo-discovery-server"), Some("someone-else|18+")),
            (Some("core/staging-geo-discovery-server"), Some(&*format!("{me}|13-17"))),
        ];
        for (caller, user) in refused {
            let status = h.spend_gems(mesh_call(spend(), caller, user)).await.unwrap_err();
            assert_eq!(status.code(), Code::PermissionDenied, "{caller:?} {user:?}");
        }
        let spent = h
            .spend_gems(mesh_call(spend(), Some("core/staging-geo-discovery-server"), Some(&adult)))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(spent.outcome, proto::SpendGemsOutcome::Spent as i32);

        let export = || proto::ExportWalletRequest { account_id: me.clone() };
        assert_eq!(h.export_wallet(mesh_call(export(), Some("core/geo-discovery-server"), None)).await.unwrap_err().code(), Code::PermissionDenied);
        assert!(h.export_wallet(mesh_call(export(), Some("core/account-server"), None)).await.is_ok());
        let history = |caller| {
            mesh_call(
                proto::ListWalletTransactionsRequest { account_id: me.clone(), currency: 0, page_size: 0, page_token: String::new() },
                caller,
                None,
            )
        };
        assert!(h.list_wallet_transactions(history(Some("core/geo-discovery-server"))).await.is_err());
        assert!(h.list_wallet_transactions(history(Some("core/account-server"))).await.is_ok());
        // The app reads its own history on the edge, with no mesh token.
        assert!(h.list_wallet_transactions(as_caller(&me, history(None).into_inner())).await.is_ok());

        // log mode lets everything through (the rollout's first step).
        let logged = gated(MeshGateMode::Log);
        assert!(logged.spend_gems(mesh_call(spend(), None, None)).await.is_ok());
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

    /// #665: a like names one of the caller's profiles, never their own post.
    #[tokio::test]
    async fn a_stake_is_the_callers_and_never_on_their_own_content() {
        use crate::application::wallets::fakes::{MemAudience, MemPublisher, MemTargets};
        let targets = Arc::new(MemTargets::default());
        targets.0.lock().unwrap().insert("mine".into(), ("my-profile".into(), true));
        let wallets = Wallets::new(Arc::new(MemoryStore::default()), WalletConfig::default())
            .with_stakes(targets as _, Arc::new(MemAudience::default()) as _, Arc::new(MemPublisher::default()) as _);
        let h = WalletServiceHandler::new(Arc::new(wallets));
        let me = Uuid::now_v7().to_string();
        let stake = |profile: &str| proto::StakeRequest {
            account_id:      me.clone(),
            profile_id:      profile.into(),
            target:          Some(proto::stake_request::Target::PostId("mine".into())),
            points:          1,
            use_stake_shot:  false,
            idempotency_key: Uuid::now_v7().to_string(),
            first_tap_at:    Some(timestamp(Utc::now())),
            conviction:      0,
        };
        let other = h.stake(as_caller(&me, stake("someone-else"))).await.unwrap_err();
        assert_eq!(other.code(), Code::PermissionDenied, "not the caller's profile");
        let own = h.stake(as_caller(&me, stake("my-profile"))).await.unwrap().into_inner();
        assert_eq!((own.outcome, own.spent), (proto::StakeOutcome::OwnContent as i32, 0));
    }
}
