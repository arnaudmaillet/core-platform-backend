//! The gem catalogue (#892): what earned gems buy, as server policy. The
//! **account** owns an item, like a country unlock; nothing about it reaches
//! ranking or visibility (charter V5.3 §35). The app bundles each item's art
//! keyed by its id; the server sends ids and prices only.

use crate::domain::{TransactionKind, Wallet};

/// What kind of thing an item is: an item id is `<family>.<name>`, and each
/// family has its own ledger kind (#892: one `TransactionKind` per family).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ItemFamily {
    /// An alternate app icon (`icon.<name>`).
    AppIcon,
    /// An app accent theme (`theme.<name>`, #946).
    Theme,
    /// An avatar frame (`frame.<name>`, #913), equipped per profile.
    AvatarFrame,
    /// An animated profile banner (`banner.<name>`, #915): unlocks setting a
    /// video banner.
    AnimatedBanner,
    /// A post card style (`card.<name>`, #914): applied by an author to their
    /// own posts.
    CardStyle,
    /// A badge (`badge.<name>`, #912): earned, never sold — awarded by the
    /// service whose rule it is (`AwardBadge`), shown in profiles' showcases.
    Badge,
    /// The animated avatar (`avatar.<name>`, #945): unlocks setting a clip as
    /// the avatar.
    AnimatedAvatar,
    /// A loader style (`loader.<name>`, #975): the app's loading animation,
    /// applied on the device. Loaders the viewer uploads are not items: they
    /// take a slot each ([`LoaderSlotPolicy`]).
    Loader,
}

impl ItemFamily {
    pub const ALL: [Self; 8] = [
        Self::AppIcon,
        Self::Theme,
        Self::AvatarFrame,
        Self::AnimatedBanner,
        Self::CardStyle,
        Self::Badge,
        Self::AnimatedAvatar,
        Self::Loader,
    ];

    fn prefix(self) -> &'static str {
        match self {
            Self::AppIcon => "icon",
            Self::Theme => "theme",
            Self::AvatarFrame => "frame",
            Self::AnimatedBanner => "banner",
            Self::CardStyle => "card",
            Self::Badge => "badge",
            Self::AnimatedAvatar => "avatar",
            Self::Loader => "loader",
        }
    }

    /// The family `item_id` belongs to; `None` when it is not a well-formed
    /// `<family>.<name>` id (`name`: 1–32 of `[a-z0-9_]`).
    pub fn of(item_id: &str) -> Option<Self> {
        let (family, name) = item_id.split_once('.')?;
        let named = (1..=32).contains(&name.len()) && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        Self::ALL.into_iter().find(|f| f.prefix() == family).filter(|_| named)
    }

    /// Whether items of this family are sold (badges are earned).
    pub fn sold(self) -> bool {
        self != Self::Badge
    }

    /// The ledger kind of a purchase in this family.
    pub fn transaction_kind(self) -> TransactionKind {
        match self {
            Self::AppIcon => TransactionKind::AppIcon,
            Self::Theme => TransactionKind::Theme,
            Self::AvatarFrame => TransactionKind::AvatarFrame,
            Self::AnimatedBanner => TransactionKind::AnimatedBanner,
            Self::CardStyle => TransactionKind::CardStyle,
            // Never bought: no ledger row.
            Self::Badge => TransactionKind::Unknown,
            Self::AnimatedAvatar => TransactionKind::AnimatedAvatar,
            Self::Loader => TransactionKind::LoaderStyle,
        }
    }
}

/// One item on sale. A free item (price 0) is owned by every account and
/// never bought (`theme.pink`, the default accent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogItem {
    pub id:         String,
    pub family:     ItemFamily,
    pub price_gems: i64,
}

impl CatalogItem {
    pub fn free(&self) -> bool {
        self.price_gems == 0
    }
}

/// The items on sale, in display order; ids distinct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    pub items: Vec<CatalogItem>,
}

impl Default for Catalog {
    /// The accent themes (#946): pink free, violet and red bought; three
    /// avatar frames (#913); the animated banner (#915, 500 gems); three post
    /// card styles (#914); the animated avatar (#945, 500 gems); the default
    /// loader, `loader.wave` (#975, free: the paid styles are not picked yet).
    /// No alternate icon until the app bundles one (`WALLET_CATALOG` adds them).
    fn default() -> Self {
        let item = |family, id: &str, price_gems| CatalogItem { id: id.to_owned(), family, price_gems };
        Self {
            items: vec![
                item(ItemFamily::Theme, "theme.pink", 0),
                item(ItemFamily::Theme, "theme.violet", 50),
                item(ItemFamily::Theme, "theme.red", 50),
                item(ItemFamily::AvatarFrame, "frame.gold", 60),
                item(ItemFamily::AvatarFrame, "frame.neon", 60),
                item(ItemFamily::AvatarFrame, "frame.aurora", 60),
                item(ItemFamily::AnimatedBanner, "banner.animated", 500),
                item(ItemFamily::CardStyle, "card.paper", 80),
                item(ItemFamily::CardStyle, "card.night", 80),
                item(ItemFamily::CardStyle, "card.mint", 80),
                item(ItemFamily::AnimatedAvatar, "avatar.animated", 500),
                item(ItemFamily::Loader, "loader.wave", 0),
            ],
        }
    }
}

impl Catalog {
    pub fn item(&self, item_id: &str) -> Option<&CatalogItem> {
        self.items.iter().find(|item| item.id == item_id)
    }

    /// The items every account owns.
    pub fn free_items(&self) -> impl Iterator<Item = &CatalogItem> {
        self.items.iter().filter(|item| item.free())
    }
}

/// Loaders a viewer uploads (#975): each import takes a slot, paid then and
/// never given back — deleting a loader frees no slot to reuse, only room
/// under the cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoaderSlotPolicy {
    /// The most uploaded loaders an account holds at once.
    pub cap:        usize,
    /// What one import costs.
    pub price_gems: i64,
}

impl Default for LoaderSlotPolicy {
    /// 10 loaders, 50 gems a slot (a placeholder until the owner prices it).
    fn default() -> Self {
        Self { cap: 10, price_gems: 50 }
    }
}

/// What importing a loader would do now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoaderImportDecision {
    Import,
    /// The account holds [`LoaderSlotPolicy::cap`] loaders already.
    MaxReached,
    /// The app showed another price.
    PriceChanged,
    InsufficientGems,
}

/// What buying an item would do now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PurchaseDecision {
    Buy,
    /// Owned already (or free): nothing charged.
    AlreadyOwned,
    /// The app showed another price: nothing charged.
    PriceChanged,
    InsufficientGems,
}

impl Wallet {
    /// What buying `item`, shown at `expected_price`, would do; `owned`: the
    /// account owns it already.
    pub fn decide_purchase(&self, item: &CatalogItem, expected_price: i64, owned: bool) -> PurchaseDecision {
        if owned || item.free() {
            PurchaseDecision::AlreadyOwned
        } else if expected_price != item.price_gems {
            PurchaseDecision::PriceChanged
        } else if !self.can_spend_gems(item.price_gems) {
            PurchaseDecision::InsufficientGems
        } else {
            PurchaseDecision::Buy
        }
    }
}

impl Wallet {
    /// What importing a loader, its slot shown at `expected_price`, would do
    /// while the account holds `held` uploaded loaders.
    pub fn decide_loader_import(&self, policy: &LoaderSlotPolicy, expected_price: i64, held: usize) -> LoaderImportDecision {
        if held >= policy.cap {
            LoaderImportDecision::MaxReached
        } else if expected_price != policy.price_gems {
            LoaderImportDecision::PriceChanged
        } else if !self.can_spend_gems(policy.price_gems) {
            LoaderImportDecision::InsufficientGems
        } else {
            LoaderImportDecision::Import
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::domain::AccountId;

    #[test]
    fn an_item_id_names_its_family() {
        assert_eq!(ItemFamily::of("theme.violet"), Some(ItemFamily::Theme));
        assert_eq!(ItemFamily::of("icon.night_2"), Some(ItemFamily::AppIcon));
        assert_eq!(ItemFamily::of("frame.gold"), Some(ItemFamily::AvatarFrame));
        for bad in ["theme", "theme.", "medal.gold", "theme.Violet", "theme.a.b", ".violet"] {
            assert_eq!(ItemFamily::of(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn a_purchase_needs_the_shown_price_the_gems_and_an_item_not_owned() {
        let catalog = Catalog::default();
        let violet = catalog.item("theme.violet").unwrap();
        let mut wallet = Wallet::opened(AccountId::from_uuid(Uuid::now_v7()), 100);
        assert_eq!(wallet.decide_purchase(violet, 50, false), PurchaseDecision::Buy);
        assert_eq!(wallet.decide_purchase(violet, 40, false), PurchaseDecision::PriceChanged);
        assert_eq!(wallet.decide_purchase(violet, 50, true), PurchaseDecision::AlreadyOwned);
        assert_eq!(wallet.decide_purchase(catalog.item("theme.pink").unwrap(), 0, false), PurchaseDecision::AlreadyOwned, "free");
        wallet.gems = 49;
        assert_eq!(wallet.decide_purchase(violet, 50, false), PurchaseDecision::InsufficientGems);
        assert_eq!(catalog.free_items().map(|i| i.id.as_str()).collect::<Vec<_>>(), ["theme.pink", "loader.wave"]);
    }

    /// #975: loader styles are items; an uploaded loader takes a slot, under
    /// a cap, at the shown price.
    #[test]
    fn a_loader_import_needs_room_the_shown_price_and_the_gems() {
        assert_eq!(ItemFamily::of("loader.wave"), Some(ItemFamily::Loader));
        assert_eq!(ItemFamily::Loader.transaction_kind(), TransactionKind::LoaderStyle);
        let policy = LoaderSlotPolicy::default();
        let mut wallet = Wallet::opened(AccountId::from_uuid(Uuid::now_v7()), 100);
        assert_eq!(wallet.decide_loader_import(&policy, 50, 9), LoaderImportDecision::Import);
        assert_eq!(wallet.decide_loader_import(&policy, 50, 10), LoaderImportDecision::MaxReached);
        assert_eq!(wallet.decide_loader_import(&policy, 40, 0), LoaderImportDecision::PriceChanged);
        wallet.gems = 49;
        assert_eq!(wallet.decide_loader_import(&policy, 50, 0), LoaderImportDecision::InsufficientGems);
    }
}
