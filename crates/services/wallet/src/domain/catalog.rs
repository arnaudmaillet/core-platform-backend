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
}

impl ItemFamily {
    pub const ALL: [Self; 3] = [Self::AppIcon, Self::Theme, Self::AvatarFrame];

    fn prefix(self) -> &'static str {
        match self {
            Self::AppIcon => "icon",
            Self::Theme => "theme",
            Self::AvatarFrame => "frame",
        }
    }

    /// The family `item_id` belongs to; `None` when it is not a well-formed
    /// `<family>.<name>` id (`name`: 1–32 of `[a-z0-9_]`).
    pub fn of(item_id: &str) -> Option<Self> {
        let (family, name) = item_id.split_once('.')?;
        let named = (1..=32).contains(&name.len()) && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        Self::ALL.into_iter().find(|f| f.prefix() == family).filter(|_| named)
    }

    /// The ledger kind of a purchase in this family.
    pub fn transaction_kind(self) -> TransactionKind {
        match self {
            Self::AppIcon => TransactionKind::AppIcon,
            Self::Theme => TransactionKind::Theme,
            Self::AvatarFrame => TransactionKind::AvatarFrame,
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
    /// avatar frames (#913). No alternate icon until the app bundles one
    /// (`WALLET_CATALOG` adds them).
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
        assert_eq!(catalog.free_items().map(|i| i.id.as_str()).collect::<Vec<_>>(), ["theme.pink"]);
    }
}
