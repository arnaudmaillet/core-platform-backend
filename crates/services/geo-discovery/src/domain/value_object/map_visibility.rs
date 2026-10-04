/// Why the map no longer shows a post (its card and pin are kept out of every
/// read path; the row stays until its TTL so a reversal can restore it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Suppression {
    /// Shown.
    #[default]
    None,
    /// The post was deleted. Permanent: nothing lifts it.
    Deleted,
    /// Moderation removed or limited the post. A newer reversal lifts it.
    Moderated,
}

impl Suppression {
    pub fn as_tinyint(self) -> i8 {
        match self {
            Self::None => 0,
            Self::Deleted => 1,
            Self::Moderated => 2,
        }
    }

    /// Unknown values read as `Moderated`: hidden, but liftable.
    pub fn from_tinyint(v: Option<i8>) -> Self {
        match v {
            None | Some(0) => Self::None,
            Some(1) => Self::Deleted,
            Some(_) => Self::Moderated,
        }
    }
}

/// The suppression held on a card and the moderation version that set it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MapVisibility {
    pub suppression:        Suppression,
    pub moderation_version: i64,
}

/// A change the map must apply to one post.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityChange {
    /// The post was deleted.
    Deleted,
    /// Moderation restricted (`true`: remove_content / visibility_limit) or
    /// lifted (`false`: a reversal) at `version`.
    Moderation { restricted: bool, version: i64 },
}

impl MapVisibility {
    /// The state after `change`, or `None` when the change does not apply: a
    /// deleted post never comes back, and a moderation event older than the
    /// held one is stale. An event *equal* to the held one is applied again (a
    /// redelivery after a partial write), which is idempotent.
    pub fn apply(self, change: VisibilityChange) -> Option<Self> {
        if self.suppression == Suppression::Deleted {
            return None;
        }
        match change {
            VisibilityChange::Deleted => Some(Self { suppression: Suppression::Deleted, ..self }),
            VisibilityChange::Moderation { restricted, version } => {
                if version < self.moderation_version {
                    return None;
                }
                let suppression = if restricted { Suppression::Moderated } else { Suppression::None };
                Some(Self { suppression, moderation_version: version })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHOWN: MapVisibility = MapVisibility { suppression: Suppression::None, moderation_version: 0 };

    fn moderation(restricted: bool, version: i64) -> VisibilityChange {
        VisibilityChange::Moderation { restricted, version }
    }

    #[test]
    fn a_takedown_hides_and_a_newer_reversal_restores() {
        let hidden = SHOWN.apply(moderation(true, 1)).unwrap();
        assert_eq!(hidden.suppression, Suppression::Moderated);
        let shown = hidden.apply(moderation(false, 2)).unwrap();
        assert_eq!(shown.suppression, Suppression::None);
        assert_eq!(shown.apply(moderation(true, 1)), None, "a stale takedown");
        assert_eq!(hidden.apply(moderation(true, 1)), Some(hidden), "a redelivery re-applies");
    }

    #[test]
    fn a_deleted_post_never_comes_back() {
        let deleted = SHOWN.apply(VisibilityChange::Deleted).unwrap();
        assert_eq!(deleted.suppression, Suppression::Deleted);
        assert_eq!(deleted.apply(moderation(false, 9)), None);
        assert_eq!(deleted.apply(VisibilityChange::Deleted), None);
    }

    #[test]
    fn tinyint_round_trips() {
        for s in [Suppression::None, Suppression::Deleted, Suppression::Moderated] {
            assert_eq!(Suppression::from_tinyint(Some(s.as_tinyint())), s);
        }
        assert_eq!(Suppression::from_tinyint(None), Suppression::None);
    }
}
