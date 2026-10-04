use crate::error::PostError;

/// What moderation currently imposes on a post. Moderation decides; post holds
/// the outcome so its reads can apply it (from `moderation.v1.events`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ModerationRestriction {
    /// No enforcement in force.
    #[default]
    None      = 0,
    /// Reach reduced (`visibility_limit`): still readable, kept out of discovery.
    Limited   = 1,
    /// Shown only to audiences cleared for mature content (`age_gate`).
    AgeGated  = 2,
    /// Taken down (`remove_content`): only the author still sees it.
    Removed   = 3,
}

impl ModerationRestriction {
    pub fn as_tinyint(self) -> i8 {
        self as i8
    }
}

impl TryFrom<i8> for ModerationRestriction {
    type Error = PostError;

    fn try_from(v: i8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::Limited),
            2 => Ok(Self::AgeGated),
            3 => Ok(Self::Removed),
            _ => Err(PostError::DomainViolation {
                field:   "moderation_restriction".into(),
                message: format!("unknown ModerationRestriction discriminant: {v}"),
            }),
        }
    }
}

/// The restriction in force plus the moderation version that set it. The
/// version is kept when a reversal clears the restriction, so a stale
/// enforcement redelivered later cannot re-apply it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ModerationState {
    pub restriction: ModerationRestriction,
    /// Moderation's per-subject `EnforcementVersion`; 0 = never moderated.
    pub version:     i64,
}

impl ModerationState {
    /// Applies an enforcement outcome if it is newer than the one held. Returns
    /// whether the state changed.
    pub fn apply(&mut self, restriction: ModerationRestriction, version: i64) -> bool {
        if version <= self.version {
            return false;
        }
        *self = Self { restriction, version };
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_versions_apply_and_stale_ones_are_ignored() {
        let mut state = ModerationState::default();
        assert!(state.apply(ModerationRestriction::Removed, 1));
        assert!(state.apply(ModerationRestriction::None, 2), "the reversal");
        assert!(!state.apply(ModerationRestriction::Removed, 1), "a redelivered takedown");
        assert_eq!(state, ModerationState { restriction: ModerationRestriction::None, version: 2 });
    }

    #[test]
    fn tinyint_round_trips() {
        for r in [
            ModerationRestriction::None,
            ModerationRestriction::Limited,
            ModerationRestriction::AgeGated,
            ModerationRestriction::Removed,
        ] {
            assert_eq!(ModerationRestriction::try_from(r.as_tinyint()).unwrap(), r);
        }
        assert!(ModerationRestriction::try_from(9).is_err());
    }
}
