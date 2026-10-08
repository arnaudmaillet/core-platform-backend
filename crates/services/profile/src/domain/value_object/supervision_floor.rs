//! The floors a teen's supervisors set (#670 part 2): the loosest settings
//! the teen's profiles may have. The teen may only be stricter; a setting
//! below its floor is refused, and setting the floor tightens what is below.

use crate::domain::value_object::{DiscoverySettings, InteractionAudience, InteractionSettings, ProfileVisibility};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SupervisionFloor {
    pub private_account:    bool,
    pub messages:           Option<InteractionAudience>,
    pub comments:           Option<InteractionAudience>,
    /// Off: handle search, suggestions, finding by phone / email. A shared QR
    /// code or link still works.
    pub hidden_from_search: bool,
}

/// How strict an audience is: everyone < followers < mutuals < no one.
fn rank(audience: InteractionAudience) -> u8 {
    match audience {
        InteractionAudience::Everyone => 0,
        InteractionAudience::Followers => 1,
        InteractionAudience::Mutuals => 2,
        InteractionAudience::NoOne => 3,
    }
}

fn at_least(current: InteractionAudience, floor: Option<InteractionAudience>) -> InteractionAudience {
    match floor {
        Some(floor) if rank(current) < rank(floor) => floor,
        _ => current,
    }
}

impl SupervisionFloor {
    pub fn allows_visibility(&self, visibility: ProfileVisibility) -> bool {
        !(self.private_account && visibility == ProfileVisibility::Public)
    }

    pub fn tighten_visibility(&self, visibility: ProfileVisibility) -> ProfileVisibility {
        if self.allows_visibility(visibility) { visibility } else { ProfileVisibility::Private }
    }

    pub fn allows_interaction(&self, settings: &InteractionSettings) -> bool {
        self.tighten_interaction(*settings) == *settings
    }

    pub fn tighten_interaction(&self, settings: InteractionSettings) -> InteractionSettings {
        InteractionSettings {
            messages: at_least(settings.messages, self.messages),
            comments: at_least(settings.comments, self.comments),
            ..settings
        }
    }

    pub fn allows_discovery(&self, settings: &DiscoverySettings) -> bool {
        self.tighten_discovery(*settings) == *settings
    }

    pub fn tighten_discovery(&self, settings: DiscoverySettings) -> DiscoverySettings {
        if !self.hidden_from_search {
            return settings;
        }
        DiscoverySettings { by_handle_search: false, in_suggestions: false, by_phone: false, by_email: false, ..settings }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn floor() -> SupervisionFloor {
        SupervisionFloor {
            private_account:    true,
            messages:           Some(InteractionAudience::Mutuals),
            comments:           Some(InteractionAudience::Followers),
            hidden_from_search: true,
        }
    }

    #[test]
    fn looser_is_refused_stricter_is_allowed_and_tightening_meets_the_floor() {
        let f = floor();
        assert!(!f.allows_visibility(ProfileVisibility::Public));
        assert!(f.allows_visibility(ProfileVisibility::Private));
        assert_eq!(f.tighten_visibility(ProfileVisibility::Public), ProfileVisibility::Private);

        let open = InteractionSettings::default();
        assert!(!f.allows_interaction(&open));
        let tight = f.tighten_interaction(open);
        assert_eq!((tight.messages, tight.comments), (InteractionAudience::Mutuals, InteractionAudience::Followers));
        assert!(f.allows_interaction(&tight));
        // Stricter than the floor stays as is.
        let stricter = InteractionSettings { messages: InteractionAudience::NoOne, ..tight };
        assert!(f.allows_interaction(&stricter));
        assert_eq!(f.tighten_interaction(stricter), stricter);
        // What the floor does not cover is the teen's.
        assert!(f.allows_interaction(&InteractionSettings { allow_downloads: !tight.allow_downloads, ..tight }));

        let found = DiscoverySettings::default();
        assert!(!f.allows_discovery(&found));
        let hidden = f.tighten_discovery(found);
        assert!(!hidden.by_handle_search && !hidden.in_suggestions && !hidden.by_phone && !hidden.by_email);
        assert!(hidden.by_qr, "a shared QR code still works");
        assert!(SupervisionFloor::default().allows_discovery(&DiscoverySettings::default()), "no floor, no lock");
    }
}
