//! Scenario — original-sound reuse permissions (#669): a sound belongs to the
//! post that made it; reusing someone else's sound follows that post's own
//! permission, else its author's default. Calling it "original" changes
//! nothing, and an unknown sound (a library track) is free.

use crate::post_it::harness::{self, ProfileId, ReuseDefaults, TestHarness};

#[tokio::test]
async fn reusing_a_sound_follows_its_post_then_its_author() {
    let h = TestHarness::start().await;
    let (creator, other) = (harness::random_id(), harness::random_id());
    let (locked_sound, open_sound) = (harness::random_id(), harness::random_id());

    // The creator's posts: one sound locked on the post, one following defaults.
    h.try_create_with_sound(&harness::random_id(), &creator, &locked_sound, true, Some(false)).await.unwrap();
    h.try_create_with_sound(&harness::random_id(), &creator, &open_sound, true, None).await.unwrap();

    // The post's own "no" wins; the author's default (allowed) covers the other.
    assert!(h.try_create_with_sound(&harness::random_id(), &other, &locked_sound, false, None).await.is_err());
    assert!(h.try_create_with_sound(&harness::random_id(), &other, &open_sound, false, None).await.is_ok());

    // The creator turns reuse off by default: the other sound closes too.
    let creator_pid = ProfileId::try_from(creator.as_str()).unwrap();
    h.reuse.set_defaults(&creator_pid, ReuseDefaults { allow_remix: true, allow_sound_reuse: false }).await.unwrap();
    assert!(h.try_create_with_sound(&harness::random_id(), &other, &open_sound, false, None).await.is_err());

    // Calling someone's sound "original" does not get around it.
    assert!(h.try_create_with_sound(&harness::random_id(), &other, &open_sound, true, None).await.is_err());

    // The creator reuses their own sounds freely; an unknown sound is free.
    assert!(h.try_create_with_sound(&harness::random_id(), &creator, &locked_sound, false, None).await.is_ok());
    assert!(h.try_create_with_sound(&harness::random_id(), &other, &harness::random_id(), false, None).await.is_ok());
}
