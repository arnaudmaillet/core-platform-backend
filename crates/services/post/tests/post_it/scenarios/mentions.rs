//! Scenario — who can mention whom (#656) over the real store: a caption may
//! mention only profiles that take mentions from its author (social-graph
//! `CheckInteraction`), on create and on edit; at most 20; an outage fails the
//! write closed.

use error::AppError;

use crate::post_it::harness::{self, TestHarness};

fn mention(id: &str) -> String {
    format!("[@someone](profile:{id})")
}

#[tokio::test]
async fn a_caption_mentions_only_profiles_that_take_mentions_from_its_author() {
    let h = TestHarness::start().await;
    let (author, open, closed) = (harness::random_id(), harness::random_id(), harness::random_id());
    h.gate.refuse_mentions(&closed);

    // Mentioning someone who takes no mentions: refused, nothing stored.
    let post = harness::random_id();
    let err = h.try_create_captioned(&post, &author, &format!("hi {} {}", mention(&open), mention(&closed))).await.unwrap_err();
    assert_eq!(err.error_code(), "PST-1009");
    assert!(h.get(&post).await.is_err(), "not stored");

    // Only open profiles (and oneself): fine.
    h.try_create_captioned(&post, &author, &format!("hi {} {}", mention(&open), mention(&author))).await.expect("create");

    // Adding the closed profile on an edit: refused, the caption stays.
    let err = h.try_recaption(&post, &author, &format!("now {}", mention(&closed))).await.unwrap_err();
    assert_eq!(err.error_code(), "PST-1009");
    assert!(h.get(&post).await.unwrap().caption().as_str().starts_with("hi "));
}

#[tokio::test]
async fn at_most_twenty_mentions_and_an_outage_fails_closed() {
    let h = TestHarness::start().await;
    let author = harness::random_id();
    let crowd: String = (0..21).map(|_| mention(&harness::random_id())).collect::<Vec<_>>().join(" ");
    assert!(h.try_create_captioned(&harness::random_id(), &author, &crowd).await.is_err(), "21 mentions");

    h.gate.set_down(true);
    let err = h
        .try_create_captioned(&harness::random_id(), &author, &mention(&harness::random_id()))
        .await
        .unwrap_err();
    assert!(err.is_retryable(), "an outage refuses the write, retryably: {err:?}");
    // Without mentions social-graph is not asked: the post goes through.
    h.try_create_captioned(&harness::random_id(), &author, "no mentions").await.expect("no check needed");
}
