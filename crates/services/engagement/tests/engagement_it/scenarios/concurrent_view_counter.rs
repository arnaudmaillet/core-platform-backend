//! Scenario — concurrent view counter (atomic Redis increment).
//!
//! Unlike likes, views are a pure counter: every record is an unconditional
//! increment. Under a concurrent burst the total must be exact — no lost updates —
//! which the atomic Redis `INCR` guarantees. This is the concurrency axis on the
//! counter path.

use std::sync::Arc;

use crate::engagement_it::harness::{self, TestHarness, DEADLINE};

const VIEWS: usize = 20;

#[tokio::test]
async fn concurrent_view_records_sum_exactly() {
    let h = TestHarness::start().await;
    let post = harness::random_post();

    let mut handles = Vec::new();
    for _ in 0..VIEWS {
        let bus = Arc::clone(&h.command_bus);
        handles.push(tokio::spawn(harness::dispatch_view(bus, post.as_str())));
    }
    for handle in handles {
        handle.await.expect("join").expect("record_view");
    }

    let h = &h;
    let post = &post;
    harness::await_until("view count reaches the exact total", DEADLINE, || async move {
        h.snapshot(post).await.view_count == VIEWS as i64
    })
    .await;
}
