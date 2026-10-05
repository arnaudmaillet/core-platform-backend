//! Core integration scenarios. Each module is a `#[tokio::test]`-bearing file
//! exercising one property of the Shadowing Pattern against live infra.

mod backpressure_recovery;
mod privacy_boundary;
mod stream_leak_raii;
mod visibility_teardown;
mod presence_settings;
mod private_join;
