//! # wallet
//!
//! The in-app economy's ledger (#665): an account's **points** (shown as
//! likes) and **gems**, every movement an append-only transaction. Nothing is
//! sold for real money. TIER-0, fail-closed: a balance is never guessed.
//!
//! `domain` (pure rules: the hourly claim, the ledger's vocabulary) →
//! `application` (the [`application::Wallets`] handler and its store port) →
//! `infrastructure` (Postgres, gRPC, the account-erasure consumer).

pub mod app;
pub mod application;
pub mod config;
pub mod domain;
pub mod error;
pub mod infrastructure;
pub mod service;
