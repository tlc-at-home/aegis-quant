//! Aegis Quant v2.0 Two-Tier Execution & Market Surveillance Engine (`aegis-execution`).
//! Strict module manifest adhering to the zero-logic `lib.rs` policy.

pub mod engine;
pub mod http_client;
pub mod market_watcher;
pub mod order_chaser;
pub mod priority_monitor;
pub mod reconcile;
pub mod signing;

pub use engine::*;
pub use http_client::*;
pub use market_watcher::*;
pub use order_chaser::*;
pub use priority_monitor::*;
pub use reconcile::*;
pub use signing::*;
