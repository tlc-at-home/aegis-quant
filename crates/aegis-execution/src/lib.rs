//! Aegis Quant v2.0 Two-Tier Execution & Market Surveillance Engine (`aegis-execution`).
//! Strict module manifest adhering to the zero-logic `lib.rs` policy.

pub mod close_ladder;
pub mod engine;
pub mod http_client;
pub mod lock_guard;
pub mod market_watcher;
pub mod order_chaser;
pub mod priority_monitor;
pub mod reconcile;
pub mod signing;
pub mod tpsl_sync;
pub mod ws_client;

pub use close_ladder::*;
pub use engine::*;
pub use http_client::*;
pub use market_watcher::*;
pub use order_chaser::*;
pub use priority_monitor::*;
pub use reconcile::*;
pub use signing::*;
pub use tpsl_sync::*;
pub use ws_client::*;
