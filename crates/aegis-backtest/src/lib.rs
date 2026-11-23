//! Aegis Quant v2.0 Multi-Sleeve Walk-Forward Backtest Engine (`aegis-backtest`).
//! Strict module manifest adhering to the zero-logic `lib.rs` policy (PLAN_V2.md Rule 9).

pub mod config;
pub mod dataset;
pub mod engine;
pub mod metrics;
pub mod settlement;

pub use config::*;
pub use dataset::*;
pub use engine::*;
pub use metrics::*;
pub use settlement::*;
