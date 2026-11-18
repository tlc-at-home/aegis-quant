//! Aegis Quant v2.0 Risk Management, Order-to-Open Capital Sizing & 1s Trailing Stop Engine (`aegis-risk`).
//! Strict module manifest adhering to the zero-logic `lib.rs` policy (PLAN_V2.md Rule 9).

pub mod engine;
pub mod limits;
pub mod stops;

pub use engine::*;
pub use limits::*;
pub use stops::*;
