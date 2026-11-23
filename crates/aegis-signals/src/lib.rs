//! Aegis Quant v2.0 Multi-Sleeve Signal & Portfolio Allocation Engine (`aegis-signals`).
//! Strict module manifest adhering to the zero-logic `lib.rs` policy (PLAN_V2.md Rule 9).

pub mod allocator;
pub mod breadth;
pub mod carry_momentum;
pub mod engine;
pub mod event_reversion;
pub mod features;
pub mod liquidity;
pub mod trend_breakout;

pub use allocator::*;
pub use breadth::*;
pub use carry_momentum::*;
pub use engine::*;
pub use event_reversion::*;
pub use features::*;
pub use liquidity::*;
pub use trend_breakout::*;
