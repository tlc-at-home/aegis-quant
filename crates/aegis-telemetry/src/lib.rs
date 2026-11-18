//! Aegis Quant v2.0 Operational Telemetry Engine, Tracing Subscriber & Alert Engine Re-exports (`aegis-telemetry`).
//! Strict module manifest adhering to the zero-logic `lib.rs` policy (Rule 9 of `PLAN_V2.md`).

pub mod engine;

pub use aegis_alerts::{AlertDispatchResult, AlertEngine, AlertRetryQueue, QueuedAlertTask};
pub use engine::*;
