//! Aegis Quant v2.0 Rich HTML Telegram & Config-Switchable Discord Alert Engine (`aegis-alerts`).
//! Strict module manifest adhering to the zero-logic `lib.rs` policy (Rule 9 of `PLAN_V2.md`).

pub mod discord;
pub mod engine;
pub mod formatter;
pub mod queue;
pub mod telegram;

pub use discord::*;
pub use engine::*;
pub use formatter::*;
pub use queue::*;
pub use telegram::*;
