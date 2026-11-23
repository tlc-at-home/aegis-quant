//! Aegis Quant v2.0 Core Domain Library (`aegis-core`).
//! Strict module manifest adhering to the zero-logic `lib.rs` policy.

pub mod config;
pub mod errors;
pub mod hl_parse;
pub mod indicators;
pub mod models;
pub mod security;
pub mod time;

pub use config::*;
pub use errors::*;
pub use hl_parse::*;
pub use indicators::*;
pub use models::*;
pub use security::*;
pub use time::*;
