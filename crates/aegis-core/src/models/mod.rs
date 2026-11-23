//! Domain models matching MariaDB tables, SQL Views, portfolio analytics, and Hyperliquid vaults (`aegis-core::models`).
//! Strict module manifest adhering to the zero-logic policy.

pub mod domain;
pub mod portfolio;
pub mod vault;
pub mod views;

pub use domain::*;
pub use portfolio::*;
pub use vault::*;
pub use views::*;
