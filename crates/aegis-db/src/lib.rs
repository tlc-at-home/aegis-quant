//! Aegis Quant v2.0 MariaDB 13.0 Rolling / 12.3 LTS Repository Layer (`aegis-db`).
//! Strict module manifest adhering to the zero-logic `lib.rs` policy.

pub mod assets_repo;
pub mod backup;
pub mod bars_repo;
pub mod config_repo;
pub mod cooldowns_repo;
pub mod engine;
pub mod fernet;
pub mod orders_repo;
pub mod portfolio_repo;
pub mod positions_repo;
pub mod signals_repo;
pub mod views_repo;

pub use aegis_core::ConfigMap;
pub use assets_repo::*;
pub use backup::*;
pub use bars_repo::*;
pub use engine::*;
pub use fernet::{
    default_host_fernet_key_b64, ensure_master_key_for_db, generate_fernet_key, is_encrypted_value,
    is_legacy_public_fallback_key, is_usable_env_key, master_key_file_for_db, normalize_fernet_key,
    resolve_fernet_master_key, write_master_key_file, FernetCipher,
};
pub use portfolio_repo::*;
pub use positions_repo::*;
pub use signals_repo::*;
