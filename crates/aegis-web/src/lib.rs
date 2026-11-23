//! Aegis Quant v2.0 Commercial Web Dashboard & Real-Time REST/SSE Server (`aegis-web`).
//! Strict module manifest adhering to the zero-logic `lib.rs` policy (Rule 9 of `PLAN_V2.md`).

pub mod assets;
pub mod auth;
pub mod handlers;
pub mod payloads;
pub mod routes;
pub mod server;
pub mod stream;

pub use assets::*;
pub use auth::*;
pub use handlers::*;
pub use payloads::*;
pub use routes::*;
pub use server::*;
pub use stream::*;
