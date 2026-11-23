//! Single-source embedded static web assets with precomputed SHA-256 ETags and RFC 9110 `304 Not Modified` (`aegis-web::assets`).

use crate::server::AppState;
use axum::{
    body::Body,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

/// Descriptor for a static web UI asset bundled into the binary via `include_bytes!`.
pub struct StaticAssetEntry {
    pub rel_path: &'static str,
    pub content_type: &'static str,
    pub bytes: &'static [u8],
    etag: OnceLock<String>,
}

impl StaticAssetEntry {
    const fn new(rel_path: &'static str, content_type: &'static str, bytes: &'static [u8]) -> Self {
        Self {
            rel_path,
            content_type,
            bytes,
            etag: OnceLock::new(),
        }
    }

    /// Returns the precomputed strong quoted SHA-256 ETag (`"<16-hex>"`).
    pub fn etag(&self) -> &str {
        self.etag
            .get_or_init(|| compute_quoted_etag(self.bytes))
            .as_str()
    }
}

pub static ASSET_INDEX_HTML: StaticAssetEntry = StaticAssetEntry::new(
    "index.html",
    "text/html; charset=utf-8",
    include_bytes!("../../../web/static/index.html"),
);
pub static ASSET_STYLES_CSS: StaticAssetEntry = StaticAssetEntry::new(
    "styles.css",
    "text/css; charset=utf-8",
    include_bytes!("../../../web/static/styles.css"),
);
pub static ASSET_APP_JS: StaticAssetEntry = StaticAssetEntry::new(
    "app.js",
    "application/javascript; charset=utf-8",
    include_bytes!("../../../web/static/app.js"),
);
pub static ASSET_JS_STATE: StaticAssetEntry = StaticAssetEntry::new(
    "js/state.js",
    "application/javascript; charset=utf-8",
    include_bytes!("../../../web/static/js/state.js"),
);
pub static ASSET_JS_FORMAT: StaticAssetEntry = StaticAssetEntry::new(
    "js/format.js",
    "application/javascript; charset=utf-8",
    include_bytes!("../../../web/static/js/format.js"),
);
pub static ASSET_JS_API: StaticAssetEntry = StaticAssetEntry::new(
    "js/api.js",
    "application/javascript; charset=utf-8",
    include_bytes!("../../../web/static/js/api.js"),
);
pub static ASSET_JS_TABLES: StaticAssetEntry = StaticAssetEntry::new(
    "js/tables.js",
    "application/javascript; charset=utf-8",
    include_bytes!("../../../web/static/js/tables.js"),
);
pub static ASSET_JS_CHARTS: StaticAssetEntry = StaticAssetEntry::new(
    "js/charts.js",
    "application/javascript; charset=utf-8",
    include_bytes!("../../../web/static/js/charts.js"),
);
pub static ASSET_JS_CONFIG: StaticAssetEntry = StaticAssetEntry::new(
    "js/config.js",
    "application/javascript; charset=utf-8",
    include_bytes!("../../../web/static/js/config.js"),
);
pub static ASSET_JS_MODALS: StaticAssetEntry = StaticAssetEntry::new(
    "js/modals.js",
    "application/javascript; charset=utf-8",
    include_bytes!("../../../web/static/js/modals.js"),
);
pub static ASSET_JS_MAIN: StaticAssetEntry = StaticAssetEntry::new(
    "js/main.js",
    "application/javascript; charset=utf-8",
    include_bytes!("../../../web/static/js/main.js"),
);

/// Strict allowlist mapping `/js/:filename` exclusively to the 8 ES module assets.
/// Rejects path traversal (`..`), arbitrary extensions, and unknown filenames with `None` (`404`).
pub fn resolve_js_module_asset(filename: &str) -> Option<&'static StaticAssetEntry> {
    match filename {
        "state.js" => Some(&ASSET_JS_STATE),
        "format.js" => Some(&ASSET_JS_FORMAT),
        "api.js" => Some(&ASSET_JS_API),
        "tables.js" => Some(&ASSET_JS_TABLES),
        "charts.js" => Some(&ASSET_JS_CHARTS),
        "config.js" => Some(&ASSET_JS_CONFIG),
        "modals.js" => Some(&ASSET_JS_MODALS),
        "main.js" => Some(&ASSET_JS_MAIN),
        _ => None,
    }
}

/// Computes a strong quoted SHA-256 ETag (`"<16-hex>"`).
pub fn compute_quoted_etag(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(18);
    hex.push('"');
    for b in &digest[..8] {
        use std::fmt::Write;
        let _ = write!(&mut hex, "{b:02x}");
    }
    hex.push('"');
    hex
}

/// Evaluates `If-None-Match` per RFC 9110 §13.1.2 (weak comparison, comma lists, `*`, and `W/` prefix).
pub fn if_none_match_matches(headers: &HeaderMap, strong_etag: &str) -> bool {
    let normalized_target = strong_etag.trim();
    for hv in headers.get_all(header::IF_NONE_MATCH) {
        let Ok(raw) = hv.to_str() else {
            continue;
        };
        for item in raw.split(',') {
            let trimmed = item.trim();
            if trimmed == "*" {
                return true;
            }
            let tag = trimmed
                .strip_prefix("W/")
                .or_else(|| trimmed.strip_prefix("w/"))
                .unwrap_or(trimmed)
                .trim();
            if tag == normalized_target {
                return true;
            }
        }
    }
    false
}

/// Returns `true` when `AEGIS_WEB_DEV_STATIC` explicitly opts into live disk overrides.
pub fn is_dev_static_override_enabled() -> bool {
    std::env::var("AEGIS_WEB_DEV_STATIC")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

const CACHE_CONTROL_REVALIDATE: &str = "public, max-age=0, must-revalidate";

/// Serves a static UI asset directly from embedded bytes (or allowlisted `rel_path` when dev override
/// is explicitly enabled), honoring `If-None-Match` with `304 Not Modified`.
pub async fn serve_static_asset(
    state: &AppState,
    headers: &HeaderMap,
    asset: &'static StaticAssetEntry,
) -> Response {
    let (body_bytes, etag): (Vec<u8>, String) = if state.dev_static_override {
        let disk_path = state.static_dir.join(asset.rel_path);
        match tokio::fs::read(&disk_path).await {
            Ok(disk_bytes) => {
                let tag = compute_quoted_etag(&disk_bytes);
                (disk_bytes, tag)
            }
            Err(_) => (asset.bytes.to_vec(), asset.etag().to_string()),
        }
    } else {
        (Vec::new(), asset.etag().to_string())
    };

    if if_none_match_matches(headers, &etag) {
        let mut resp = Response::new(Body::empty());
        *resp.status_mut() = StatusCode::NOT_MODIFIED;
        resp.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(CACHE_CONTROL_REVALIDATE),
        );
        if let Ok(hv) = HeaderValue::from_str(&etag) {
            resp.headers_mut().insert(header::ETAG, hv);
        }
        return resp;
    }

    let body = if state.dev_static_override {
        Body::from(body_bytes)
    } else {
        Body::from(asset.bytes)
    };
    let mut resp = Response::new(body);
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(asset.content_type),
    );
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_CONTROL_REVALIDATE),
    );
    if let Ok(hv) = HeaderValue::from_str(&etag) {
        resp.headers_mut().insert(header::ETAG, hv);
    }
    resp.into_response()
}
