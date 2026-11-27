//! Integration tests for Aegis Quant v2.0 Web Dashboard & REST/SSE API (`aegis-web`).

use aegis_core::{now_ms, AegisError, OrderSide};
use aegis_db::{
    AssetUpsertInput, DatabaseManager, ExchangeOpenPositionSyncInput, OidUpdate,
    SnapshotInsertInput, WorkingOrderInput,
};
use aegis_web::{
    build_router, create_web_router, create_web_router_for_host, health_result_to_response,
    serve_web_with_shutdown, validate_config_key_value, AppState, AuthLoginPayload,
    ClosePositionRequest, ConfigUpdateRequest, RiskTierUpdateRequest, DEFAULT_CSP_POLICY,
};
use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{header, Request, StatusCode},
};
use mysql::params;
use mysql::prelude::*;
use serde_json::Value;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio_stream::StreamExt;
use tower::ServiceExt;

fn new_test_db() -> DatabaseManager {
    DatabaseManager::new_isolated_test(None).unwrap()
}

fn new_test_db_noauth() -> DatabaseManager {
    let mgr = DatabaseManager::new_isolated_test(None).unwrap();
    mgr.set_config_value("web_auth_enabled", "false", None)
        .unwrap();
    mgr
}

#[tokio::test]
async fn test_web_health_dashboard_risk_tier_and_position_close() {
    let db = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("execution_mode", "PAPER", None)
            .unwrap();
        let btc_id = mgr
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "BTC".to_string(),
                asset_index: 0,
                sz_decimals: 4,
                tick_size: 0.1,
                lot_step_size: 0.001,
                max_leverage: 40.0,
                mark_price: 94250.0,
                mid_price: 94250.0,
                oracle_price: 94248.0,
                funding_rate_hourly: 0.000015,
                day_volume_usdc: 85_000_000.0,
                open_interest_usdc: 25_000_000.0,
                prev_day_price: 92100.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();

        let _pos_id = mgr
            .create_working_order(&WorkingOrderInput {
                asset_id: btc_id,
                client_order_id: "test-web-oid-1".to_string(),
                side: OrderSide::Long,
                limit_price: 94200.0,
                requested_quantity: 0.05,
                reserved_margin_usdc: 4710.0,
                portfolio_weight: 0.471,
                s1_weight_contrib: 0.20,
                s2_weight_contrib: 0.271,
                s3_weight_contrib: 0.0,
                protective_stop_px: 90432.0,
                take_profit_px: None,
                exchange_order_id: Some("hl-oid-101".to_string()),
                reason: "S1_S2_CONFLUENCE".to_string(),
            })
            .unwrap();

        mgr
    })
    .await
    .unwrap();

    let app = create_web_router(db.clone(), None);

    // 1. Static assets: GET /, GET /styles.css, GET /app.js
    let res_idx = app
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res_idx.status(), StatusCode::OK);

    let res_css = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/styles.css")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_css.status(), StatusCode::OK);

    let res_js = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/app.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_js.status(), StatusCode::OK);

    // 2. GET /api/v1/health
    let res_health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_health.status(), StatusCode::OK);
    let health_bytes = axum::body::to_bytes(res_health.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let health_json: Value = serde_json::from_slice(&health_bytes).unwrap();
    assert_eq!(health_json["status"], "ok");
    assert_eq!(health_json["execution_mode"], "PAPER");
    assert_eq!(health_json["risk_tier"], "extreme");
    assert_eq!(health_json["views_healthy_count"], 7);

    // 3. GET /api/v1/dashboard
    let res_dash = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_dash.status(), StatusCode::OK);
    let dash_bytes = axum::body::to_bytes(res_dash.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let dash_json: Value = serde_json::from_slice(&dash_bytes).unwrap();
    assert_eq!(dash_json["v_assets_ordered"].as_array().unwrap().len(), 1);
    assert_eq!(dash_json["v_assets_ordered"][0]["symbol"], "BTC-USDC");
    assert_eq!(dash_json["risk_capital_cap_updated_at_ms"], 0);
    assert_eq!(dash_json["max_open_positions"], 6);
    assert_eq!(dash_json["active_cap_open_count"], 0);
    assert_eq!(dash_json["pre_cap_open_count"], 0);
    assert!(dash_json["pnl_summary"]["daily"].is_object());
    assert!(dash_json["pnl_summary"]["weekly"].is_object());
    assert!(dash_json["pnl_summary"]["monthly"].is_object());
    assert!(dash_json["pnl_summary"]["ytd"].is_object());

    // 4. POST /api/v1/risk-tier
    let tier_req = RiskTierUpdateRequest {
        risk_tier: "aggressive".to_string(),
        custom_leverage_multiplier: None,
    };
    let res_tier = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/risk-tier")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&tier_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_tier.status(), StatusCode::OK);
    let tier_bytes = axum::body::to_bytes(res_tier.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let tier_json: Value = serde_json::from_slice(&tier_bytes).unwrap();
    assert_eq!(tier_json["risk_tier"], "aggressive");
    assert!((tier_json["target_leverage_multiplier"].as_f64().unwrap() - 1.35).abs() < 1e-6);

    // Switch back to extreme (1.85x)
    let tier_ext = RiskTierUpdateRequest {
        risk_tier: "extreme".to_string(),
        custom_leverage_multiplier: None,
    };
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/risk-tier")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&tier_ext).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    // 5. POST /api/v1/config
    let cfg_req = ConfigUpdateRequest {
        config_key: Some("max_gross_leverage_x".to_string()),
        config_value: Some(serde_json::json!("2.40")),
        description: None,
        updates: None,
    };
    let res_cfg = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&cfg_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_cfg.status(), StatusCode::OK);

    // 6. POST /api/v1/positions/close (cancels working order and applies manual cooldown)
    let close_req = ClosePositionRequest {
        position_id: None,
        symbol: Some("BTC-USDC".to_string()),
        cooldown_sec: Some(600),
        reason: Some("TEST_WEB_CLOSE".to_string()),
        close_all: false,
    };
    let res_close = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/positions/close")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&close_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_close.status(), StatusCode::OK);
    let close_bytes = axum::body::to_bytes(res_close.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let close_json: Value = serde_json::from_slice(&close_bytes).unwrap();
    assert_eq!(close_json["new_status"], "CANCELLED");
    assert_eq!(close_json["cooldown_applied_sec"], 600);
}

#[tokio::test]
async fn test_web_auth_enforced_on_get_and_mutating_routes_and_csp() {
    let db = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "true", None)
            .unwrap();
        mgr.set_config_value("web_api_token", "aegis-secret-token-123", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();

    // Non-loopback bind with token also fails closed to require_bearer_auth=true
    let app = create_web_router_for_host(db.clone(), None, Some("0.0.0.0"));

    // 1. Static / and /api/v1/health remain reachable, CSP omits 'unsafe-inline' in script-src
    let res_idx = app
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res_idx.status(), StatusCode::OK);
    let csp = res_idx
        .headers()
        .get(header::CONTENT_SECURITY_POLICY)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(csp.contains("script-src 'self';"));
    assert!(!DEFAULT_CSP_POLICY.contains("script-src 'self' 'unsafe-inline'"));

    let res_health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_health.status(), StatusCode::OK);

    // 2. Unauthenticated GET /api/v1/dashboard, /api/v1/state, /api/v1/stream, and POST return 401
    for uri in ["/api/v1/dashboard", "/api/v1/state", "/api/v1/stream"] {
        let res = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            StatusCode::UNAUTHORIZED,
            "Expected 401 for {uri}"
        );
    }

    let res_post_noauth = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/risk-tier")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"risk_tier":"standard"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_post_noauth.status(), StatusCode::UNAUTHORIZED);

    // 3. Invalid login -> 401; Valid login -> 200 + Set-Cookie; Bearer & Cookie unlock GET routes
    let bad_login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&AuthLoginPayload {
                        token: "wrong-token".to_string(),
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad_login.status(), StatusCode::UNAUTHORIZED);

    let ok_login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&AuthLoginPayload {
                        token: "aegis-secret-token-123".to_string(),
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok_login.status(), StatusCode::OK);
    let set_cookie = ok_login
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let cookie_kv = set_cookie.split(';').next().unwrap().to_string();

    let res_dash_bearer = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::AUTHORIZATION, "Bearer aegis-secret-token-123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_dash_bearer.status(), StatusCode::OK);

    let res_state_cookie = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/state")
                .header(header::COOKIE, &cookie_kv)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_state_cookie.status(), StatusCode::OK);

    let res_stream_bearer = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/stream")
                .header(header::AUTHORIZATION, "Bearer aegis-secret-token-123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_stream_bearer.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_web_csrf_origin_mismatch_rejected() {
    let db = tokio::task::spawn_blocking(new_test_db_noauth)
        .await
        .unwrap();
    let app = create_web_router(db, None);

    // Cross-origin Origin -> 403 Forbidden
    let res_bad_origin = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/risk-tier")
                .header(header::HOST, "127.0.0.1:8000")
                .header(header::ORIGIN, "https://evil.example")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"risk_tier":"standard"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_bad_origin.status(), StatusCode::FORBIDDEN);

    // Cross-origin Referer -> 403 Forbidden
    let res_bad_referer = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/alerts/test")
                .header(header::HOST, "127.0.0.1:8000")
                .header(header::REFERER, "https://evil.example/csrf.html")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_bad_referer.status(), StatusCode::FORBIDDEN);

    // Matching Origin == Host -> 200 OK
    let res_same_origin = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/risk-tier")
                .header(header::HOST, "127.0.0.1:8000")
                .header(header::ORIGIN, "http://127.0.0.1:8000")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"risk_tier":"standard"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_same_origin.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_web_health_503_and_config_validation() {
    // 1. Verify health 503 error reporting when MariaDB fails
    let err_resp = health_result_to_response(Err(AegisError::Database(
        "MariaDB connection lost".to_string(),
    )));
    assert_eq!(err_resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = axum::body::to_bytes(err_resp.into_body(), 64 * 1024)
        .await
        .unwrap();
    let json_body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json_body["status"], "unavailable");

    // 2. Verify config validation rejects dangerous keys, unknown keys, out-of-range bounds, and raw gAAAAAB strings
    assert!(validate_config_key_value("'alert(1)", "1").is_err());
    assert!(validate_config_key_value("unknown_arbitrary_key", "val").is_err());
    assert!(validate_config_key_value("web_require_auth", "false").is_err());
    assert!(validate_config_key_value("max_open_positions", "999").is_err());
    assert!(validate_config_key_value("max_gross_leverage_x", "-1.0").is_err());
    assert!(validate_config_key_value("execution_mode", "INVALID").is_err());
    assert!(validate_config_key_value("private_key", "gAAAAABrawinvalid").is_err());
    assert!(validate_config_key_value("risk_capital_cap_usdc", "1000.0").is_ok());
    assert!(validate_config_key_value("profit_gate_signal_exits", "true").is_ok());
    assert!(validate_config_key_value("profit_gate_signal_exits", "false").is_ok());
    assert!(validate_config_key_value("profit_gate_signal_exits", "invalid").is_err());
    assert!(validate_config_key_value("signal_exit_min_net_profit_pct", "0.0025").is_ok());
    assert!(validate_config_key_value("signal_exit_min_net_profit_pct", "-0.01").is_err());
    assert!(validate_config_key_value("signal_exit_min_net_profit_pct", "0.60").is_err());
}

#[tokio::test]
async fn test_web_auth_explicit_disable_on_non_loopback_respects_operator_opt_out() {
    // 1. When web_auth_enabled is unset and a token exists on non-loopback, fail closed (401).
    let db_default = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_api_token", "aegis-secret-token-123", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app_default = create_web_router_for_host(db_default, None, Some("0.0.0.0"));
    let res_default = app_default
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_default.status(), StatusCode::UNAUTHORIZED);

    // 2. When web_auth_enabled is explicitly "false" in DB (immutable via HTTP), allow direct access.
    let db_disabled = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_api_token", "aegis-secret-token-123", None)
            .unwrap();
        mgr.set_config_value("web_auth_enabled", "false", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app_disabled = create_web_router_for_host(db_disabled, None, Some("0.0.0.0"));
    let res_disabled = app_disabled
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_disabled.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_web_dashboard_separates_pre_cap_and_active_cap_holdings() {
    let cap_epoch_ms = now_ms() - 600_000;
    let db = tokio::task::spawn_blocking(move || {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        mgr.set_config_value("max_open_positions", "6", None)
            .unwrap();
        mgr.set_config_value(
            "risk_capital_cap_updated_at_ms",
            &cap_epoch_ms.to_string(),
            None,
        )
        .unwrap();

        let coins = [
            ("ETH", 0, 3200.0, Some(cap_epoch_ms - 120_000)),
            ("AVAX", 1, 35.0, Some(cap_epoch_ms - 1)),
            ("BTC", 2, 94000.0, Some(cap_epoch_ms)),
            ("SOL", 3, 180.0, Some(cap_epoch_ms + 10_000)),
            ("ARB", 4, 1.10, Some(cap_epoch_ms + 20_000)),
            ("SUI", 5, 3.40, Some(cap_epoch_ms + 30_000)),
            ("LINK", 6, 18.50, Some(cap_epoch_ms + 40_000)),
            ("DOGE", 7, 0.38, Some(cap_epoch_ms + 50_000)),
        ];

        for (coin, idx, px, opened_at) in coins {
            let asset_id = mgr
                .upsert_exchange_asset(&AssetUpsertInput {
                    raw_symbol_or_coin: coin.to_string(),
                    asset_index: idx,
                    sz_decimals: 3,
                    tick_size: 0.01,
                    lot_step_size: 0.01,
                    max_leverage: 25.0,
                    mark_price: px,
                    mid_price: px,
                    oracle_price: px,
                    funding_rate_hourly: 0.00001,
                    day_volume_usdc: 40_000_000.0,
                    open_interest_usdc: 10_000_000.0,
                    prev_day_price: px * 0.99,
                    is_core_eligible: true,
                    is_event_eligible: true,
                    is_priority_watched: false,
                })
                .unwrap();

            mgr.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
                asset_id,
                coin,
                side: OrderSide::Long,
                entry_price: px,
                mark_price: px,
                quantity: 150.0 / px,
                committed_notional_usdc: 150.0,
                unrealized_pnl_usdc: 0.0,
                portfolio_weight: 0.15,
                protective_stop_px: px * 0.95,
                take_profit_px: px * 1.10,
                funding_accrued_usdc: 0.0,
                opened_at_ms: opened_at,
            })
            .unwrap();
        }

        mgr
    })
    .await
    .unwrap();

    let app = create_web_router(db.clone(), None);

    // 1. TESTNET: 6 active-cap holdings (>= cap_epoch_ms) and 2 pre-cap holdings (< cap_epoch_ms)
    let res_dash = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_dash.status(), StatusCode::OK);
    let dash_bytes = axum::body::to_bytes(res_dash.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let dash_json: Value = serde_json::from_slice(&dash_bytes).unwrap();
    assert_eq!(dash_json["v_assets_held"].as_array().unwrap().len(), 8);
    assert_eq!(dash_json["risk_capital_cap_updated_at_ms"], cap_epoch_ms);
    assert_eq!(dash_json["max_open_positions"], 6);
    assert_eq!(dash_json["active_cap_open_count"], 6);
    assert_eq!(dash_json["pre_cap_open_count"], 2);

    // 2. PAPER mode disables cap epoch isolation even if risk_capital_cap_updated_at_ms > 0 in DB
    let db_paper = db.clone();
    tokio::task::spawn_blocking(move || {
        db_paper
            .set_config_value("execution_mode", "PAPER", None)
            .unwrap();
    })
    .await
    .unwrap();

    let res_paper = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_paper.status(), StatusCode::OK);
    let paper_bytes = axum::body::to_bytes(res_paper.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let paper_json: Value = serde_json::from_slice(&paper_bytes).unwrap();
    assert_eq!(paper_json["risk_capital_cap_updated_at_ms"], 0);
    assert_eq!(paper_json["max_open_positions"], 6);
    assert_eq!(paper_json["active_cap_open_count"], 8);
    assert_eq!(paper_json["pre_cap_open_count"], 0);
}

#[tokio::test]
async fn test_web_auth_fail_closed_and_kvm_bridge_opt_out() {
    // 1. Empty token with web_auth_enabled=true fails closed (401 on routes, 503 on login, startup Err)
    let db_empty = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "true", None)
            .unwrap();
        mgr.set_config_value("web_api_token", "", None).unwrap();
        mgr
    })
    .await
    .unwrap();

    let app_empty = create_web_router(db_empty.clone(), None);
    let res_empty_dash = app_empty
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_empty_dash.status(), StatusCode::UNAUTHORIZED);

    let res_empty_login = app_empty
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":""}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_empty_login.status(), StatusCode::SERVICE_UNAVAILABLE);

    let (_sd_tx1, sd_rx1) = tokio::sync::watch::channel(false);
    let err_empty = serve_web_with_shutdown(db_empty, "127.0.0.1", 0, sd_rx1).await;
    assert!(matches!(err_empty, Err(AegisError::SecurityViolation(_))));

    // 2. Corrupt encrypted web_api_token with web_auth_enabled unset fails closed
    let db_corrupt = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        let mut conn = mgr.get_conn().unwrap();
        let ts = now_ms();
        conn.exec_drop(
            r#"INSERT INTO system_config
               (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
               VALUES ('web_api_token', 'gAAAAABcorrupt_cipher', 'secret', 'bad', :ts, :ts)
               ON DUPLICATE KEY UPDATE config_value = VALUES(config_value)"#,
            params! { "ts" => ts },
        )
        .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app_corrupt = create_web_router(db_corrupt.clone(), None);
    let res_corrupt = app_corrupt
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_corrupt.status(), StatusCode::UNAUTHORIZED);

    let res_corrupt_login = app_corrupt
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"anything"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_corrupt_login.status(), StatusCode::SERVICE_UNAVAILABLE);

    let (_sd_tx2, sd_rx2) = tokio::sync::watch::channel(false);
    let err_corrupt = serve_web_with_shutdown(db_corrupt, "127.0.0.1", 0, sd_rx2).await;
    assert!(matches!(err_corrupt, Err(AegisError::SecurityViolation(_))));

    // 3. Unset web_auth_enabled + unset token on non-loopback 0.0.0.0 fails closed
    let db_unset_remote = tokio::task::spawn_blocking(new_test_db).await.unwrap();

    let app_unset_remote =
        create_web_router_for_host(db_unset_remote.clone(), None, Some("0.0.0.0"));
    let res_unset_remote = app_unset_remote
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_unset_remote.status(), StatusCode::UNAUTHORIZED);

    let (_sd_tx3, sd_rx3) = tokio::sync::watch::channel(false);
    let err_remote = serve_web_with_shutdown(db_unset_remote, "0.0.0.0", 0, sd_rx3).await;
    assert!(matches!(err_remote, Err(AegisError::SecurityViolation(_))));

    // 4. Loopback bind with web_api_token configured enforces auth even when web_auth_enabled is unset
    let db_loopback_tok = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_api_token", "loopback-tok-99", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app_loopback_tok = create_web_router_for_host(db_loopback_tok, None, Some("127.0.0.1"));
    let res_lb_noauth = app_loopback_tok
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_lb_noauth.status(), StatusCode::UNAUTHORIZED);

    let res_lb_auth = app_loopback_tok
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::AUTHORIZATION, "Bearer loopback-tok-99")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_lb_auth.status(), StatusCode::OK);

    // 5. KVM bridge: explicit DB-immutable web_auth_enabled=false on 0.0.0.0 + Host 192.168.122.254:8000 stays 200
    let db_kvm = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "false", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app_kvm = create_web_router_for_host(db_kvm, None, Some("0.0.0.0"));
    let res_kvm_get = app_kvm
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::HOST, "192.168.122.254:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_kvm_get.status(), StatusCode::OK);

    let res_kvm_post = app_kvm
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/risk-tier")
                .header(header::HOST, "192.168.122.254:8000")
                .header(header::ORIGIN, "http://192.168.122.254:8000")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"risk_tier":"standard"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_kvm_post.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_web_dns_rebinding_host_allowlist_and_ipv6() {
    let db_kvm = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "false", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app_kvm = create_web_router_for_host(db_kvm, None, Some("0.0.0.0"));

    // Untrusted Host on GET and POST -> 403 Forbidden (blocks DNS rebinding)
    let res_rebind_get = app_kvm
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::HOST, "evil.example:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_rebind_get.status(), StatusCode::FORBIDDEN);

    let res_rebind_health = app_kvm
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .header(header::HOST, "evil.example:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_rebind_health.status(), StatusCode::FORBIDDEN);

    let res_rebind_post = app_kvm
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/risk-tier")
                .header(header::HOST, "evil.example:8000")
                .header(header::ORIGIN, "http://evil.example:8000")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"risk_tier":"standard"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_rebind_post.status(), StatusCode::FORBIDDEN);

    // Missing Host with Origin -> 403 Forbidden
    let res_no_host_origin = app_kvm
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::ORIGIN, "http://127.0.0.1:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_no_host_origin.status(), StatusCode::FORBIDDEN);

    // Bracketed IPv6 loopback -> 200 OK
    let res_ipv6 = app_kvm
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::HOST, "[::1]:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_ipv6.status(), StatusCode::OK);

    // Explicit CSV web_allowed_hosts disables wildcard IP literals
    let db_allow = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "false", None)
            .unwrap();
        mgr.set_config_value("web_allowed_hosts", "aegis.lan:8000", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();
    let app_allow = create_web_router_for_host(db_allow, None, Some("0.0.0.0"));

    let res_listed = app_allow
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::HOST, "aegis.lan:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_listed.status(), StatusCode::OK);

    let res_unlisted_ip = app_allow
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::HOST, "192.168.122.254:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_unlisted_ip.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_web_session_channel_separation_rotation_and_expiry() {
    let db = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "true", None)
            .unwrap();
        mgr.set_config_value("web_api_token", "api-master-secret-20", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();

    let state = AppState::new_with_bind_host(db, None, Some("127.0.0.1"));
    let app = build_router(state.clone());

    // 1. Login -> receive cookie 1
    let login1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&AuthLoginPayload {
                        token: "api-master-secret-20".to_string(),
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login1.status(), StatusCode::OK);
    let set_cookie1 = login1
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let cookie1_kv = set_cookie1.split(';').next().unwrap().to_string();
    let tok1 = cookie1_kv
        .strip_prefix("aegis_session=")
        .unwrap()
        .to_string();

    // 2. Session cookie rejected in Bearer / X-Aegis-Token headers
    let res_cookie_as_bearer = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::AUTHORIZATION, format!("Bearer {tok1}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_cookie_as_bearer.status(), StatusCode::UNAUTHORIZED);

    let res_cookie_as_xtok = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header("x-aegis-token", &tok1)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_cookie_as_xtok.status(), StatusCode::UNAUTHORIZED);

    // API token rejected when sent directly as session cookie
    let res_api_as_cookie = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::COOKIE, "aegis_session=api-master-secret-20")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_api_as_cookie.status(), StatusCode::UNAUTHORIZED);

    // 3. Re-login rotates session and revokes prior cookie
    let login2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header(header::HOST, "127.0.0.1:8000")
                .header(header::ORIGIN, "http://127.0.0.1:8000")
                .header(header::COOKIE, &cookie1_kv)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&AuthLoginPayload {
                        token: "api-master-secret-20".to_string(),
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login2.status(), StatusCode::OK);
    let set_cookie2 = login2
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let cookie2_kv = set_cookie2.split(';').next().unwrap().to_string();
    assert_ne!(cookie1_kv, cookie2_kv);

    let res_old_cookie = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::COOKIE, &cookie1_kv)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_old_cookie.status(), StatusCode::UNAUTHORIZED);

    let res_new_cookie = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::COOKIE, &cookie2_kv)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_new_cookie.status(), StatusCode::OK);

    // 4. Expired session is rejected and evicted
    {
        let mut guard = state.sessions.lock();
        let past = Instant::now() - Duration::from_secs(5);
        for exp in guard.values_mut() {
            *exp = past;
        }
    }
    let res_expired = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::COOKIE, &cookie2_kv)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_expired.status(), StatusCode::UNAUTHORIZED);
    assert!(state.sessions.lock().is_empty());
}

#[tokio::test]
async fn test_web_cookie_secure_and_immutable_proxy_headers() {
    assert!(matches!(
        validate_config_key_value("web_trust_proxy_headers", "true"),
        Err(AegisError::SecurityViolation(_))
    ));
    assert!(matches!(
        validate_config_key_value("web_cookie_secure", "true"),
        Err(AegisError::SecurityViolation(_))
    ));
    assert!(matches!(
        validate_config_key_value("allow_web_live_mode", "true"),
        Err(AegisError::SecurityViolation(_))
    ));

    let db_plain = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "true", None)
            .unwrap();
        mgr.set_config_value("web_api_token", "tok-cookie-30", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();
    let app_plain = create_web_router(db_plain, None);

    // Mutating web_trust_proxy_headers via Web API -> 403 Forbidden
    let res_mut_proxy = app_plain
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header(header::AUTHORIZATION, "Bearer tok-cookie-30")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"config_key":"web_trust_proxy_headers","config_value":"true"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_mut_proxy.status(), StatusCode::FORBIDDEN);

    // Plain HTTP login omits ; Secure
    let login_http = app_plain
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header(header::HOST, "127.0.0.1:8000")
                .header(header::ORIGIN, "http://127.0.0.1:8000")
                .header("x-forwarded-proto", "https")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"tok-cookie-30"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login_http.status(), StatusCode::OK);
    let cookie_http = login_http
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(!cookie_http.contains("; Secure"));

    // Origin https:// sets ; Secure
    let login_https_origin = app_plain
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header(header::HOST, "127.0.0.1:8000")
                .header(header::ORIGIN, "https://127.0.0.1:8000")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"tok-cookie-30"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login_https_origin.status(), StatusCode::OK);
    let cookie_https = login_https_origin
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(cookie_https.contains("; Secure"));

    // Trusted proxy headers honor X-Forwarded-Proto: https ONLY when peer matches web_trusted_proxies
    let db_proxy = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "true", None)
            .unwrap();
        mgr.set_config_value("web_api_token", "tok-cookie-30", None)
            .unwrap();
        mgr.set_config_value("web_trust_proxy_headers", "true", None)
            .unwrap();
        mgr.set_config_value("web_trusted_proxies", "127.0.0.1/32", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();
    let app_proxy = create_web_router(db_proxy, None);

    let trusted_peer: SocketAddr = "127.0.0.1:43210".parse().unwrap();
    let login_xfp = app_proxy
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .extension(ConnectInfo(trusted_peer))
                .header("x-forwarded-proto", "https")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"tok-cookie-30"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login_xfp.status(), StatusCode::OK);
    let cookie_xfp = login_xfp
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(cookie_xfp.contains("; Secure"));
}

#[tokio::test]
async fn test_web_auth_rate_limiting_429_and_reset() {
    let db = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "true", None)
            .unwrap();
        mgr.set_config_value("web_api_token", "real-token-36", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app = create_web_router(db.clone(), None);

    // 1. Credential-less probes never trigger 429 rate limiting
    for _ in 0..8 {
        let res_probe = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/dashboard")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res_probe.status(), StatusCode::UNAUTHORIZED);
    }

    // 2. 4 bad logins followed by 1 valid login resets failure bucket
    for _ in 0..4 {
        let res_bad = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/auth/login")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"token":"bad-guess"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res_bad.status(), StatusCode::UNAUTHORIZED);
    }

    let res_reset = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"real-token-36"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_reset.status(), StatusCode::OK);

    // 3. 5 consecutive failed logins trigger 429 + Retry-After on subsequent attempt (even with valid token)
    for _ in 0..5 {
        let res_fail = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/auth/login")
                    .header("x-forwarded-for", "1.2.3.4") // Ignored because trust_proxy_headers=false
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"token":"bad-guess"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res_fail.status(), StatusCode::UNAUTHORIZED);
    }

    let res_locked = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("x-forwarded-for", "9.9.9.9") // Spoofed XFF ignored
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"real-token-36"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_locked.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after = res_locked
        .headers()
        .get(header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(retry_after >= 1);

    // Middleware bearer brute-force while locked out also returns 429
    let res_bearer_locked = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .header(header::AUTHORIZATION, "Bearer wrong-guess")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_bearer_locked.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn test_web_execution_mode_transition_guards() {
    let db = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("execution_mode", "PAPER", None)
            .unwrap();
        let btc_id = mgr
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "BTC".to_string(),
                asset_index: 0,
                sz_decimals: 4,
                tick_size: 0.1,
                lot_step_size: 0.001,
                max_leverage: 40.0,
                mark_price: 94000.0,
                mid_price: 94000.0,
                oracle_price: 94000.0,
                funding_rate_hourly: 0.0,
                day_volume_usdc: 50_000_000.0,
                open_interest_usdc: 20_000_000.0,
                prev_day_price: 93000.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: false,
            })
            .unwrap();
        mgr.create_working_order(&WorkingOrderInput {
            asset_id: btc_id,
            client_order_id: "mode-guard-oid-1".to_string(),
            side: OrderSide::Long,
            limit_price: 93900.0,
            requested_quantity: 0.02,
            reserved_margin_usdc: 1878.0,
            portfolio_weight: 0.18,
            s1_weight_contrib: 0.18,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 90000.0,
            take_profit_px: None,
            exchange_order_id: None,
            reason: "TEST_GUARD".to_string(),
        })
        .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app = create_web_router(db.clone(), None);

    // 1. Active WORKING order blocks PAPER -> TESTNET (400 Bad Request)
    let res_active_block = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"config_key":"execution_mode","config_value":"TESTNET"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_active_block.status(), StatusCode::BAD_REQUEST);

    // Cancel working order so portfolio is flat
    let res_close = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/positions/close")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&ClosePositionRequest {
                        close_all: true,
                        ..Default::default()
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_close.status(), StatusCode::OK);

    // 2. -> LIVE blocked (403 Forbidden) without DB-immutable allow_web_live_mode=true
    let res_live_blocked = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"config_key":"execution_mode","config_value":"LIVE"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_live_blocked.status(), StatusCode::FORBIDDEN);

    // 3. Flat PAPER -> testnet (lowercase) succeeds, normalizes to TESTNET, and syncs hyperliquid_testnet=true
    let res_to_testnet = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"config_key":"execution_mode","config_value":"testnet"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_to_testnet.status(), StatusCode::OK);
    let cfg_testnet = db.load_config().unwrap();
    assert_eq!(cfg_testnet.get_str("execution_mode", ""), "TESTNET");
    assert_eq!(cfg_testnet.get_str("hyperliquid_testnet", ""), "true");

    // 4. Flat TESTNET -> paper succeeds and keeps hyperliquid_testnet=true
    let res_to_paper = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"config_key":"execution_mode","config_value":"paper"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_to_paper.status(), StatusCode::OK);
    let cfg_paper = db.load_config().unwrap();
    assert_eq!(cfg_paper.get_str("execution_mode", ""), "PAPER");
    assert_eq!(cfg_paper.get_str("hyperliquid_testnet", ""), "true");

    // 5. With allow_web_live_mode=true in DB, flat -> live succeeds and sets hyperliquid_testnet=false
    let db_allow_live = db.clone();
    tokio::task::spawn_blocking(move || {
        db_allow_live
            .set_config_value("allow_web_live_mode", "true", None)
            .unwrap();
    })
    .await
    .unwrap();

    let res_to_live = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"config_key":"execution_mode","config_value":"live"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_to_live.status(), StatusCode::OK);
    let cfg_live = db.load_config().unwrap();
    assert_eq!(cfg_live.get_str("execution_mode", ""), "LIVE");
    assert_eq!(cfg_live.get_str("hyperliquid_testnet", ""), "false");
}

#[tokio::test]
async fn test_web_discord_webhook_ssrf_rejected_and_uppercase_config_normalized() {
    // 1. SSRF Discord webhook URLs rejected (#19)
    for bad_url in [
        "https://127.0.0.1/api/webhooks/123/abc",
        "https://169.254.169.254/latest/meta-data",
        "https://discord.com@evil.com/api/webhooks/123/abc",
        "http://discord.com/api/webhooks/123/abc",
        "https://discord.com:8443/api/webhooks/123/abc",
        "https://evil.example/api/webhooks/123/abc",
    ] {
        assert!(
            validate_config_key_value("discord_webhook_url", bad_url).is_err(),
            "Expected SSRF rejection for {bad_url}"
        );
    }
    assert!(validate_config_key_value(
        "discord_webhook_url",
        "https://discord.com/api/webhooks/123456789/valid_Token-abc"
    )
    .is_ok());
    assert!(validate_config_key_value("discord_webhook_url", "").is_ok());

    let db = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("risk_capital_cap_usdc", "1000.0", None)
            .unwrap();
        mgr.set_config_value("risk_capital_cap_updated_at_ms", "100", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();
    let app = create_web_router(db.clone(), None);

    // POST /api/v1/config with SSRF URL -> 400 Bad Request
    let res_ssrf = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"config_key":"discord_webhook_url","config_value":"https://169.254.169.254/api/webhooks/1/a"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_ssrf.status(), StatusCode::BAD_REQUEST);

    // 2. Uppercase RISK_CAPITAL_CAP_USDC normalizes to lowercase and updates epoch (#26)
    let res_cap = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"config_key":"RISK_CAPITAL_CAP_USDC","config_value":"2500.0"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_cap.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(res_cap.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body_json: Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(body_json["updated_keys"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v == "risk_capital_cap_usdc"));

    let cfg = db.load_config().unwrap();
    assert_eq!(cfg.get_str("risk_capital_cap_usdc", ""), "2500.0");
    assert!(cfg.get_i64("risk_capital_cap_updated_at_ms", 0) > 100);
}

#[tokio::test]
async fn test_web_vault_info_contract_readonly_config_and_pnl_percentages() {
    use aegis_core::{VaultInfoPayload, VaultPositionInfo};
    use aegis_db::ReconciledClosedTradeInput;

    // 1. Direct writes to live_vault_info_json are rejected as SecurityViolation (403 Forbidden)
    assert!(matches!(
        validate_config_key_value("live_vault_info_json", "{}"),
        Err(AegisError::SecurityViolation(_))
    ));
    assert!(matches!(
        validate_config_key_value("LIVE_VAULT_INFO_JSON", "{}"),
        Err(AegisError::SecurityViolation(_))
    ));

    let now = now_ms();
    let locked_future = now + 86_400_000;
    let locked_past = now - 86_400_000;
    let hlp_addr = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303";

    let db = tokio::task::spawn_blocking(move || {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        mgr.set_config_value("live_exchange_equity_usdc", "629.84", None)
            .unwrap();
        mgr.set_config_value("hyperliquid_vault_address", hlp_addr, None)
            .unwrap();

        let sol_id = mgr
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "SOL".to_string(),
                asset_index: 2,
                sz_decimals: 2,
                tick_size: 0.01,
                lot_step_size: 0.01,
                max_leverage: 20.0,
                mark_price: 186.17,
                mid_price: 186.17,
                oracle_price: 186.17,
                funding_rate_hourly: 0.00001,
                day_volume_usdc: 50_000_000.0,
                open_interest_usdc: 20_000_000.0,
                prev_day_price: 180.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();

        let met_id = mgr
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "MET".to_string(),
                asset_index: 195,
                sz_decimals: 0,
                tick_size: 0.00001,
                lot_step_size: 1.0,
                max_leverage: 3.0,
                mark_price: 0.19258,
                mid_price: 0.19258,
                oracle_price: 0.19258,
                funding_rate_hourly: 0.00001,
                day_volume_usdc: 5_000_000.0,
                open_interest_usdc: 1_000_000.0,
                prev_day_price: 0.23,
                is_core_eligible: false,
                is_event_eligible: true,
                is_priority_watched: false,
            })
            .unwrap();

        // Open SOL position: entry=180.0, qty=2.0 (entry notional=360.0), uPnL=+12.34 -> +3.43%
        mgr.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sol_id,
            coin: "SOL",
            side: OrderSide::Long,
            entry_price: 180.0,
            mark_price: 186.17,
            quantity: 2.0,
            committed_notional_usdc: 372.34,
            unrealized_pnl_usdc: 12.34,
            portfolio_weight: 0.59,
            protective_stop_px: 172.0,
            take_profit_px: 195.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now - 600_000),
        })
        .unwrap();

        // Closed MET position in MariaDB with committed_notional_usdc == 0.0!
        // entry=0.23227, qty=9634.0 (entry notional=2237.69), realized_pnl=-382.37 -> -17.09%
        mgr.reconcile_closed_positions_from_exchange_fills(&[ReconciledClosedTradeInput {
            asset_id: met_id,
            client_order_id: "hl-closed-met-9001".to_string(),
            exchange_order_id: "hl-9001".to_string(),
            side: OrderSide::Long,
            entry_price: 0.23227,
            exit_price: 0.19258,
            filled_quantity: 9634.0,
            realized_pnl_usdc: -382.37,
            fees_paid_usdc: 1.25,
            funding_accrued_usdc: -0.40,
            created_at_ms: now - 3_600_000,
            closed_at_ms: now - 60_000,
            exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
            close_epoch_id: None,
        }])
        .unwrap();

        // Cancelled unfilled order should NOT appear in recent_closed_positions
        let cancelled_pid = mgr
            .create_working_order(&WorkingOrderInput {
                asset_id: met_id,
                client_order_id: "cancelled-alo-1".to_string(),
                side: OrderSide::Short,
                limit_price: 0.20,
                requested_quantity: 100.0,
                reserved_margin_usdc: 20.0,
                portfolio_weight: -0.05,
                s1_weight_contrib: 0.0,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: -0.05,
                protective_stop_px: 0.22,
                take_profit_px: None,
                exchange_order_id: None,
                reason: "TEST_CANCEL".to_string(),
            })
            .unwrap();
        mgr.cancel_working_order(cancelled_pid, "TEST_CANCELLED")
            .unwrap();

        mgr.insert_portfolio_snapshot(&SnapshotInsertInput {
            total_equity_usdc: 629.84,
            uncapped_equity_usdc: 629.84,
            available_cash_usdc: 428.58,
            reserved_order_cash_usdc: 0.0,
            committed_notional_usdc: 372.34,
            net_notional_usdc: 372.34,
            gross_leverage_x: 0.591,
            net_leverage_x: 0.591,
            market_beta_exposure: 0.5,
            market_breadth_bull: 0.5,
            market_regime_label: "NEUTRAL".to_string(),
            risk_tier: "extreme".to_string(),
            leverage_multiplier: 1.85,
            s1_gross_weight: 0.59,
            s2_gross_weight: 0.0,
            s3_gross_weight: 0.0,
            working_order_count: 0,
            open_long_count: 1,
            open_short_count: 0,
            eligible_coin_count: 2,
        })
        .unwrap();

        mgr
    })
    .await
    .unwrap();

    let app = create_web_router(db.clone(), None);

    // 2. POST /api/v1/config rejects direct and batch writes to live_vault_info_json with 403
    for payload in [
        r#"{"config_key":"live_vault_info_json","config_value":"{}"}"#,
        r#"{"updates":{"live_vault_info_json":"{}"}}"#,
    ] {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/config")
                    .header("content-type", "application/json")
                    .body(Body::from(payload))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }

    // 3. Before vault sync, /api/v1/dashboard returns vault_info with zeros + vaults: []
    let res_pre = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_pre.status(), StatusCode::OK);
    let pre_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_pre.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(pre_json["vault_info"]["configured_vault_address"], hlp_addr);
    assert_eq!(pre_json["vault_info"]["total_vault_equity_usdc"], 0.0);
    assert!(
        (pre_json["vault_info"]["combined_portfolio_equity_usdc"]
            .as_f64()
            .unwrap()
            - 629.84)
            .abs()
            < 1e-6
    );
    assert_eq!(pre_json["vault_info"]["vault_count"], 0);
    assert_eq!(pre_json["vault_info"]["synced_at_ms"], 0);
    assert_eq!(
        pre_json["vault_info"]["vaults"].as_array().unwrap().len(),
        0
    );

    // Verify open holding unrealized_pnl_pct and closed position realized_pnl_pct
    let held = pre_json["v_assets_held"].as_array().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0]["symbol"], "SOL-USDC");
    assert!((held[0]["unrealized_pnl_pct"].as_f64().unwrap() - 3.43).abs() < 1e-6);

    let closed = pre_json["recent_closed_positions"].as_array().unwrap();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0]["symbol"], "MET-USDC");
    assert_eq!(closed[0]["coin"], "MET");
    assert!((closed[0]["entry_price"].as_f64().unwrap() - 0.23227).abs() < 1e-6);
    assert!((closed[0]["exit_price"].as_f64().unwrap() - 0.19258).abs() < 1e-6);
    assert!((closed[0]["quantity"].as_f64().unwrap() - 9634.0).abs() < 1e-6);
    assert!((closed[0]["filled_quantity"].as_f64().unwrap() - 9634.0).abs() < 1e-6);
    assert!((closed[0]["realized_pnl_usdc"].as_f64().unwrap() - (-382.37)).abs() < 1e-4);
    assert!((closed[0]["realized_pnl_pct"].as_f64().unwrap() - (-17.09)).abs() < 1e-6);

    // 4. Persist synced vault info snapshot internally and verify /api/v1/dashboard + /api/v1/state
    let synced_payload = VaultInfoPayload {
        configured_vault_address: hlp_addr.to_string(),
        total_vault_equity_usdc: 202.40,
        combined_portfolio_equity_usdc: 832.24,
        vault_count: 2,
        synced_at_ms: now,
        vaults: vec![
            VaultPositionInfo {
                vault_address: hlp_addr.to_string(),
                name: "Hyperliquidity Provider (HLP)".to_string(),
                leader: "0x677d831aef5328190852e24f13c46cac05f984e7".to_string(),
                equity_usdc: 202.40,
                pnl_usdc: 0.92,
                all_time_pnl_usdc: 1.34,
                pnl_pct: 0.46,
                all_time_pnl_pct: 0.67,
                apr_pct: 8.14,
                days_following: 25,
                locked_until_ms: locked_past,
                is_locked: true, // stale flag should be recomputed dynamically to false!
                is_closed: false,
                ..Default::default()
            },
            VaultPositionInfo {
                vault_address: "0x3333333333333333333333333333333333333333".to_string(),
                name: "Locked Vault".to_string(),
                leader: "0x4444444444444444444444444444444444444444".to_string(),
                equity_usdc: 0.0,
                pnl_usdc: 0.0,
                all_time_pnl_usdc: 0.0,
                pnl_pct: 0.0,
                all_time_pnl_pct: 0.0,
                apr_pct: 4.2,
                days_following: 1,
                locked_until_ms: locked_future,
                is_locked: false, // stale flag should be recomputed dynamically to true!
                is_closed: false,
                ..Default::default()
            },
        ],
    };
    let db_sync = db.clone();
    tokio::task::spawn_blocking(move || {
        db_sync
            .set_config_value(
                "live_vault_info_json",
                &serde_json::to_string(&synced_payload).unwrap(),
                None,
            )
            .unwrap();
    })
    .await
    .unwrap();

    for uri in ["/api/v1/dashboard", "/api/v1/state"] {
        let res = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(res.into_body(), 4 * 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();

        // Perp equity strictly untouched
        assert!((body["snapshot"]["total_equity_usdc"].as_f64().unwrap() - 629.84).abs() < 1e-6);

        let vi = &body["vault_info"];
        assert_eq!(vi["configured_vault_address"], hlp_addr);
        assert!((vi["total_vault_equity_usdc"].as_f64().unwrap() - 202.40).abs() < 1e-6);
        assert!((vi["combined_portfolio_equity_usdc"].as_f64().unwrap() - 832.24).abs() < 1e-6);
        assert_eq!(vi["vault_count"], 2);
        assert_eq!(vi["synced_at_ms"], now);
        let vaults = vi["vaults"].as_array().unwrap();
        assert_eq!(vaults.len(), 2);
        assert_eq!(vaults[0]["vault_address"], hlp_addr);
        assert_eq!(vaults[0]["name"], "Hyperliquidity Provider (HLP)");
        assert_eq!(vaults[0]["is_locked"], false);
        assert_eq!(vaults[0]["is_closed"], false);
        assert_eq!(vaults[1]["is_locked"], true);
    }

    // 4b. Verify SSE broadcaster (/api/v1/stream) serializes the exact same vault_info & PnL % fields
    let sse_state = AppState::new(db.clone(), None);
    let mut sse_rx = sse_state.dashboard_rx.clone();
    sse_state.ensure_broadcaster_running();
    sse_state.notify_dashboard_refresh();
    let sse_payload_str = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let cur = sse_rx.borrow_and_update().clone();
            if let Some(snap) = cur {
                if !snap.json.is_empty() {
                    return snap.json;
                }
            }
            if sse_rx.changed().await.is_err() {
                break;
            }
        }
        std::sync::Arc::new(String::new())
    })
    .await
    .expect("SSE broadcaster emitted payload within 5s");
    sse_state.shutdown();
    let sse_json: Value = serde_json::from_str(&sse_payload_str).unwrap();
    assert!(
        (sse_json["vault_info"]["total_vault_equity_usdc"]
            .as_f64()
            .unwrap()
            - 202.40)
            .abs()
            < 1e-6
    );
    assert!(
        (sse_json["vault_info"]["combined_portfolio_equity_usdc"]
            .as_f64()
            .unwrap()
            - 832.24)
            .abs()
            < 1e-6
    );
    assert!(
        (sse_json["v_assets_held"][0]["unrealized_pnl_pct"]
            .as_f64()
            .unwrap()
            - 3.43)
            .abs()
            < 1e-6
    );
    assert!(
        (sse_json["recent_closed_positions"][0]["realized_pnl_pct"]
            .as_f64()
            .unwrap()
            - (-17.09))
            .abs()
            < 1e-6
    );

    // 5. In PAPER mode, vault_info always returns zeros and vaults: []
    let db_paper = db.clone();
    tokio::task::spawn_blocking(move || {
        db_paper
            .set_config_value("execution_mode", "PAPER", None)
            .unwrap();
    })
    .await
    .unwrap();

    let res_paper = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_paper.status(), StatusCode::OK);
    let paper_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_paper.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(paper_json["vault_info"]["total_vault_equity_usdc"], 0.0);
    assert_eq!(paper_json["vault_info"]["vault_count"], 0);
    assert_eq!(paper_json["vault_info"]["synced_at_ms"], 0);
    assert!(paper_json["vault_info"]["vaults"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(
        (paper_json["vault_info"]["combined_portfolio_equity_usdc"]
            .as_f64()
            .unwrap()
            - paper_json["snapshot"]["total_equity_usdc"]
                .as_f64()
                .unwrap())
        .abs()
            < 1e-6
    );
}

#[tokio::test]
async fn test_web_readonly_telemetry_keys_rejected_403() {
    let readonly_keys = [
        "live_exchange_equity_synced_at_ms",
        "live_exchange_margin_used_usdc",
        "live_exchange_withdrawable_usdc",
        "live_exchange_open_orders_count",
        "live_exchange_spot_usdc",
        "live_exchange_spot_synced_at_ms",
        "live_exchange_account_mode",
        "live_exchange_perp_margin_equity_usdc",
        "live_exchange_portfolio_pnl_json",
    ];

    for key in readonly_keys {
        assert!(
            matches!(
                validate_config_key_value(key, "123"),
                Err(AegisError::SecurityViolation(_))
            ),
            "Expected SecurityViolation for {key}"
        );
        let upper = key.to_ascii_uppercase();
        assert!(
            matches!(
                validate_config_key_value(&upper, "123"),
                Err(AegisError::SecurityViolation(_))
            ),
            "Expected SecurityViolation for {upper}"
        );
    }

    let db = tokio::task::spawn_blocking(new_test_db_noauth)
        .await
        .unwrap();
    let app = create_web_router(db, None);

    for key in readonly_keys {
        let single_body = serde_json::json!({
            "config_key": key,
            "config_value": "999",
        });
        let res_single = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/config")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&single_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res_single.status(),
            StatusCode::FORBIDDEN,
            "Expected 403 for single POST {key}"
        );

        let batch_body = serde_json::json!({
            "updates": {
                key: "999"
            }
        });
        let res_batch = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/config")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&batch_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res_batch.status(),
            StatusCode::FORBIDDEN,
            "Expected 403 for batch POST {key}"
        );
    }
}

#[tokio::test]
async fn test_web_option_b_equity_basis_spot_vault_and_stale_flag() {
    use aegis_core::{VaultInfoPayload, VaultPositionInfo};

    let now = now_ms();
    let synced_recent = now - 5_000;
    let hlp_addr = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303";

    let db = tokio::task::spawn_blocking(move || {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        mgr.set_config_value("live_exchange_equity_usdc", "10944.72", None)
            .unwrap();
        mgr.set_config_value("risk_capital_cap_usdc", "1000.0", None)
            .unwrap();
        mgr.set_config_value("live_exchange_spot_usdc", "5000.0", None)
            .unwrap();
        mgr.set_config_value("live_exchange_margin_used_usdc", "350.25", None)
            .unwrap();
        mgr.set_config_value("live_exchange_withdrawable_usdc", "8200.50", None)
            .unwrap();
        mgr.set_config_value(
            "live_exchange_equity_synced_at_ms",
            &synced_recent.to_string(),
            None,
        )
        .unwrap();

        let vault_payload = VaultInfoPayload {
            configured_vault_address: hlp_addr.to_string(),
            total_vault_equity_usdc: 202.40,
            combined_portfolio_equity_usdc: 16147.12,
            vault_count: 1,
            synced_at_ms: now,
            vaults: vec![VaultPositionInfo {
                vault_address: hlp_addr.to_string(),
                name: "HLP".to_string(),
                leader: "0x677d831aef5328190852e24f13c46cac05f984e7".to_string(),
                equity_usdc: 202.40,
                pnl_usdc: 0.92,
                all_time_pnl_usdc: 1.34,
                pnl_pct: 0.46,
                all_time_pnl_pct: 0.67,
                apr_pct: 8.14,
                days_following: 25,
                locked_until_ms: 0,
                is_locked: false,
                is_closed: false,
                ..Default::default()
            }],
        };
        mgr.set_config_value(
            "live_vault_info_json",
            &serde_json::to_string(&vault_payload).unwrap(),
            None,
        )
        .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app = create_web_router(db.clone(), None);

    // 1. When snapshot.id == 0 (synthesized), Option B caps snapshot.total_equity_usdc at 1000.0
    // while perp_wallet, spot, and vault combine cleanly to 16147.12.
    let res_synth = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_synth.status(), StatusCode::OK);
    let synth_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_synth.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();

    assert!(
        (synth_json["snapshot"]["total_equity_usdc"]
            .as_f64()
            .unwrap()
            - 1000.0)
            .abs()
            < 1e-6
    );
    let eb = &synth_json["equity_basis"];
    assert_eq!(eb["basis_label"], "CAPPED_RISK_CAPITAL");
    assert!((eb["effective_equity_usdc"].as_f64().unwrap() - 1000.0).abs() < 1e-6);
    assert!((eb["perp_wallet_equity_usdc"].as_f64().unwrap() - 10944.72).abs() < 1e-6);
    assert!((eb["risk_capital_cap_usdc"].as_f64().unwrap() - 1000.0).abs() < 1e-6);
    assert_eq!(eb["is_cap_binding"], true);
    assert!((eb["spot_usdc"].as_f64().unwrap() - 5000.0).abs() < 1e-6);
    assert!((eb["total_wallet_equity_usdc"].as_f64().unwrap() - 16147.12).abs() < 1e-6);
    assert!((eb["margin_used_usdc"].as_f64().unwrap() - 350.25).abs() < 1e-6);
    assert!((eb["withdrawable_usdc"].as_f64().unwrap() - 8200.50).abs() < 1e-6);
    assert_eq!(eb["is_stale"], false);
    assert!(
        (synth_json["vault_info"]["combined_portfolio_equity_usdc"]
            .as_f64()
            .unwrap()
            - 16147.12)
            .abs()
            < 1e-6
    );

    // 2. With a persisted Option B snapshot (snapshot.id > 0) and stale sync timestamp (>90s)
    let db_persisted = db.clone();
    let stale_ts = now - 120_000;
    tokio::task::spawn_blocking(move || {
        db_persisted
            .set_config_value(
                "live_exchange_equity_synced_at_ms",
                &stale_ts.to_string(),
                None,
            )
            .unwrap();
        db_persisted
            .insert_portfolio_snapshot(&SnapshotInsertInput {
                total_equity_usdc: 1000.0,
                uncapped_equity_usdc: 10944.72,
                available_cash_usdc: 750.0,
                reserved_order_cash_usdc: 0.0,
                committed_notional_usdc: 250.0,
                net_notional_usdc: 250.0,
                gross_leverage_x: 0.25,
                net_leverage_x: 0.25,
                market_beta_exposure: 0.2,
                market_breadth_bull: 0.55,
                market_regime_label: "NEUTRAL".to_string(),
                risk_tier: "extreme".to_string(),
                leverage_multiplier: 1.85,
                s1_gross_weight: 0.25,
                s2_gross_weight: 0.0,
                s3_gross_weight: 0.0,
                working_order_count: 0,
                open_long_count: 1,
                open_short_count: 0,
                eligible_coin_count: 10,
            })
            .unwrap();
    })
    .await
    .unwrap();

    let res_persisted = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_persisted.status(), StatusCode::OK);
    let persisted_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_persisted.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();

    assert!(
        (persisted_json["snapshot"]["total_equity_usdc"]
            .as_f64()
            .unwrap()
            - 1000.0)
            .abs()
            < 1e-6
    );
    assert_eq!(persisted_json["equity_basis"]["is_stale"], true);
    assert_eq!(persisted_json["equity_basis"]["is_cap_binding"], true);
    assert!(
        (persisted_json["equity_basis"]["total_wallet_equity_usdc"]
            .as_f64()
            .unwrap()
            - 16147.12)
            .abs()
            < 1e-6
    );
    assert!(
        (persisted_json["vault_info"]["combined_portfolio_equity_usdc"]
            .as_f64()
            .unwrap()
            - 16147.12)
            .abs()
            < 1e-6
    );

    // 3. PAPER mode disables cap binding, spot, and stale alerts
    let db_paper = db.clone();
    tokio::task::spawn_blocking(move || {
        db_paper
            .set_config_value("execution_mode", "PAPER", None)
            .unwrap();
    })
    .await
    .unwrap();

    let res_paper = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_paper.status(), StatusCode::OK);
    let paper_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_paper.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(paper_json["equity_basis"]["basis_label"], "PAPER_EQUITY");
    assert_eq!(paper_json["equity_basis"]["is_cap_binding"], false);
    assert_eq!(paper_json["equity_basis"]["spot_usdc"], 0.0);
    assert_eq!(paper_json["equity_basis"]["is_stale"], false);
}

#[tokio::test]
async fn test_web_drawdown_breaker_attached_tpsl_open_orders_and_cap_clamped_pnl() {
    let now = now_ms();
    let cap_two_days_ago = now - 2 * 86_400_000;

    let db = tokio::task::spawn_blocking(move || {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        mgr.set_config_value("max_daily_drawdown_pct", "0.15", None)
            .unwrap();
        mgr.set_config_value(
            "risk_capital_cap_updated_at_ms",
            &cap_two_days_ago.to_string(),
            None,
        )
        .unwrap();
        mgr.set_config_value("live_exchange_equity_synced_at_ms", &now.to_string(), None)
            .unwrap();
        mgr.set_config_value("live_exchange_open_orders_count", "5", None)
            .unwrap();

        let sol_id = mgr
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "SOL".to_string(),
                asset_index: 2,
                sz_decimals: 2,
                tick_size: 0.01,
                lot_step_size: 0.01,
                max_leverage: 20.0,
                mark_price: 100.0,
                mid_price: 100.0,
                oracle_price: 100.0,
                funding_rate_hourly: 0.00001,
                day_volume_usdc: 50_000_000.0,
                open_interest_usdc: 20_000_000.0,
                prev_day_price: 98.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();
        let btc_id = mgr
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "BTC".to_string(),
                asset_index: 0,
                sz_decimals: 4,
                tick_size: 0.1,
                lot_step_size: 0.001,
                max_leverage: 40.0,
                mark_price: 94000.0,
                mid_price: 94000.0,
                oracle_price: 94000.0,
                funding_rate_hourly: 0.00001,
                day_volume_usdc: 85_000_000.0,
                open_interest_usdc: 25_000_000.0,
                prev_day_price: 93000.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            })
            .unwrap();

        // Open SOL position with both protective SL & TP OIDs attached
        let (open_pos_id, _) = mgr
            .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
                asset_id: sol_id,
                coin: "SOL",
                side: OrderSide::Long,
                entry_price: 100.0,
                mark_price: 100.0,
                quantity: 2.0,
                committed_notional_usdc: 200.0,
                unrealized_pnl_usdc: 0.0,
                portfolio_weight: 0.20,
                protective_stop_px: 92.0,
                take_profit_px: 112.0,
                funding_accrued_usdc: 0.0,
                opened_at_ms: Some(now - 3_600_000),
            })
            .unwrap();
        mgr.update_position_tpsl_orders(
            open_pos_id,
            92.0,
            OidUpdate::Set("hl-sl-oid-1"),
            112.0,
            OidUpdate::Set("hl-tp-oid-2"),
        )
        .unwrap();

        // Working BTC entry ALO order
        mgr.create_working_order(&WorkingOrderInput {
            asset_id: btc_id,
            client_order_id: "dd-test-btc-1".to_string(),
            side: OrderSide::Long,
            limit_price: 93900.0,
            requested_quantity: 0.01,
            reserved_margin_usdc: 939.0,
            portfolio_weight: 0.50,
            s1_weight_contrib: 0.50,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 90000.0,
            take_profit_px: None,
            exchange_order_id: Some("hl-entry-oid-3".to_string()),
            reason: "TEST_WORKING".to_string(),
        })
        .unwrap();

        // Insert 24h peak snapshot (1000.0) followed by trough snapshot (676.0 -> 32.4% drawdown >= 15.0%)
        mgr.insert_portfolio_snapshot(&SnapshotInsertInput {
            total_equity_usdc: 1000.0,
            uncapped_equity_usdc: 1000.0,
            available_cash_usdc: 1000.0,
            reserved_order_cash_usdc: 0.0,
            committed_notional_usdc: 0.0,
            net_notional_usdc: 0.0,
            gross_leverage_x: 0.0,
            net_leverage_x: 0.0,
            market_beta_exposure: 0.0,
            market_breadth_bull: 0.5,
            market_regime_label: "NEUTRAL".to_string(),
            risk_tier: "extreme".to_string(),
            leverage_multiplier: 1.85,
            s1_gross_weight: 0.0,
            s2_gross_weight: 0.0,
            s3_gross_weight: 0.0,
            working_order_count: 0,
            open_long_count: 0,
            open_short_count: 0,
            eligible_coin_count: 2,
        })
        .unwrap();
        mgr.insert_portfolio_snapshot(&SnapshotInsertInput {
            total_equity_usdc: 676.0,
            uncapped_equity_usdc: 676.0,
            available_cash_usdc: 400.0,
            reserved_order_cash_usdc: 939.0,
            committed_notional_usdc: 200.0,
            net_notional_usdc: 200.0,
            gross_leverage_x: 0.296,
            net_leverage_x: 0.296,
            market_beta_exposure: 0.2,
            market_breadth_bull: 0.4,
            market_regime_label: "RISK_OFF".to_string(),
            risk_tier: "extreme".to_string(),
            leverage_multiplier: 1.85,
            s1_gross_weight: 0.296,
            s2_gross_weight: 0.0,
            s3_gross_weight: 0.0,
            working_order_count: 1,
            open_long_count: 1,
            open_short_count: 0,
            eligible_coin_count: 2,
        })
        .unwrap();

        mgr
    })
    .await
    .unwrap();

    let app = create_web_router(db.clone(), None);
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(
        &axum::body::to_bytes(res.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();

    // Drawdown breaker tripped (1000 -> 676 = 32.4% >= 15.0%)
    let dd = &body["drawdown_breaker"];
    assert_eq!(dd["tripped"], true);
    assert!((dd["drawdown_pct"].as_f64().unwrap() - 32.4).abs() < 1e-6);
    assert!((dd["max_daily_drawdown_pct"].as_f64().unwrap() - 15.0).abs() < 1e-6);
    assert!((dd["peak_24h_equity_usdc"].as_f64().unwrap() - 1000.0).abs() < 1e-6);
    assert!((dd["latest_equity_usdc"].as_f64().unwrap() - 676.0).abs() < 1e-6);

    // Attached SL/TP orders + synced HL open orders count
    assert_eq!(body["attached_tpsl_order_count"], 2);
    assert_eq!(body["exchange_open_orders_count"], 5);

    // Cap epoch 2 days ago: daily is not clamped, weekly/monthly/ytd are clamped
    assert_eq!(body["pnl_summary"]["daily"]["is_cap_clamped"], false);
    assert_eq!(body["pnl_summary"]["weekly"]["is_cap_clamped"], true);
    assert_eq!(body["pnl_summary"]["monthly"]["is_cap_clamped"], true);
    assert_eq!(body["pnl_summary"]["ytd"]["is_cap_clamped"], true);

    // When unsynced (live_exchange_equity_synced_at_ms == 0), exchange_open_orders_count falls back to ordered + attached SL/TP
    let db_unsynced = db.clone();
    tokio::task::spawn_blocking(move || {
        db_unsynced
            .set_config_value("live_exchange_equity_synced_at_ms", "0", None)
            .unwrap();
    })
    .await
    .unwrap();

    let res_unsynced = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_unsynced.status(), StatusCode::OK);
    let unsynced_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_unsynced.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(unsynced_json["attached_tpsl_order_count"], 2);
    assert_eq!(unsynced_json["exchange_open_orders_count"], 3);
}

#[tokio::test]
async fn test_web_unified_account_equity_basis_and_hl_portfolio_pnl() {
    use aegis_core::{
        CachedHlPortfolioPnl, HlPortfolioWindowCache, VaultInfoPayload, VaultPositionInfo,
    };

    let now = now_ms();
    let cap_ms = now - 3 * 86_400_000;
    let d1_start = now - 86_400_000 - 3_600_000;
    let w1_start = now - 7 * 86_400_000;
    let hlp_addr = "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0";

    let db = tokio::task::spawn_blocking(move || {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        mgr.set_config_value("live_exchange_account_mode", "unifiedAccount", None)
            .unwrap();
        mgr.set_config_value("live_exchange_perp_margin_equity_usdc", "679.15", None)
            .unwrap();
        mgr.set_config_value("live_exchange_equity_usdc", "10978.37", None)
            .unwrap();
        mgr.set_config_value("live_exchange_spot_usdc", "10978.37", None)
            .unwrap();
        mgr.set_config_value("live_exchange_margin_used_usdc", "675.77", None)
            .unwrap();
        mgr.set_config_value("live_exchange_withdrawable_usdc", "10298.32", None)
            .unwrap();
        mgr.set_config_value(
            "live_exchange_equity_synced_at_ms",
            &(now - 2_000).to_string(),
            None,
        )
        .unwrap();
        mgr.set_config_value("risk_capital_cap_usdc", "1000.0", None)
            .unwrap();
        mgr.set_config_value("risk_capital_cap_updated_at_ms", &cap_ms.to_string(), None)
            .unwrap();

        let vault_payload = VaultInfoPayload {
            configured_vault_address: hlp_addr.to_string(),
            total_vault_equity_usdc: 202.47,
            combined_portfolio_equity_usdc: 11180.84,
            vault_count: 1,
            synced_at_ms: now - 2_000,
            vaults: vec![VaultPositionInfo {
                vault_address: hlp_addr.to_string(),
                name: "HLP".to_string(),
                leader: "0x677d831aef5328190852e24f13c46cac05f984e7".to_string(),
                equity_usdc: 202.47,
                pnl_usdc: 0.99,
                all_time_pnl_usdc: 1.41,
                pnl_pct: 0.49,
                all_time_pnl_pct: 0.70,
                apr_pct: 8.45,
                days_following: 26,
                locked_until_ms: 0,
                is_locked: false,
                is_closed: false,
                ..Default::default()
            }],
        };
        mgr.set_config_value(
            "live_vault_info_json",
            &serde_json::to_string(&vault_payload).unwrap(),
            None,
        )
        .unwrap();

        let hl_cache = CachedHlPortfolioPnl {
            synced_at_ms: now - 2_000,
            perp_day: HlPortfolioWindowCache {
                pnl_history: vec![(d1_start, 0.0), (now - 1_000, 242.70)],
                vlm: 1412.91,
            },
            perp_week: HlPortfolioWindowCache {
                pnl_history: vec![
                    (w1_start, 0.0),
                    (cap_ms - 3_600_000, -100.0),
                    (cap_ms + 3_600_000, -300.0),
                    (now - 1_000, 362.52),
                ],
                vlm: 64604.1,
            },
            perp_month: HlPortfolioWindowCache {
                pnl_history: vec![
                    (w1_start, 0.0),
                    (cap_ms - 3_600_000, -100.0),
                    (cap_ms + 3_600_000, -300.0),
                    (now - 1_000, 362.52),
                ],
                vlm: 83291.08,
            },
            perp_all_time: HlPortfolioWindowCache {
                pnl_history: vec![
                    (w1_start, 0.0),
                    (cap_ms - 3_600_000, -100.0),
                    (cap_ms + 3_600_000, -300.0),
                    (now - 1_000, 362.52),
                ],
                vlm: 83291.08,
            },
            ..Default::default()
        };
        mgr.set_config_value(
            "live_exchange_portfolio_pnl_json",
            &serde_json::to_string(&hl_cache).unwrap(),
            None,
        )
        .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app = create_web_router(db, None);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(
        &axum::body::to_bytes(res.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();

    let eb = &body["equity_basis"];
    assert_eq!(eb["account_mode"], "unifiedAccount");
    assert_eq!(eb["is_unified_account"], true);
    assert!((eb["perp_margin_equity_usdc"].as_f64().unwrap() - 679.15).abs() < 1e-6);
    assert!((eb["effective_equity_usdc"].as_f64().unwrap() - 1000.0).abs() < 1e-6);
    assert_eq!(eb["is_cap_binding"], true);
    assert!((eb["total_wallet_equity_usdc"].as_f64().unwrap() - 11180.84).abs() < 1e-6);
    assert!(
        (body["vault_info"]["combined_portfolio_equity_usdc"]
            .as_f64()
            .unwrap()
            - 11180.84)
            .abs()
            < 1e-6
    );

    assert_eq!(body["pnl_summary"]["daily"]["pnl_source"], "HL_PORTFOLIO");
    assert!(
        (body["pnl_summary"]["daily"]["net_pnl_usdc"]
            .as_f64()
            .unwrap()
            - 232.99)
            .abs()
            < 0.05
    );
    assert!(
        (body["pnl_summary"]["daily"]["unrealized_pnl_usdc"]
            .as_f64()
            .unwrap()
            - 0.0)
            .abs()
            < 1e-9
    );
    assert_eq!(body["pnl_summary"]["daily"]["is_cap_clamped"], false);
    assert_eq!(body["pnl_summary"]["weekly"]["pnl_source"], "HL_PORTFOLIO");
    assert_eq!(body["pnl_summary"]["weekly"]["is_cap_clamped"], true);
    assert!(
        (body["pnl_summary"]["weekly"]["net_pnl_usdc"]
            .as_f64()
            .unwrap()
            - 562.52)
            .abs()
            < 1e-4
    );
    assert!(
        (body["pnl_summary"]["weekly"]["unrealized_pnl_usdc"]
            .as_f64()
            .unwrap()
            - 0.0)
            .abs()
            < 1e-9
    );
}

async fn spawn_mock_hl_info_server<F>(handler: F) -> (u16, tokio::sync::oneshot::Sender<()>)
where
    F: Fn(&Value) -> (StatusCode, Value) + Send + Sync + 'static,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback mock Hyperliquid server");
    let port = listener.local_addr().unwrap().port();
    let handler = std::sync::Arc::new(handler);
    let router = axum::Router::new().route(
        "/info",
        axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            let h = handler.clone();
            async move {
                let (status, resp) = h(&body);
                (status, axum::Json(resp))
            }
        }),
    );
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = stop_rx.await;
            })
            .await;
    });
    (port, stop_tx)
}

#[tokio::test]
async fn test_web_vault_config_keys_validation_and_role_normalization() {
    // 1. Direct validator unit checks
    assert!(validate_config_key_value("vault_role", "follower").is_ok());
    assert!(validate_config_key_value("vault_role", "OPERATOR").is_ok());
    assert!(validate_config_key_value("vault_role", "None").is_ok());
    assert!(matches!(
        validate_config_key_value("vault_role", "manager"),
        Err(AegisError::Validation(_))
    ));

    assert!(validate_config_key_value("vault_target_allocation_pct", "0.0").is_ok());
    assert!(validate_config_key_value("vault_target_allocation_pct", "100.0").is_ok());
    assert!(matches!(
        validate_config_key_value("vault_target_allocation_pct", "-0.1"),
        Err(AegisError::Validation(_))
    ));
    assert!(matches!(
        validate_config_key_value("vault_target_allocation_pct", "100.1"),
        Err(AegisError::Validation(_))
    ));

    assert!(validate_config_key_value("vault_profit_sweep_threshold_usdc", "0").is_ok());
    assert!(validate_config_key_value("vault_profit_sweep_threshold_usdc", "10000000").is_ok());
    assert!(matches!(
        validate_config_key_value("vault_profit_sweep_threshold_usdc", "-1.0"),
        Err(AegisError::Validation(_))
    ));
    assert!(matches!(
        validate_config_key_value("vault_profit_sweep_threshold_usdc", "10000000.5"),
        Err(AegisError::Validation(_))
    ));

    assert!(validate_config_key_value("vault_notes", "").is_ok());
    assert!(validate_config_key_value("vault_notes", "Line 1\nLine 2\tOK\r\n").is_ok());
    assert!(validate_config_key_value("vault_notes", &"x".repeat(500)).is_ok());
    assert!(matches!(
        validate_config_key_value("vault_notes", &"x".repeat(501)),
        Err(AegisError::Validation(_))
    ));
    assert!(matches!(
        validate_config_key_value("vault_notes", "bad\x07bell"),
        Err(AegisError::Validation(_))
    ));
    assert!(matches!(
        validate_config_key_value("vault_notes", "bad\x00nul"),
        Err(AegisError::Validation(_))
    ));
    assert!(matches!(
        validate_config_key_value("vault_notes", "bad\x1besc"),
        Err(AegisError::Validation(_))
    ));

    // 2. End-to-end POST /api/v1/config with uppercase VAULT_ROLE normalization
    let db = tokio::task::spawn_blocking(new_test_db_noauth)
        .await
        .unwrap();
    let app = create_web_router(db.clone(), None);

    let hlp_addr = "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0";
    let res_ok = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "updates": {
                            "hyperliquid_vault_address": hlp_addr,
                            "VAULT_ROLE": "  OPERATOR  ",
                            "vault_target_allocation_pct": 25.5,
                            "vault_profit_sweep_threshold_usdc": 250.0,
                            "vault_notes": "Barbell HLP sleeve\nMonthly profit sweep"
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_ok.status(), StatusCode::OK);

    let db_check = db.clone();
    let cfg = tokio::task::spawn_blocking(move || db_check.load_config_or_default())
        .await
        .unwrap();
    assert_eq!(cfg.get_str("hyperliquid_vault_address", ""), hlp_addr);
    assert_eq!(cfg.get_str("vault_role", ""), "operator");
    assert!((cfg.get_f64("vault_target_allocation_pct", 0.0) - 25.5).abs() < 1e-6);
    assert!((cfg.get_f64("vault_profit_sweep_threshold_usdc", 0.0) - 250.0).abs() < 1e-6);
    assert_eq!(
        cfg.get_str("vault_notes", ""),
        "Barbell HLP sleeve\nMonthly profit sweep"
    );

    // Invalid batch config payloads return 400 Bad Request
    for bad_body in [
        serde_json::json!({ "config_key": "vault_role", "config_value": "leader" }).to_string(),
        serde_json::json!({ "config_key": "vault_target_allocation_pct", "config_value": 150.0 })
            .to_string(),
        serde_json::json!({ "config_key": "vault_notes", "config_value": "a".repeat(501) })
            .to_string(),
        serde_json::json!({ "config_key": "vault_notes", "config_value": "ctrl\x07char" })
            .to_string(),
    ] {
        let res_bad = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/config")
                    .header("content-type", "application/json")
                    .body(Body::from(bad_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res_bad.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_web_vault_inspect_endpoint_validation_paper_found_and_not_found() {
    let hlp_addr = "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0";
    let unknown_addr = "0x9999999999999999999999999999999999999999";

    let (port, stop_tx) = spawn_mock_hl_info_server(move |body| {
        let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
        if typ == "vaultDetails" {
            let addr = body
                .get("vaultAddress")
                .and_then(Value::as_str)
                .unwrap_or("");
            if addr.eq_ignore_ascii_case(hlp_addr) {
                let followers: Vec<Value> = (0..100)
                    .map(|i| serde_json::json!({ "user": format!("0x{:040x}", i + 1), "vaultEquity": "50.0" }))
                    .collect();
                return (
                    StatusCode::OK,
                    serde_json::json!({
                        "name": "Hyperliquidity Provider (HLP)",
                        "vaultAddress": hlp_addr,
                        "leader": "0x677d831aef5328190852e24f13c46cac05f984e7",
                        "description": "Protocol market-making vault",
                        "apr": 0.1125,
                        "leaderFraction": 0.025,
                        "leaderCommission": 0.0,
                        "allowDeposits": true,
                        "alwaysCloseOnWithdraw": false,
                        "maxDistributable": "25000000.55",
                        "maxWithdrawable": "24000000.25",
                        "isClosed": false,
                        "followers": followers,
                        "portfolio": [
                            ["day", { "accountValueHistory": [[100, "26000000.0"], [200, "26543210.75"]], "vlm": "1000000.0" }],
                            ["allTime", { "accountValueHistory": [[50, "1000000.0"]], "vlm": "987654321.50" }]
                        ],
                        "relationship": {
                            "type": "parent",
                            "data": {
                                "childAddresses": [
                                    "0x0000000000000000000000000000000000000001",
                                    "0x0000000000000000000000000000000000000002"
                                ]
                            }
                        },
                        "followerState": {
                            "vaultEquity": "500.0",
                            "pnl": "12.5",
                            "allTimePnl": "25.0",
                            "daysFollowing": 14,
                            "lockupUntil": 0
                        }
                    }),
                );
            }
            return (StatusCode::OK, Value::Null);
        }
        (StatusCode::OK, serde_json::json!([]))
    })
    .await;

    let base_url = format!("http://127.0.0.1:{port}");
    let db = tokio::task::spawn_blocking(move || {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("execution_mode", "PAPER", None)
            .unwrap();
        mgr.set_config_value("hyperliquid_testnet_url", &base_url, None)
            .unwrap();
        mgr.set_config_value(
            "hyperliquid_allowed_hosts",
            "127.0.0.1,api.hyperliquid.xyz,api.hyperliquid-testnet.xyz",
            None,
        )
        .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app = create_web_router(db.clone(), None);

    // 1. 400 Bad Request on malformed vault address
    let res_bad_addr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/vault/inspect")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"vault_address":"not-an-evm-address"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_bad_addr.status(), StatusCode::BAD_REQUEST);

    // 2. 400 Bad Request in PAPER mode even with valid EVM address
    let res_paper = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/vault/inspect")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "vault_address": hlp_addr }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_paper.status(), StatusCode::BAD_REQUEST);

    // Switch to TESTNET
    let db_sw = db.clone();
    tokio::task::spawn_blocking(move || {
        db_sw
            .set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
    })
    .await
    .unwrap();

    // 3. 200 OK found == true with enriched VaultPositionInfo
    let res_found = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/vault/inspect")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "vault_address": hlp_addr.to_ascii_uppercase().replacen("0X", "0x", 1) }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_found.status(), StatusCode::OK);
    let body_found: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_found.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body_found["status"], "ok");
    assert_eq!(body_found["execution_mode"], "TESTNET");
    assert_eq!(body_found["is_testnet"], true);
    assert_eq!(body_found["found"], true);
    assert!(body_found["inspected_at_ms"].as_i64().unwrap() > 0);
    let v = &body_found["vault"];
    assert_eq!(v["name"], "Hyperliquidity Provider (HLP)");
    assert_eq!(v["description"], "Protocol market-making vault");
    assert!((v["tvl_usdc"].as_f64().unwrap() - 26_543_210.75).abs() < 1e-2);
    assert_eq!(v["follower_count"], 100);
    assert_eq!(v["followers_capped"], true);
    assert!((v["leader_fraction_pct"].as_f64().unwrap() - 2.5).abs() < 1e-6);
    assert!((v["leader_commission_pct"].as_f64().unwrap() - 0.0).abs() < 1e-6);
    assert_eq!(v["allow_deposits"], true);
    assert_eq!(v["always_close_on_withdraw"], false);
    assert!((v["max_distributable_usdc"].as_f64().unwrap() - 25_000_000.55).abs() < 1e-2);
    assert!((v["max_withdrawable_usdc"].as_f64().unwrap() - 24_000_000.25).abs() < 1e-2);
    assert_eq!(v["relationship_type"], "parent");
    assert_eq!(v["child_vault_count"], 2);
    assert!((v["all_time_volume_usdc"].as_f64().unwrap() - 987_654_321.50).abs() < 1e-2);

    // Inspect must NOT mutate live_vault_info_json
    let db_chk = db.clone();
    let cached_raw = tokio::task::spawn_blocking(move || {
        db_chk
            .load_config_or_default()
            .get_str("live_vault_info_json", "")
    })
    .await
    .unwrap();
    assert!(cached_raw.is_empty());

    // 4. 200 OK found == false, vault == null when Hyperliquid returns null
    let res_missing = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/vault/inspect")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "vault_address": unknown_addr }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_missing.status(), StatusCode::OK);
    let body_missing: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_missing.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body_missing["status"], "ok");
    assert_eq!(body_missing["found"], false);
    assert!(body_missing["vault"].is_null());

    let _ = stop_tx.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_web_vault_sync_endpoint_immediate_sync_and_dashboard_enriched_fields() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let hlp_addr = "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0";
    let user_wallet = "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23";
    let user_eq_calls = Arc::new(AtomicUsize::new(0));
    let details_calls = Arc::new(AtomicUsize::new(0));
    let ueq_srv = user_eq_calls.clone();
    let det_srv = details_calls.clone();

    let (port, stop_tx) = spawn_mock_hl_info_server(move |body| {
        let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
        match typ {
            "userVaultEquities" => {
                ueq_srv.fetch_add(1, Ordering::SeqCst);
                (
                    StatusCode::OK,
                    serde_json::json!([{
                        "vaultAddress": hlp_addr,
                        "equity": "205.50",
                        "lockedUntilTimestamp": 0
                    }]),
                )
            }
            "vaultDetails" => {
                let call_no = det_srv.fetch_add(1, Ordering::SeqCst) + 1;
                let has_user = body
                    .get("user")
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty());
                let tvl_str = if call_no >= 2 {
                    "42000100.25"
                } else {
                    "42000000.00"
                };
                let follower_state = if has_user {
                    serde_json::json!({
                        "vaultEquity": "205.50",
                        "pnl": "3.10",
                        "allTimePnl": "5.50",
                        "daysFollowing": 31,
                        "lockupUntil": 0
                    })
                } else {
                    Value::Null
                };
                (
                    StatusCode::OK,
                    serde_json::json!({
                        "name": "Hyperliquidity Provider (HLP)",
                        "vaultAddress": hlp_addr,
                        "leader": "0x677d831aef5328190852e24f13c46cac05f984e7",
                        "description": "Testnet HLP protocol vault",
                        "apr": 0.095,
                        "leaderFraction": 0.015,
                        "leaderCommission": 0.0,
                        "allowDeposits": true,
                        "alwaysCloseOnWithdraw": false,
                        "maxDistributable": "40000000.00",
                        "maxWithdrawable": "39500000.00",
                        "isClosed": false,
                        "followers": [
                            { "user": "0x1", "vaultEquity": "100.0" },
                            { "user": "0x2", "vaultEquity": "205.5" }
                        ],
                        "portfolio": [
                            ["day", { "accountValueHistory": [[1000, tvl_str]], "vlm": "50000.0" }],
                            ["allTime", { "accountValueHistory": [[500, "1000000.0"]], "vlm": "750000000.88" }]
                        ],
                        "relationship": {
                            "type": "parent",
                            "data": {
                                "childAddresses": [
                                    "0x0000000000000000000000000000000000000011",
                                    "0x0000000000000000000000000000000000000022",
                                    "0x0000000000000000000000000000000000000033"
                                ]
                            }
                        },
                        "followerState": follower_state
                    }),
                )
            }
            _ => (StatusCode::OK, serde_json::json!([])),
        }
    })
    .await;

    let base_url = format!("http://127.0.0.1:{port}");
    let db = tokio::task::spawn_blocking(move || {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("execution_mode", "PAPER", None)
            .unwrap();
        mgr.set_config_value("live_exchange_equity_usdc", "1000.0", None)
            .unwrap();
        mgr.set_config_value("hyperliquid_testnet_url", &base_url, None)
            .unwrap();
        mgr.set_config_value(
            "hyperliquid_allowed_hosts",
            "127.0.0.1,api.hyperliquid.xyz,api.hyperliquid-testnet.xyz",
            None,
        )
        .unwrap();
        mgr.set_config_value("hyperliquid_vault_address", hlp_addr, None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app = create_web_router(db.clone(), None);

    // 1. PAPER mode returns synced=false without hitting Hyperliquid
    let res_paper = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/vault/sync")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_paper.status(), StatusCode::OK);
    let body_paper: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_paper.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body_paper["status"], "ok");
    assert_eq!(body_paper["execution_mode"], "PAPER");
    assert_eq!(body_paper["synced"], false);
    assert_eq!(user_eq_calls.load(Ordering::SeqCst), 0);
    assert_eq!(details_calls.load(Ordering::SeqCst), 0);

    // 2. TESTNET with configured vault ONLY (empty wallet_address): skips userVaultEquities, fetches vaultDetails
    let db_t1 = db.clone();
    tokio::task::spawn_blocking(move || {
        db_t1
            .set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
    })
    .await
    .unwrap();

    let res_sync1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/vault/sync")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_sync1.status(), StatusCode::OK);
    let body_sync1: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_sync1.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body_sync1["status"], "ok");
    assert_eq!(body_sync1["execution_mode"], "TESTNET");
    assert_eq!(body_sync1["synced"], true);
    assert!(body_sync1["synced_at_ms"].as_i64().unwrap() > 0);
    assert_eq!(user_eq_calls.load(Ordering::SeqCst), 0);
    assert_eq!(details_calls.load(Ordering::SeqCst), 1);
    assert_eq!(body_sync1["vault_info"]["vault_count"], 1);
    assert!(
        (body_sync1["vault_info"]["vaults"][0]["tvl_usdc"]
            .as_f64()
            .unwrap()
            - 42_000_000.0)
            .abs()
            < 1e-2
    );

    // 3. Configure wallet_address and immediately invoke POST /api/v1/vault/sync again (min_interval_ms = 0 bypasses 60s throttle)
    let db_t2 = db.clone();
    tokio::task::spawn_blocking(move || {
        db_t2
            .set_config_value("wallet_address", user_wallet, None)
            .unwrap();
    })
    .await
    .unwrap();

    let res_sync2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/vault/sync")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_sync2.status(), StatusCode::OK);
    let body_sync2: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_sync2.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body_sync2["synced"], true);
    assert_eq!(user_eq_calls.load(Ordering::SeqCst), 1);
    assert_eq!(details_calls.load(Ordering::SeqCst), 2);
    assert!(
        (body_sync2["vault_info"]["total_vault_equity_usdc"]
            .as_f64()
            .unwrap()
            - 205.50)
            .abs()
            < 1e-6
    );
    assert!(
        (body_sync2["vault_info"]["combined_portfolio_equity_usdc"]
            .as_f64()
            .unwrap()
            - 1205.50)
            .abs()
            < 1e-6
    );

    // 4. Verify immediate third sync with identical wallet+vault STILL executes (min_interval_ms = 0)
    let res_sync3 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/vault/sync")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_sync3.status(), StatusCode::OK);
    assert_eq!(user_eq_calls.load(Ordering::SeqCst), 2);
    assert_eq!(details_calls.load(Ordering::SeqCst), 3);

    // 5. Verify GET /api/v1/dashboard returns enriched vault_info.vaults[0] fields
    let res_dash = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_dash.status(), StatusCode::OK);
    let dash: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_dash.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();

    // Perp equity strictly segregated from vault equity
    assert!((dash["snapshot"]["total_equity_usdc"].as_f64().unwrap() - 1000.0).abs() < 1e-6);
    let v0 = &dash["vault_info"]["vaults"][0];
    assert_eq!(v0["vault_address"], hlp_addr);
    assert_eq!(v0["name"], "Hyperliquidity Provider (HLP)");
    assert_eq!(v0["description"], "Testnet HLP protocol vault");
    assert!((v0["equity_usdc"].as_f64().unwrap() - 205.50).abs() < 1e-6);
    assert!((v0["tvl_usdc"].as_f64().unwrap() - 42_000_100.25).abs() < 1e-2);
    assert_eq!(v0["follower_count"], 2);
    assert_eq!(v0["followers_capped"], false);
    assert!((v0["leader_fraction_pct"].as_f64().unwrap() - 1.5).abs() < 1e-6);
    assert!((v0["leader_commission_pct"].as_f64().unwrap() - 0.0).abs() < 1e-6);
    assert_eq!(v0["allow_deposits"], true);
    assert_eq!(v0["always_close_on_withdraw"], false);
    assert!((v0["max_distributable_usdc"].as_f64().unwrap() - 40_000_000.0).abs() < 1e-2);
    assert!((v0["max_withdrawable_usdc"].as_f64().unwrap() - 39_500_000.0).abs() < 1e-2);
    assert_eq!(v0["relationship_type"], "parent");
    assert_eq!(v0["child_vault_count"], 3);
    assert!((v0["all_time_volume_usdc"].as_f64().unwrap() - 750_000_000.88).abs() < 1e-2);

    let _ = stop_tx.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sse_stalled_client_drops_on_timeout_and_never_blocks_shutdown() {
    use std::sync::atomic::Ordering;

    // 1. Stalled / unpolled SSE client drops on send timeout instead of pinning MAX_SSE_CLIENTS slot
    let db1 = tokio::task::spawn_blocking(new_test_db_noauth)
        .await
        .unwrap();
    let mut state1 = AppState::new(db1, None);
    state1.sse_client_send_timeout = Duration::from_millis(80);
    let app1 = build_router(state1.clone());

    let stalled_resp = app1
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stalled_resp.status(), StatusCode::OK);
    // Intentionally hold `stalled_resp` without reading its body so `mpsc::channel(2)` stays unpolled.
    // Push updates until the 2-slot buffer fills and `deliver_sse_event` hits the 80ms timeout.
    for _ in 0..8 {
        state1.notify_dashboard_refresh();
        tokio::time::sleep(Duration::from_millis(25)).await;
        if state1.sse_client_count.load(Ordering::SeqCst) == 0 {
            break;
        }
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while state1.sse_client_count.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
        state1.notify_dashboard_refresh();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        state1.sse_client_count.load(Ordering::SeqCst),
        0,
        "Stalled SSE client slot must be released after send timeout"
    );
    drop(stalled_resp);
    state1.shutdown();

    // 2. Shutdown signal preempts even an active slow send immediately (< 200ms)
    let db2 = tokio::task::spawn_blocking(new_test_db_noauth)
        .await
        .unwrap();
    let mut state2 = AppState::new(db2, None);
    state2.sse_client_send_timeout = Duration::from_secs(30);
    let app2 = build_router(state2.clone());

    let stalled_shutdown_resp = app2
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stalled_shutdown_resp.status(), StatusCode::OK);
    assert_eq!(state2.sse_client_count.load(Ordering::SeqCst), 1);

    // Fill 2-slot queue so deliver_sse_event parks inside `tokio::select!`
    for _ in 0..4 {
        state2.notify_dashboard_refresh();
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    let t0 = Instant::now();
    state2.shutdown();
    while state2.sse_client_count.load(Ordering::SeqCst) > 0
        && t0.elapsed() < Duration::from_millis(200)
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        state2.sse_client_count.load(Ordering::SeqCst),
        0,
        "SSE stalled stream must abort immediately (<200ms) on shutdown"
    );
    assert!(t0.elapsed() < Duration::from_millis(200));
    drop(stalled_shutdown_resp);
}

#[tokio::test]
async fn test_dashboard_db_failure_returns_503_and_sse_broadcasts_degraded_without_empty_holdings()
{
    // 1. Critical view failure (`v_assets_held` dropped) -> HTTP 503 with sanitized JSON, zero SQL/DSN leak,
    //    and SSE broadcasts `DegradedDashboardEvent` omitting holdings arrays so clients retain stale rows.
    let db_crit = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db_noauth();
        let mut conn = mgr.get_conn().unwrap();
        conn.query_drop("DROP VIEW IF EXISTS v_assets_held")
            .unwrap();
        mgr
    })
    .await
    .unwrap();
    let app_crit = create_web_router(db_crit.clone(), None);

    for uri in ["/api/v1/dashboard", "/api/v1/state"] {
        let res = app_crit
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "URI {uri}");
        let body_bytes = axum::body::to_bytes(res.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body_bytes);
        let json: Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["degraded"], true);
        assert_eq!(json["database_reachable"], true);
        assert!(json["correlation_id"].as_str().is_some());
        let sections = json["degraded_sections"].as_array().unwrap();
        assert!(sections.iter().any(|s| s == "v_assets_held"));
        assert!(!body_str.contains("mysql://"));
        assert!(!body_str.contains("synthetic_test_pw_99"));
        assert!(!body_str.contains("SELECT "));
    }

    let sse_state = AppState::new(db_crit, None);
    let mut sse_rx = sse_state.dashboard_rx.clone();
    sse_state.ensure_broadcaster_running();
    sse_state.notify_dashboard_refresh();
    let sse_snap = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let cur = sse_rx.borrow_and_update().clone();
            if let Some(snap) = cur {
                if !snap.json.is_empty() {
                    return snap;
                }
            }
            if sse_rx.changed().await.is_err() {
                break;
            }
        }
        panic!("Expected degraded SSE broadcast snapshot");
    })
    .await
    .unwrap();
    sse_state.shutdown();
    assert!(sse_snap.degraded);
    let sse_json: Value = serde_json::from_str(&sse_snap.json).unwrap();
    assert_eq!(sse_json["degraded"], true);
    assert!(sse_json["degraded_sections"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s == "v_assets_held"));
    assert!(sse_json.get("v_assets_held").is_none());
    assert!(sse_json.get("v_assets_ordered").is_none());
    assert!(sse_json.get("snapshot").is_none());

    // 2. Non-critical secondary section failure (`v_assets_in_cooldown` dropped) -> HTTP 200 with `degraded: true`
    let db_sec = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db_noauth();
        let mut conn = mgr.get_conn().unwrap();
        conn.query_drop("DROP VIEW IF EXISTS v_assets_in_cooldown")
            .unwrap();
        mgr
    })
    .await
    .unwrap();
    let app_sec = create_web_router(db_sec, None);
    let res_sec = app_sec
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_sec.status(), StatusCode::OK);
    let json_sec: Value = serde_json::from_slice(
        &axum::body::to_bytes(res_sec.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(json_sec["degraded"], true);
    assert!(json_sec["degraded_sections"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s == "v_assets_in_cooldown"));
}

#[tokio::test]
async fn test_dashboard_snapshot_seq_monotonic_across_rest_and_sse() {
    let db = tokio::task::spawn_blocking(new_test_db_noauth)
        .await
        .unwrap();
    let state = AppState::new(db, None);
    let app = build_router(state.clone());

    let res1 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res1.status(), StatusCode::OK);
    let j1: Value = serde_json::from_slice(
        &axum::body::to_bytes(res1.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    let seq1 = j1["snapshot_seq"].as_u64().unwrap();
    let boot1 = j1["server_boot_id"].as_i64().unwrap();
    assert!(seq1 > 0);
    assert!(boot1 > 0);

    let mut rx = state.dashboard_rx.clone();
    state.ensure_broadcaster_running();
    state.notify_dashboard_refresh();
    let sse_json_str = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let cur = rx.borrow_and_update().clone();
            if let Some(snap) = cur {
                if snap.seq > seq1 && !snap.json.is_empty() {
                    return snap.json;
                }
            }
            if rx.changed().await.is_err() {
                break;
            }
        }
        panic!("Timed out waiting for monotonic SSE snapshot");
    })
    .await
    .unwrap();
    let j2: Value = serde_json::from_str(&sse_json_str).unwrap();
    let seq2 = j2["snapshot_seq"].as_u64().unwrap();
    let boot2 = j2["server_boot_id"].as_i64().unwrap();
    assert!(seq2 > seq1);
    assert_eq!(boot1, boot2);

    let res3 = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let j3: Value = serde_json::from_slice(
        &axum::body::to_bytes(res3.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    let seq3 = j3["snapshot_seq"].as_u64().unwrap();
    assert!(seq3 > seq2);
    assert_eq!(j3["server_boot_id"].as_i64().unwrap(), boot1);
    state.shutdown();
}

#[tokio::test]
async fn test_trusted_proxies_cidr_blocks_spoofed_xff_and_xfp_and_parses_rtl() {
    // 1. web_trust_proxy_headers=true with EMPTY web_trusted_proxies MUST ignore XFP & XFF headers
    let db_empty_cidr = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "true", None)
            .unwrap();
        mgr.set_config_value("web_api_token", "cidr-tok-1", None)
            .unwrap();
        mgr.set_config_value("web_trust_proxy_headers", "true", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();
    let app_empty_cidr = create_web_router(db_empty_cidr, None);
    let attacker_peer: SocketAddr = "198.51.100.10:5555".parse().unwrap();

    let res_no_cidr = app_empty_cidr
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .extension(ConnectInfo(attacker_peer))
                .header("x-forwarded-proto", "https")
                .header("x-forwarded-for", "203.0.113.1")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"cidr-tok-1"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_no_cidr.status(), StatusCode::OK);
    let cookie_no_cidr = res_no_cidr
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(!cookie_no_cidr.contains("; Secure"));

    // 2. With web_trusted_proxies="10.0.0.0/8", untrusted peer spoofing XFF cannot bypass rate-limit or forge XFP,
    //    while trusted proxy peer `10.1.2.3` strips intermediate trusted hops `10.2.3.4` right-to-left.
    let db_cidr = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "true", None)
            .unwrap();
        mgr.set_config_value("web_api_token", "cidr-tok-2", None)
            .unwrap();
        mgr.set_config_value("web_trust_proxy_headers", "true", None)
            .unwrap();
        mgr.set_config_value("web_trusted_proxies", "10.0.0.0/8", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();
    let app_cidr = create_web_router(db_cidr, None);

    // Untrusted peer attempting XFP=https + XFF=127.0.0.1 must NOT get `; Secure`
    let spoof_login = app_cidr
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .extension(ConnectInfo(attacker_peer))
                .header("x-forwarded-proto", "https")
                .header("x-forwarded-for", "127.0.0.1, 10.0.0.2")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"cidr-tok-2"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(spoof_login.status(), StatusCode::OK);
    let spoof_cookie = spoof_login
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(!spoof_cookie.contains("; Secure"));

    // Trusted proxy peer with XFF "spoofed-left, 198.51.100.77, 10.9.9.9" (rightmost non-trusted = 198.51.100.77)
    let proxy_peer: SocketAddr = "10.1.2.3:8443".parse().unwrap();
    let proxy_ok = app_cidr
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .extension(ConnectInfo(proxy_peer))
                .header("x-forwarded-proto", "https")
                .header("x-forwarded-for", "203.0.113.99, 198.51.100.77, 10.9.9.9")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"cidr-tok-2"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(proxy_ok.status(), StatusCode::OK);
    assert!(proxy_ok
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .contains("; Secure"));

    // Exhaust 5 bad login failures for client 198.51.100.77 through trusted proxy
    for _ in 0..5 {
        let bad = app_cidr
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/auth/login")
                    .extension(ConnectInfo(proxy_peer))
                    .header("x-forwarded-for", "203.0.113.99, 198.51.100.77, 10.9.9.9")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"token":"wrong"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::UNAUTHORIZED);
    }
    // 6th attempt from 198.51.100.77 is rate-limited (429) even with different spoofed leftmost IP
    let throttled = app_cidr
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .extension(ConnectInfo(proxy_peer))
                .header("x-forwarded-for", "1.2.3.4, 198.51.100.77, 10.9.9.9")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"cidr-tok-2"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(throttled.status(), StatusCode::TOO_MANY_REQUESTS);

    // Another real client (198.51.100.88) behind the SAME trusted proxy peer is unaffected (200 OK)
    let other_client = app_cidr
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .extension(ConnectInfo(proxy_peer))
                .header("x-forwarded-for", "198.51.100.88, 10.9.9.9")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"cidr-tok-2"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(other_client.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_loopback_bind_requires_auth_by_default_unless_explicitly_opted_out() {
    // 1. Fresh DB with unset web_auth_enabled and unset token refuses loopback bind & enforces auth
    let db_unset = tokio::task::spawn_blocking(new_test_db).await.unwrap();
    let app_unset = create_web_router_for_host(db_unset.clone(), None, Some("127.0.0.1"));
    let res_unset = app_unset
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_unset.status(), StatusCode::UNAUTHORIZED);

    let (_sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    let startup_err = serve_web_with_shutdown(db_unset, "127.0.0.1", 0, sd_rx).await;
    assert!(matches!(startup_err, Err(AegisError::SecurityViolation(_))));

    // 2. Explicit DB-immutable `web_auth_enabled=false` opts out for loopback access
    let db_optout = tokio::task::spawn_blocking(new_test_db_noauth)
        .await
        .unwrap();
    let app_optout = create_web_router_for_host(db_optout, None, Some("127.0.0.1"));
    let res_optout = app_optout
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_optout.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sse_idle_to_active_transition_rebuilds_fresh_snapshot_not_stale_cache() {
    let db = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("risk_tier", "aggressive", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();
    let state = AppState::new(db, None);

    // Inject a stale cached snapshot (> 2s old) while 0 SSE clients are connected
    let stale_json = r#"{"risk_tier":"conservative","snapshot_seq":1,"stale":true}"#;
    let _ = state.dashboard_tx.send(Some(aegis_web::BroadcastSnapshot {
        seq: 1,
        generated_at: Instant::now() - Duration::from_secs(10),
        generated_at_ms: now_ms() - 10_000,
        json: std::sync::Arc::new(stale_json.to_string()),
        degraded: false,
    }));

    let app = build_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let mut data_stream = resp.into_body().into_data_stream();
    let first_chunk = tokio::time::timeout(Duration::from_secs(4), data_stream.next())
        .await
        .expect("SSE emitted initial snapshot within timeout")
        .expect("Stream returned chunk")
        .expect("Chunk succeeded");
    let text = String::from_utf8_lossy(&first_chunk);
    assert!(
        text.contains(r#""risk_tier":"aggressive""#),
        "Expected fresh DB snapshot on idle->active transition, got: {text}"
    );
    assert!(!text.contains(r#""stale":true"#));
    drop(data_stream);
    state.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sse_stream_terminates_immediately_on_session_revocation_and_expiry() {
    let db = tokio::task::spawn_blocking(|| {
        let mgr = new_test_db();
        mgr.set_config_value("web_auth_enabled", "true", None)
            .unwrap();
        mgr.set_config_value("web_api_token", "sse-revoke-secret", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();
    let mut state = AppState::new(db, None);
    state.sse_auth_recheck_interval = Duration::from_millis(50);
    let app = build_router(state.clone());

    let login_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"sse-revoke-secret"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login_res.status(), StatusCode::OK);
    let set_cookie = login_res
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let cookie_pair = set_cookie.split(';').next().unwrap().to_string();

    let sse_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/stream")
                .header(header::COOKIE, &cookie_pair)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(sse_resp.status(), StatusCode::OK);

    let mut data_stream = sse_resp.into_body().into_data_stream();
    let initial = tokio::time::timeout(Duration::from_secs(4), data_stream.next())
        .await
        .expect("Initial SSE frame")
        .expect("Initial chunk exists")
        .expect("Chunk ok");
    assert!(!initial.is_empty());

    // Revoke session via POST /api/v1/auth/logout -> stream emits `event: auth_expired` and closes immediately
    let logout_res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/logout")
                .header(header::HOST, "127.0.0.1:8000")
                .header(header::ORIGIN, "http://127.0.0.1:8000")
                .header(header::COOKIE, &cookie_pair)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(logout_res.status(), StatusCode::OK);
    let clear_cookie = logout_res
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(clear_cookie.contains("Max-Age=0"));

    let mut seen_auth_expired = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), data_stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                let s = String::from_utf8_lossy(&chunk);
                if s.contains("event: auth_expired") || s.contains("auth_expired") {
                    seen_auth_expired = true;
                }
            }
            Ok(None) => break,
            Ok(Some(Err(_))) | Err(_) => break,
        }
    }
    assert!(
        seen_auth_expired,
        "Expected SSE stream to emit event: auth_expired upon session logout/revocation"
    );
    state.shutdown();
}

#[tokio::test]
async fn test_health_503_and_internal_500_sanitize_mysql_errors_with_correlation_id() {
    let secret_dsn_err =
        "mysql://aegis:synthetic_test_pw_99@10.0.0.5:3306/aegis_quant Access denied SELECT * FROM system_config";
    let resp_503 = health_result_to_response(Err(AegisError::Database(secret_dsn_err.to_string())));
    assert_eq!(resp_503.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes_503 = axum::body::to_bytes(resp_503.into_body(), 64 * 1024)
        .await
        .unwrap();
    let str_503 = String::from_utf8_lossy(&bytes_503);
    let json_503: Value = serde_json::from_slice(&bytes_503).unwrap();
    assert_eq!(json_503["status"], "unavailable");
    assert!(json_503["correlation_id"]
        .as_str()
        .is_some_and(|c| c.starts_with("err-")));
    for secret in ["synthetic_test_pw_99", "mysql://", "10.0.0.5", "SELECT *"] {
        assert!(!str_503.contains(secret), "503 leaked {secret}: {str_503}");
    }

    let resp_500 = aegis_web::map_aegis_error_response(AegisError::Crypto(
        "Failed reading /var/lib/aegis/secret.key for user root@127.0.0.1".to_string(),
    ));
    assert_eq!(resp_500.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes_500 = axum::body::to_bytes(resp_500.into_body(), 64 * 1024)
        .await
        .unwrap();
    let str_500 = String::from_utf8_lossy(&bytes_500);
    let json_500: Value = serde_json::from_slice(&bytes_500).unwrap();
    assert!(json_500["correlation_id"]
        .as_str()
        .is_some_and(|c| c.starts_with("err-")));
    assert!(!str_500.contains("secret.key"));
    assert!(!str_500.contains("/var/lib"));
}

#[tokio::test]
async fn test_csp_strict_no_unsafe_inline_and_no_external_fonts() {
    assert!(!DEFAULT_CSP_POLICY.contains("'unsafe-inline'"));
    assert!(!DEFAULT_CSP_POLICY.contains("'unsafe-eval'"));
    assert!(!DEFAULT_CSP_POLICY.contains("fonts.googleapis.com"));
    assert!(!DEFAULT_CSP_POLICY.contains("fonts.gstatic.com"));
    assert!(DEFAULT_CSP_POLICY.contains("script-src 'self';"));
    assert!(DEFAULT_CSP_POLICY.contains("style-src 'self';"));
    assert!(DEFAULT_CSP_POLICY.contains("style-src-attr 'none';"));

    let db = tokio::task::spawn_blocking(new_test_db_noauth)
        .await
        .unwrap();
    let app = create_web_router(db, None);

    for path in ["/", "/styles.css", "/js/main.js", "/api/v1/dashboard"] {
        let res = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "path {path}");
        let csp = res
            .headers()
            .get(header::CONTENT_SECURITY_POLICY)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(csp, DEFAULT_CSP_POLICY);
    }
}

#[tokio::test]
async fn test_static_html_and_js_audit_zero_inline_styles_and_innerhtml() {
    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../web/static");
    let index_html = std::fs::read_to_string(static_dir.join("index.html")).unwrap();
    assert!(
        !index_html.contains("style="),
        "index.html contains inline style="
    );
    assert!(
        !index_html.contains("fonts.googleapis.com") && !index_html.contains("fonts.gstatic.com"),
        "index.html references external Google Fonts"
    );
    assert!(index_html.contains(r#"<script type="module" src="/js/main.js"></script>"#));

    let app_js = std::fs::read_to_string(static_dir.join("app.js")).unwrap();
    assert_eq!(app_js.trim(), "import '/js/main.js';");

    for module in [
        "state.js",
        "format.js",
        "api.js",
        "tables.js",
        "charts.js",
        "config.js",
        "modals.js",
        "main.js",
    ] {
        let src = std::fs::read_to_string(static_dir.join("js").join(module)).unwrap();
        assert!(
            !src.contains("innerHTML"),
            "web/static/js/{module} must not use innerHTML"
        );
        assert!(
            !src.contains("style=") && !src.contains(".style."),
            "web/static/js/{module} must not use inline style= or .style. DOM mutations"
        );
    }
}

#[tokio::test]
async fn test_static_assets_etag_304_and_js_module_allowlist_blocks_traversal() {
    let db = tokio::task::spawn_blocking(new_test_db_noauth)
        .await
        .unwrap();
    let app = create_web_router(db, None);

    let paths = [
        "/",
        "/styles.css",
        "/app.js",
        "/js/state.js",
        "/js/format.js",
        "/js/api.js",
        "/js/tables.js",
        "/js/charts.js",
        "/js/config.js",
        "/js/modals.js",
        "/js/main.js",
    ];

    for path in paths {
        let res_200 = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res_200.status(), StatusCode::OK, "200 expected for {path}");
        assert_eq!(
            res_200
                .headers()
                .get(header::CACHE_CONTROL)
                .unwrap()
                .to_str()
                .unwrap(),
            "public, max-age=0, must-revalidate"
        );
        let etag = res_200
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(etag.starts_with('"') && etag.ends_with('"'));

        // Exact strong match -> 304
        let res_304 = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header(header::IF_NONE_MATCH, etag.as_str())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res_304.status(), StatusCode::NOT_MODIFIED, "304 for {path}");

        // RFC 9110 comma-separated list with W/ prefix -> 304
        let multi_inm = format!(r#""stale", W/{etag}"#);
        let res_304_multi = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header(header::IF_NONE_MATCH, multi_inm)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res_304_multi.status(),
            StatusCode::NOT_MODIFIED,
            "304 multi If-None-Match for {path}"
        );
    }

    for bad_path in [
        "/js/unknown.js",
        "/js/..%2Findex.html",
        "/js/../index.html",
        "/js/sub/state.js",
    ] {
        let res_404 = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(bad_path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res_404.status(),
            StatusCode::NOT_FOUND,
            "404 expected for traversal/unknown path {bad_path}"
        );
    }
}

#[tokio::test]
async fn test_forensic_tier1_and_p0_p1_config_validation_uncapped_drawdown_and_priority_health() {
    // 1. Validate Tier-1 & P0/P1 strategy/execution/retention bounds
    for (k, v) in [
        ("enable_breadth_regime_gate", "true"),
        ("early_loser_invalidation_enabled", "false"),
        ("breadth_bull_block_shorts_threshold", "0.75"),
        ("breadth_bear_block_longs_threshold", "0.25"),
        ("breakeven_trigger_r", "0.50"),
        ("breakeven_lock_r", "0.10"),
        ("stagnation_time_stop_hours", "12.0"),
        ("stagnation_min_mfe_r", "0.25"),
        ("risk_per_trade_equity_pct", "0.02"),
        ("min_equity_halt_usdc", "50.0"),
        ("retention_ohlcv_bars_days", "90"),
        ("hl_rest_weight_limit_per_min", "900"),
    ] {
        assert!(
            validate_config_key_value(k, v).is_ok(),
            "Expected valid config {k}={v}"
        );
    }
    for (k, bad_v) in [
        ("breadth_bull_block_shorts_threshold", "0.10"),
        ("breakeven_trigger_r", "5.0"),
        ("retention_ohlcv_bars_days", "5"),
        ("hl_rest_weight_limit_per_min", "2000"),
    ] {
        assert!(
            validate_config_key_value(k, bad_v).is_err(),
            "Expected invalid config {k}={bad_v}"
        );
    }
    for immut in ["web_trusted_proxies", "priority_monitor_health_json"] {
        assert!(matches!(
            validate_config_key_value(immut, "bad"),
            Err(AegisError::SecurityViolation(_))
        ));
    }

    // 2. Uncapped drawdown detection in TESTNET even when capped equity barely moved + PriorityMonitorHealth
    let now = now_ms();
    let db = tokio::task::spawn_blocking(move || {
        let mgr = new_test_db_noauth();
        mgr.set_config_value("execution_mode", "TESTNET", None)
            .unwrap();
        mgr.set_config_value("risk_capital_cap_usdc", "1000.0", None)
            .unwrap();
        mgr.set_config_value("max_daily_drawdown_pct", "0.15", None)
            .unwrap();
        mgr.set_config_value("min_equity_halt_usdc", "50.0", None)
            .unwrap();
        mgr.set_config_value("live_exchange_equity_usdc", "9000.0", None)
            .unwrap();
        mgr.set_config_value("live_exchange_equity_synced_at_ms", &now.to_string(), None)
            .unwrap();
        mgr.set_config_value(
            "priority_monitor_health_json",
            &serde_json::json!({
                "last_tick_at_ms": now,
                "consecutive_error_ticks": 3,
                "last_tick_error_count": 1,
                "failed_symbols": ["SOL"],
                "unfilled_stop_breach_symbols": ["BTC"],
                "unmonitored_alert_active": true,
                "is_stale": false,
                "status": "DEGRADED"
            })
            .to_string(),
            None,
        )
        .unwrap();

        mgr.insert_portfolio_snapshot(&SnapshotInsertInput {
            total_equity_usdc: 1000.0,
            uncapped_equity_usdc: 12000.0,
            available_cash_usdc: 1000.0,
            reserved_order_cash_usdc: 0.0,
            committed_notional_usdc: 0.0,
            net_notional_usdc: 0.0,
            gross_leverage_x: 0.0,
            net_leverage_x: 0.0,
            market_beta_exposure: 0.0,
            market_breadth_bull: 0.5,
            market_regime_label: "NEUTRAL".to_string(),
            risk_tier: "standard".to_string(),
            leverage_multiplier: 1.0,
            s1_gross_weight: 0.0,
            s2_gross_weight: 0.0,
            s3_gross_weight: 0.0,
            working_order_count: 0,
            open_long_count: 0,
            open_short_count: 0,
            eligible_coin_count: 2,
        })
        .unwrap();
        mgr.insert_portfolio_snapshot(&SnapshotInsertInput {
            total_equity_usdc: 990.0,
            uncapped_equity_usdc: 9000.0,
            available_cash_usdc: 990.0,
            reserved_order_cash_usdc: 0.0,
            committed_notional_usdc: 0.0,
            net_notional_usdc: 0.0,
            gross_leverage_x: 0.0,
            net_leverage_x: 0.0,
            market_beta_exposure: 0.0,
            market_breadth_bull: 0.4,
            market_regime_label: "NEUTRAL".to_string(),
            risk_tier: "standard".to_string(),
            leverage_multiplier: 1.0,
            s1_gross_weight: 0.0,
            s2_gross_weight: 0.0,
            s3_gross_weight: 0.0,
            working_order_count: 0,
            open_long_count: 0,
            open_short_count: 0,
            eligible_coin_count: 2,
        })
        .unwrap();
        mgr
    })
    .await
    .unwrap();

    let app = create_web_router(db, None);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let dash: Value = serde_json::from_slice(
        &axum::body::to_bytes(res.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();

    assert_eq!(dash["drawdown_breaker"]["basis"], "UNCAPPED_WALLET_EQUITY");
    assert_eq!(dash["drawdown_breaker"]["tripped"], true);
    assert!((dash["drawdown_breaker"]["drawdown_pct"].as_f64().unwrap() - 25.0).abs() < 1e-4);
    assert_eq!(dash["priority_monitor_health"]["status"], "DEGRADED");
    assert_eq!(
        dash["priority_monitor_health"]["unmonitored_alert_active"],
        true
    );
    assert!(dash["immutable_config_keys"]
        .as_array()
        .unwrap()
        .iter()
        .any(|k| k == "web_trusted_proxies"));
}

#[tokio::test]
async fn test_web_risk_tier_switch_and_config_sync_with_override_preservation() {
    let db = tokio::task::spawn_blocking(new_test_db_noauth)
        .await
        .unwrap();
    let app = create_web_router(db.clone(), None);

    // 1. POST /api/v1/risk-tier synchronizes target_leverage_multiplier, risk_per_trade_equity_pct, and max_single_asset_weight
    for (tier, custom_mult, exp_mult, exp_risk, exp_weight) in [
        ("conservative", None, 0.65, 0.005, 0.15),
        ("standard", None, 1.00, 0.010, 0.15),
        ("aggressive", None, 1.35, 0.015, 0.20),
        ("extreme", None, 1.85, 0.020, 0.25),
        ("custom", Some(1.50), 1.50, 0.015, 0.225),
    ] {
        let req = RiskTierUpdateRequest {
            risk_tier: tier.to_string(),
            custom_leverage_multiplier: custom_mult,
        };
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/risk-tier")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let cfg = db.load_config().unwrap();
        assert_eq!(cfg.get_str("risk_tier", ""), tier);
        assert!((cfg.get_f64("target_leverage_multiplier", 0.0) - exp_mult).abs() < 1e-6);
        assert!((cfg.get_f64("risk_per_trade_equity_pct", 0.0) - exp_risk).abs() < 1e-6);
        assert!((cfg.get_f64("max_single_asset_weight", 0.0) - exp_weight).abs() < 1e-6);
    }

    // 2. POST /api/v1/config changing risk_tier directly syncs all 3 unless explicitly overridden in same write
    let res_sync = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"updates":{"risk_tier":"standard"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_sync.status(), StatusCode::OK);
    let cfg_std = db.load_config().unwrap();
    assert!((cfg_std.get_f64("target_leverage_multiplier", 0.0) - 1.00).abs() < 1e-6);
    assert!((cfg_std.get_f64("risk_per_trade_equity_pct", 0.0) - 0.010).abs() < 1e-6);
    assert!((cfg_std.get_f64("max_single_asset_weight", 0.0) - 0.15).abs() < 1e-6);

    // Override risk_per_trade_equity_pct in the same tier switch write -> override preserved, weight synced
    let res_override = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"updates":{"risk_tier":"aggressive","risk_per_trade_equity_pct":"0.0300"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_override.status(), StatusCode::OK);
    let cfg_ovr = db.load_config().unwrap();
    assert!((cfg_ovr.get_f64("target_leverage_multiplier", 0.0) - 1.35).abs() < 1e-6);
    assert!((cfg_ovr.get_f64("risk_per_trade_equity_pct", 0.0) - 0.030).abs() < 1e-6);
    assert!((cfg_ovr.get_f64("max_single_asset_weight", 0.0) - 0.20).abs() < 1e-6);

    // Custom tier switch via POST /api/v1/config with explicit weight override preserves weight & syncs risk_pct
    let res_custom_ovr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"updates":{"risk_tier":"custom","target_leverage_multiplier":"1.50","max_single_asset_weight":"0.18"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_custom_ovr.status(), StatusCode::OK);
    let cfg_cust = db.load_config().unwrap();
    assert!((cfg_cust.get_f64("target_leverage_multiplier", 0.0) - 1.50).abs() < 1e-6);
    assert!((cfg_cust.get_f64("risk_per_trade_equity_pct", 0.0) - 0.015).abs() < 1e-6);
    assert!((cfg_cust.get_f64("max_single_asset_weight", 0.0) - 0.18).abs() < 1e-6);

    // 3. New v2.2.0 keys writable via POST /api/v1/config and out-of-range values rejected with 400
    let res_new_ok = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"updates":{"signal_timeframe":"1h","max_portfolio_heat_pct":"0.18","wall_clock_normalize_indicators":false,"max_entries_per_bar":4,"maker_only_rest_ttl_ms":30000,"early_loser_r0_mult":"0.30","early_loser_max_loss_pct":"0.018"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_new_ok.status(), StatusCode::OK);
    let cfg_new = db.load_config().unwrap();
    assert_eq!(cfg_new.get_str("signal_timeframe", ""), "1h");
    assert!((cfg_new.get_f64("max_portfolio_heat_pct", 0.0) - 0.18).abs() < 1e-6);
    assert!(!cfg_new.get_bool("wall_clock_normalize_indicators", true));
    assert_eq!(cfg_new.get_i64("max_entries_per_bar", 0), 4);
    assert_eq!(cfg_new.get_i64("maker_only_rest_ttl_ms", 0), 30_000);
    assert!((cfg_new.get_f64("early_loser_r0_mult", 0.0) - 0.30).abs() < 1e-6);
    assert!((cfg_new.get_f64("early_loser_max_loss_pct", 0.0) - 0.018).abs() < 1e-6);

    for bad_payload in [
        r#"{"config_key":"signal_timeframe","config_value":"30m"}"#,
        r#"{"config_key":"max_portfolio_heat_pct","config_value":"1.5"}"#,
        r#"{"config_key":"signal_exit_confirm_bars","config_value":"9"}"#,
        r#"{"config_key":"early_loser_r0_mult","config_value":"0.01"}"#,
        r#"{"config_key":"early_loser_max_loss_pct","config_value":"0.20"}"#,
    ] {
        let bad_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/config")
                    .header("content-type", "application/json")
                    .body(Body::from(bad_payload))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_res.status(), StatusCode::BAD_REQUEST);
    }
}
