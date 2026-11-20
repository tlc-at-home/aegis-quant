//! Integration tests for Aegis Quant v2.0 Web Dashboard & REST/SSE API (`aegis-web`).

use aegis_core::{now_ms, AegisError, OrderSide};
use aegis_db::{AssetUpsertInput, DatabaseManager, WorkingOrderInput};
use aegis_web::{
    build_router, create_web_router, create_web_router_for_host, health_result_to_response,
    serve_web_with_shutdown, validate_config_key_value, AppState, AuthLoginPayload,
    ClosePositionRequest, ConfigUpdateRequest, RiskTierUpdateRequest, DEFAULT_CSP_POLICY,
};
use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use mysql::params;
use mysql::prelude::*;
use serde_json::Value;
use std::path::Path;
use std::time::{Duration, Instant};
use tower::ServiceExt;

#[tokio::test]
async fn test_web_health_dashboard_risk_tier_and_position_close() {
    let db = tokio::task::spawn_blocking(|| {
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
    let db = tokio::task::spawn_blocking(|| {
        DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap()
    })
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
}

#[tokio::test]
async fn test_web_auth_explicit_disable_on_non_loopback_respects_operator_opt_out() {
    // 1. When web_auth_enabled is unset and a token exists on non-loopback, fail closed (401).
    let db_default = tokio::task::spawn_blocking(|| {
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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

            mgr.sync_open_position_from_exchange_v2(
                asset_id,
                coin,
                OrderSide::Long,
                px,
                px,
                150.0 / px,
                150.0,
                0.0,
                0.15,
                px * 0.95,
                px * 1.10,
                0.0,
                opened_at,
            )
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
    let db_unset_remote = tokio::task::spawn_blocking(|| {
        DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap()
    })
    .await
    .unwrap();

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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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

    // Trusted proxy headers honor X-Forwarded-Proto: https
    let db_proxy = tokio::task::spawn_blocking(|| {
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
        mgr.set_config_value("web_auth_enabled", "true", None)
            .unwrap();
        mgr.set_config_value("web_api_token", "tok-cookie-30", None)
            .unwrap();
        mgr.set_config_value("web_trust_proxy_headers", "true", None)
            .unwrap();
        mgr
    })
    .await
    .unwrap();
    let app_proxy = create_web_router(db_proxy, None);

    let login_xfp = app_proxy
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
        mgr.sync_open_position_from_exchange_v2(
            sol_id,
            "SOL",
            OrderSide::Long,
            180.0,
            186.17,
            2.0,
            372.34,
            12.34,
            0.59,
            172.0,
            195.0,
            0.0,
            Some(now - 600_000),
        )
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
                exchange_order_id: None,
                reason: "TEST_CANCEL".to_string(),
            })
            .unwrap();
        mgr.cancel_working_order(cancelled_pid, "TEST_CANCELLED")
            .unwrap();

        mgr.insert_portfolio_snapshot(&aegis_db::SnapshotInsertInput {
            total_equity_usdc: 629.84,
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
    sse_state.notify_dashboard_refresh();
    let sse_payload_str = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let cur = sse_rx.borrow_and_update().clone();
            if !cur.is_empty() {
                return cur;
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
