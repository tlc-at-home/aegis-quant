//! Integration tests for Aegis Quant v2.0 Web Dashboard & REST/SSE API (`aegis-web`).

use aegis_core::{now_ms, AegisError, OrderSide};
use aegis_db::{AssetUpsertInput, DatabaseManager, WorkingOrderInput};
use aegis_web::{
    create_web_router, create_web_router_for_host, health_result_to_response,
    validate_config_key_value, AuthLoginPayload, ClosePositionRequest, ConfigUpdateRequest,
    RiskTierUpdateRequest, DEFAULT_CSP_POLICY,
};
use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use serde_json::Value;
use std::path::Path;
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
