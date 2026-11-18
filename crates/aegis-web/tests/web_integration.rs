//! Integration tests for Aegis Quant v2.0 Web Dashboard & REST/SSE API (`aegis-web`).

use aegis_core::OrderSide;
use aegis_db::{AssetUpsertInput, DatabaseManager, WorkingOrderInput};
use aegis_web::{
    create_web_router, ClosePositionRequest, ConfigUpdateRequest, RiskTierUpdateRequest,
};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::Value;
use std::path::Path;
use tower::ServiceExt;

#[tokio::test]
async fn test_web_health_dashboard_risk_tier_and_position_close() {
    let db = tokio::task::spawn_blocking(|| {
        let mgr = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
