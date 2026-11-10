    use super::*;
    use serde_json::json;

    #[test]
    fn test_order_fsm_and_eip712_signing() {
        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        let exec = ExecutionEngine::new(db.clone());
        let assets = db.get_usdc_perp_universe().unwrap();
        let sol = assets.iter().find(|a| a.symbol == "SOL/USDC:USDC").unwrap();

        let partial = exec.execute_maker_limit_chaser(
            sol,
            "LONG",
            5.0,
            180.0,
            182.0,
            179.0,
            12_000.0,
            Some(0.5),
            None,
            Some("test-partial-1"),
            false,
        );
        assert_eq!(partial.fsm_state, "FILLED");
        assert!(partial
            .state_history
            .contains(&"PARTIALLY_FILLED".to_string()));

        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let sig = ExecutionEngine::sign_hyperliquid_action(
            pk,
            &json!({"type": "order"}),
            1700000000000,
            None,
            true,
        )
        .unwrap();
        assert!(sig["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig["signer"].as_str().unwrap().starts_with("0x"));
    }

    #[test]
    fn test_hyperliquid_python_sdk_action_hash_vector() {
        let order_action = OrderAction {
            action_type: "order".to_string(),
            orders: vec![OrderWire {
                a: 4,
                b: true,
                p: "1670.1".to_string(),
                s: "0.0147".to_string(),
                r: false,
                t: OrderTypeWire {
                    limit: Some(LimitOrderType {
                        tif: "Ioc".to_string(),
                    }),
                    trigger: None,
                },
                c: None,
            }],
            grouping: "na".to_string(),
        };

        let hash = compute_l1_action_hash(&order_action, None, 1677777606040, None).unwrap();
        assert_eq!(
            format!("0x{}", hex::encode(hash)),
            "0x0fcbeda5ae3c4950a548021552a4fea2226858c4453571bf3f24ba017eac2908"
        );
    }

    #[test]
    fn test_hyperliquid_python_sdk_l1_signing_order_vector() {
        let pk = "0x0123456789012345678901234567890123456789012345678901234567890123";
        let order_action = OrderAction {
            action_type: "order".to_string(),
            orders: vec![OrderWire {
                a: 1,
                b: true,
                p: "100".to_string(),
                s: "100".to_string(),
                r: false,
                t: OrderTypeWire {
                    limit: Some(LimitOrderType {
                        tif: "Gtc".to_string(),
                    }),
                    trigger: None,
                },
                c: None,
            }],
            grouping: "na".to_string(),
        };

        let sig_mainnet = sign_l1_action(pk, &order_action, None, 0, None, true).unwrap();
        assert_eq!(
            sig_mainnet["r"].as_str().unwrap(),
            "0xd65369825a9df5d80099e513cce430311d7d26ddf477f5b3a33d2806b100d78e"
        );
        assert_eq!(
            sig_mainnet["s"].as_str().unwrap(),
            "0x2b54116ff64054968aa237c20ca9ff68000f977c93289157748a3162b6ea940e"
        );
        assert_eq!(sig_mainnet["v"].as_u64().unwrap(), 28);

        let sig_testnet = sign_l1_action(pk, &order_action, None, 0, None, false).unwrap();
        assert_eq!(
            sig_testnet["r"].as_str().unwrap(),
            "0x82b2ba28e76b3d761093aaded1b1cdad4960b3af30212b343fb2e6cdfa4e3d54"
        );
        assert_eq!(
            sig_testnet["s"].as_str().unwrap(),
            "0x6b53878fc99d26047f4d7e8c90eb98955a109f44209163f52d8dc4278cbbd9f5"
        );
        assert_eq!(sig_testnet["v"].as_u64().unwrap(), 27);
    }

    #[test]
    fn test_vault_deposit_and_withdrawal_signing() {
        let pk = "0x0123456789012345678901234567890123456789012345678901234567890123";
        let vault_addr = "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0";

        let deposit_action = VaultTransferAction {
            action_type: "vaultTransfer".to_string(),
            vault_address: vault_addr.to_string(),
            is_deposit: true,
            usd: 100_000_000,
        };

        let msgpack_bytes = rmp_serde::encode::to_vec_named(&deposit_action).unwrap();
        assert!(!msgpack_bytes.is_empty());

        let deposit_hash =
            compute_l1_action_hash(&deposit_action, None, 1700000000000, None).unwrap();
        assert_eq!(deposit_hash.len(), 32);

        let sig_deposit_mainnet =
            sign_l1_action(pk, &deposit_action, None, 1700000000000, None, true).unwrap();
        assert!(sig_deposit_mainnet["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig_deposit_mainnet["s"].as_str().unwrap().starts_with("0x"));
        let v_dep_main = sig_deposit_mainnet["v"].as_u64().unwrap();
        assert!(v_dep_main == 27 || v_dep_main == 28);

        let sig_deposit_testnet =
            sign_l1_action(pk, &deposit_action, None, 1700000000000, None, false).unwrap();
        assert!(sig_deposit_testnet["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig_deposit_testnet["s"].as_str().unwrap().starts_with("0x"));
        let v_dep_test = sig_deposit_testnet["v"].as_u64().unwrap();
        assert!(v_dep_test == 27 || v_dep_test == 28);

        let withdraw_action = VaultTransferAction {
            action_type: "vaultTransfer".to_string(),
            vault_address: vault_addr.to_string(),
            is_deposit: false,
            usd: 50_000_000,
        };

        let withdraw_hash =
            compute_l1_action_hash(&withdraw_action, None, 1700000000000, None).unwrap();
        assert_eq!(withdraw_hash.len(), 32);
        assert_ne!(deposit_hash, withdraw_hash);

        let sig_withdraw_testnet =
            sign_l1_action(pk, &withdraw_action, None, 1700000000000, None, false).unwrap();
        assert!(sig_withdraw_testnet["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig_withdraw_testnet["s"].as_str().unwrap().starts_with("0x"));

        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        let exec = ExecutionEngine::new(db);
        let sim_deposit = exec.execute_hlp_vault_transfer(100.0, true, Some(vault_addr));
        assert_eq!(sim_deposit.status, "SIMULATED");
        assert_eq!(sim_deposit.usd_micro, 100_000_000);
        assert!(sim_deposit.is_deposit);

        let sim_withdraw = exec.execute_hlp_vault_transfer(50.0, false, Some(vault_addr));
        assert_eq!(sim_withdraw.status, "SIMULATED");
        assert_eq!(sim_withdraw.usd_micro, 50_000_000);
        assert!(!sim_withdraw.is_deposit);
    }

    #[test]
    fn test_asset_index_cache_and_fallback() {
        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        assert!(db
            .set_config_value("hyperliquid_mainnet_url", &json!("http://127.0.0.1:1"))
            .unwrap());
        assert!(db
            .set_config_value("hyperliquid_testnet_url", &json!("http://127.0.0.1:1"))
            .unwrap());
        assert!(db
            .set_config_value("hyperliquid_info_url", &json!("http://127.0.0.1:1/info"))
            .unwrap());
        assert!(db
            .set_config_value(
                "hyperliquid_testnet_info_url",
                &json!("http://127.0.0.1:1/info"),
            )
            .unwrap());
        let mut exec = ExecutionEngine::new(db.clone());
        exec.timeout_sec = 1;

        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("ADA", true),
            Some(45)
        );
        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("ada", true),
            Some(45)
        );
        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("ADA", false),
            Some(65)
        );
        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("SOL", true),
            Some(0)
        );
        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("BTC", false),
            Some(0)
        );

        assert!(exec.asset_index_cache.lock().is_empty());
        assert!(db
            .set_config_value("execution_mode", &json!("LIVE"))
            .unwrap());
        assert_eq!(
            exec.fetch_hyperliquid_universe_asset_index("ADA", false),
            None
        );
        assert_eq!(
            exec.fetch_live_hyperliquid_universe_asset_index("BTC", false),
            None
        );
    }

    #[test]
    fn test_single_wallet_two_book_scaling_perps_cap_and_pond_reconciliation() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        let scaled = db.scale_risk_caps_for_equity(956.80).unwrap();
        assert!((scaled["initial_equity_usdc"] - 956.80).abs() < 1e-6);
        assert!((scaled["baseline_risk_per_trade_usdc"] - 4.78).abs() < 1e-6);
        assert!((scaled["min_risk_per_trade_usdc"] - 2.39).abs() < 1e-6);
        assert!((scaled["max_risk_per_trade_usdc"] - 7.17).abs() < 1e-6);
        assert!((scaled["max_portfolio_heat_usdc"] - 19.14).abs() < 1e-6);
        assert!((scaled["max_single_asset_notional_usdc"] - 119.60).abs() < 1e-6);
        assert!((scaled["max_gross_leverage_notional_usdc"] - 717.60).abs() < 1e-6);
        assert!((scaled["pond_baseline_risk_usdc"] - 4.78).abs() < 1e-6);
        assert!((scaled["pond_min_risk_usdc"] - 2.39).abs() < 1e-6);
        assert!((scaled["pond_max_risk_usdc"] - 7.17).abs() < 1e-6);
        assert!((scaled["pond_max_heat_usdc"] - 19.14).abs() < 1e-6);
        assert!((scaled["pond_max_cell_notional_usdc"] - 119.60).abs() < 1e-6);

        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();
        db.set_config_value("pond_execution_mode", &json!("TESTNET"))
            .unwrap();
        let mut snap = db.get_latest_portfolio_snapshot().unwrap();
        snap.total_equity_usdc = 956.80;
        snap.basis_harvest_notional_usdc = 429.93;
        snap.available_cash_usdc = 526.87;
        db.save_portfolio_snapshot(&snap).unwrap();

        let (tot_eq, perps_eq, vault_eq, deployed) = db.get_wallet_perps_margin_state().unwrap();
        assert!((tot_eq - 956.80).abs() < 1e-6);
        assert!((perps_eq - 526.87).abs() < 1e-6);
        assert!((vault_eq - 429.93).abs() < 1e-6);
        assert!((deployed - 0.0).abs() < 1e-6);

        let phantom_cell = aegis_core::PondCell {
            cell_id: 0,
            symbol: "JUP/USDC:USDC".to_string(),
            champion_id: 1,
            state: "OPEN".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(1.0),
            quantity: Some(100.0),
            notional_usdc: Some(100.0),
            risk_budget_usdc: Some(4.78),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.5),
            funding_rate_entry: Some(0.0005),
            initial_stop: Some(1.1),
            chandelier_stop: Some(1.1),
            highest_price_seen: Some(1.0),
            lowest_price_seen: Some(1.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: None,
            resting_order_id: None,
            opened_at: Some("2026-11-05T00:00:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-11-05T00:00:00Z".to_string(),
        };
        db.upsert_pond_cell(&phantom_cell).unwrap();
        assert!(db
            .get_all_open_wallet_symbols()
            .unwrap()
            .contains("JUP/USDC:USDC"));

        let exec = ExecutionEngine::new(db.clone());
        let empty_on_chain = HashMap::new();

        let resting_orders = vec![json!({"coin": "JUP", "oid": 99999})];
        let recon_resting =
            exec.reconcile_exchange_state(Some(&empty_on_chain), Some(&resting_orders));
        assert_eq!(recon_resting.status, "RECONCILED");
        assert_eq!(recon_resting.discrepancies_found, 0);
        assert_eq!(db.get_open_pond_cells().unwrap().len(), 1);

        let recon = exec.reconcile_exchange_state(Some(&empty_on_chain), None);
        assert_eq!(recon.status, "RECONCILED");
        assert_eq!(recon.discrepancies_found, 1);
        assert_eq!(db.get_open_pond_cells().unwrap().len(), 0);
        let closed_cells = db.get_pond_cells(10).unwrap();
        assert_eq!(closed_cells[0].state, "CLOSED");
        assert_eq!(
            closed_cells[0].exit_reason.as_deref(),
            Some("RECONCILED_CLOSED_ON_EXCHANGE")
        );

        let risk = RiskEngine::new(db.clone());
        let (budget, mult) = risk.compute_conviction_risk_budget(0.5, 478.40, None);
        assert!((budget - 4.78).abs() < 1e-6);
        assert!((mult - 1.0).abs() < 1e-6);

        let fill = exec.execute_pond_order(
            "JUP/USDC:USDC",
            "SELL",
            100.0,
            1.0,
            0.0001,
            0.1,
            1.01,
            0.99,
            50_000.0,
            false,
            true,
            false,
            None,
        );
        assert_eq!(fill.fsm_state, "REJECTED");
    }

    #[test]
    fn test_hyperliquid_order_ack_parsing_and_wire_formatting_nh7_nh9_nm9() {
        // 1. Missing avgPx/totalSz in filled response must NOT fabricate FILLED (NH-7)
        let missing_fill_resp = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        { "filled": { "oid": 777 } }
                    ]
                }
            }
        });
        let res_missing = parse_hyperliquid_order_ack(
            &missing_fill_resp,
            "SOL/USDC:USDC",
            "LONG",
            5.0,
            180.0,
            true,
            1.5,
            "coid-1",
        );
        assert_eq!(res_missing.fsm_state, "OPEN_ON_BOOK");
        assert_eq!(res_missing.filled_units, 0.0);
        assert_eq!(
            res_missing.fill_type,
            "HL_ACK_MISSING_FILL_DETAILS_PENDING_RECONCILE"
        );

        // 2. Unrecognized / empty statuses on status=="ok" must NOT return REJECTED (NH-7)
        let empty_statuses_resp = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": []
                }
            }
        });
        let res_empty = parse_hyperliquid_order_ack(
            &empty_statuses_resp,
            "SOL/USDC:USDC",
            "LONG",
            5.0,
            180.0,
            true,
            1.5,
            "coid-2",
        );
        assert_eq!(res_empty.fsm_state, "OPEN_ON_BOOK");
        assert_eq!(
            res_empty.fill_type,
            "HL_OK_UNRECOGNIZED_STATUS_PENDING_RECONCILE"
        );

        // 3. Valid filled response computes fee_bps when fee field is absent (NM-9)
        let valid_fill_resp = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        { "filled": { "oid": 888, "avgPx": "180.0", "totalSz": "2.0" } }
                    ]
                }
            }
        });
        let res_valid = parse_hyperliquid_order_ack(
            &valid_fill_resp,
            "SOL/USDC:USDC",
            "LONG",
            2.0,
            180.0,
            false,
            4.5,
            "coid-3",
        );
        assert_eq!(res_valid.fsm_state, "FILLED");
        assert_eq!(res_valid.exchange_order_id, "hl-888");
        assert!((res_valid.fees_paid_usdc - 0.162).abs() < 1e-6);

        // 4. Wire formatting enforces 5 sig figs and (6 - szDecimals) decimals (NH-9)
        assert_eq!(format_hl_wire_price(95123.456, 0.1, 4), "95123");
        assert_eq!(format_hl_wire_price(185.1234, 0.01, 2), "185.12");
        assert_eq!(format_hl_wire_size(12.3456, 0.01, 2), "12.35");
    }

    #[test]
    fn test_live_subaccount_transfer_never_falls_back_to_virtual_ledger_nb1_nm6() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        db.set_config_value("execution_mode", &json!("LIVE")).unwrap();
        let pk = "0x0123456789012345678901234567890123456789012345678901234567890123";
        let derived_wallet = aegis_core::derive_evm_address_from_private_key(pk).unwrap();
        db.set_config_value("wallet_address", &json!(derived_wallet))
            .unwrap();
        db.set_config_value(
            "pond_subaccount_address",
            &json!("0x2222222222222222222222222222222222222222"),
        )
        .unwrap();

        let exec = ExecutionEngine::new(db.clone());
        // Without signing key in LIVE mode with subaccount configured, must return Err, never VIRTUAL_SINGLE_WALLET_LEDGER
        let err = exec
            .execute_subaccount_transfer("PIT", "POND", 100.0, false)
            .unwrap_err();
        assert!(err.to_string().contains("Missing signing key"));

        // With signing key but unreachable exchange endpoint, must also return Err, never VIRTUAL_SINGLE_WALLET_LEDGER
        db.set_config_value("hyperliquid_mainnet_url", &json!("https://127.0.0.1:1"))
            .unwrap();
        db.set_config_value("private_key", &json!(pk)).unwrap();
        let err_http = exec
            .execute_subaccount_transfer("PIT", "POND", 100.0, false)
            .unwrap_err();
        assert!(err_http.to_string().contains("subAccountTransfer"));
    }

    #[test]
    fn test_resting_order_status_does_not_misattribute_existing_position_nh6() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();
        db.set_config_value(
            "wallet_address",
            &json!("0x1111111111111111111111111111111111111111"),
        )
        .unwrap();
        db.set_config_value("hyperliquid_testnet_url", &json!("https://127.0.0.1:1"))
            .unwrap();
        let exec = ExecutionEngine::with_timeout(db.clone(), 1);

        // R3-H4: When /info endpoint is unreachable, must report UNKNOWN (never GONE)
        let unreachable_status =
            exec.check_resting_order_status(Some("hl-123456"));
        assert_eq!(unreachable_status.state, "UNKNOWN");

        // Spin up a local mock /info server that returns empty openOrders, empty userFills, and empty orderStatus
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let stop_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_clone = stop_flag.clone();
        let handle = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed)
                && std::time::Instant::now() < deadline
            {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let _ =
                            stream.set_read_timeout(Some(std::time::Duration::from_millis(200)));
                        let mut buf = [0u8; 4096];
                        let mut total = Vec::new();
                        while let Ok(n) = stream.read(&mut buf) {
                            if n == 0 {
                                break;
                            }
                            total.extend_from_slice(&buf[..n]);
                            if let Some(hdr_end) =
                                total.windows(4).position(|w| w == b"\r\n\r\n")
                            {
                                let body_start = hdr_end + 4;
                                if total.len() > body_start {
                                    break;
                                }
                            }
                        }
                        let body = "[]";
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(resp.as_bytes());
                        let _ = stream.flush();
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let _ = stream.read(&mut buf);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        db.set_config_value(
            "hyperliquid_testnet_url",
            &json!(format!("http://{addr}")),
        )
        .unwrap();

        // NH-6 & R3-H4: When /info is reachable and order is absent from both openOrders and userFills, it must report GONE
        let status = exec.check_resting_order_status(Some("hl-123456"));
        stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
        handle.join().unwrap();
        assert_eq!(status.state, "GONE");
        assert_eq!(status.filled_units, 0.0);
    }

    #[test]
    fn test_short_private_key_rejected_nm11() {
        let err = sign_l1_action(
            "0x1234",
            &json!({"type": "order"}),
            None,
            1700000000000,
            None,
            true,
        )
        .unwrap_err();
        assert!(err.to_string().contains("64 hex characters"));
    }

    #[test]
    fn test_nh7_nh8_nm9_exchange_ack_parsing_partial_fills_and_live_fees() {
        test_hyperliquid_order_ack_parsing_and_wire_formatting_nh7_nh9_nm9();
    }

    #[test]
    fn test_nh9_wire_rounding_5_sig_figs_and_sz_decimals() {
        assert_eq!(format_hl_wire_price(95123.456, 0.1, 4), "95123");
        assert_eq!(format_hl_wire_price(185.1234, 0.01, 2), "185.12");
        assert_eq!(format_hl_wire_size(12.3456, 0.01, 2), "12.35");
    }

    #[test]
    fn test_r3_b4_reconcile_exchange_state_preserves_armed_pond_orders() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        let armed_cell = aegis_core::PondCell {
            cell_id: 0,
            symbol: "SOL/USDC:USDC".to_string(),
            champion_id: 1,
            state: "ARMED".to_string(),
            cycle_opened_id: None,
            side: "LONG".to_string(),
            option_analogue: "SHORT_PUT".to_string(),
            entry_price: Some(180.0),
            quantity: Some(2.0),
            notional_usdc: Some(360.0),
            risk_budget_usdc: Some(5.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(-2.0),
            funding_rate_entry: Some(-0.0002),
            initial_stop: Some(171.0),
            chandelier_stop: Some(171.0),
            highest_price_seen: Some(180.0),
            lowest_price_seen: Some(180.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: None,
            resting_order_id: Some("hl-777888".to_string()),
            opened_at: Some("2026-11-08T00:00:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-11-08T00:00:00Z".to_string(),
        };
        db.upsert_pond_cell(&armed_cell).unwrap();

        let exec = ExecutionEngine::new(db.clone());
        let empty_on_chain = HashMap::new();
        // Resting order is present on Hyperliquid book matching ARMED Pond cell oid 777888
        let resting_orders = vec![json!({"coin": "SOL", "oid": 777888})];
        let recon = exec.reconcile_exchange_state(Some(&empty_on_chain), Some(&resting_orders));
        assert_eq!(recon.status, "RECONCILED");
        // Must NOT cancel or flag the ARMED Pond resting order as an orphaned order!
        assert_eq!(recon.discrepancies_found, 0);
        assert!(recon.discrepancies.is_empty());
    }

    #[test]
    fn test_r4_n6_n9_h2_utc_day_volume_median_and_subaccount_resume_guard() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let exec = ExecutionEngine::new(db.clone());

        // R4-H2: Dry-run virtual-ledger transfer prefixes tx_hash with "dryrun-0x"
        let dry_res = exec
            .execute_subaccount_transfer("PIT", "POND", 250.0, true)
            .unwrap();
        assert_eq!(dry_res.status, "DRY_RUN_SIMULATED");
        assert!(dry_res.tx_hash.starts_with("dryrun-0x"));
        assert!(dry_res.dry_run);

        let real_virt_res = exec
            .execute_subaccount_transfer("PIT", "POND", 250.0, false)
            .unwrap();
        assert_eq!(real_virt_res.status, "EXECUTED");
        assert!(real_virt_res.tx_hash.starts_with("0x"));
        assert!(!real_virt_res.dry_run);

        // R4-N9: Calling execute_subaccount_transfer_for_record when already_completed >= legs.len()
        // returns CONFIRMED immediately without making any HTTP call.
        db.set_config_value("execution_mode", &json!("LIVE")).unwrap();
        let pk = "0x0123456789012345678901234567890123456789012345678901234567890123";
        let derived_wallet = aegis_core::derive_evm_address_from_private_key(pk).unwrap();
        db.set_config_value("wallet_address", &json!(derived_wallet))
            .unwrap();
        db.set_config_value("private_key", &json!(pk)).unwrap();
        db.set_config_value(
            "pit_subaccount_address",
            &json!("0x1111111111111111111111111111111111111111"),
        )
        .unwrap();
        db.set_config_value(
            "pond_subaccount_address",
            &json!("0x2222222222222222222222222222222222222222"),
        )
        .unwrap();
        db.set_config_value("hyperliquid_mainnet_url", &json!("https://127.0.0.1:1"))
            .unwrap();

        let tid = db
            .insert_book_transfer_with_mode(
                &aegis_core::BookTransfer {
                    transfer_id: 0,
                    settle_date: "2026-11-08T00:05:00Z".to_string(),
                    from_book: "PIT".to_string(),
                    to_book: "POND".to_string(),
                    amount_usdc: 200.0,
                    pre_from_usdc: Some(5200.0),
                    pre_to_usdc: Some(4800.0),
                    post_from_usdc: Some(5200.0),
                    post_to_usdc: Some(4800.0),
                    drift_bps_before: Some(200.0),
                    drift_bps_after: Some(200.0),
                    blocked_remainder_usdc: 0.0,
                    status: "PENDING".to_string(),
                    attempts: 1,
                    tx_ref: None,
                    created_at: "2026-11-08T00:05:00Z".to_string(),
                },
                "LIVE",
            )
            .unwrap();
        db.update_book_transfer_leg_progress(tid, 2, Some("0xleg1"), Some("0xleg2"))
            .unwrap();

        let confirmed = exec
            .execute_subaccount_transfer_for_record(Some(tid), "PIT", "POND", 200.0, false)
            .unwrap();
        assert_eq!(confirmed.status, "CONFIRMED");
        assert_eq!(confirmed.tx_hash, "0xleg2");
        assert_eq!(confirmed.legs_executed, 2);

        // R4-N6: UTC calendar-day bucketing for median daily volume handles missing hourly bar gaps cleanly
        // Day 1 (UTC day 20000): 22 bars (2 hours missing due to API outage), each with 10_000 USDC notional => 220_000 USDC
        // Day 2 (UTC day 20001): 24 bars, each with 20_000 USDC notional => 480_000 USDC
        // Day 3 (UTC day 20002): 24 bars, each with 30_000 USDC notional => 720_000 USDC
        // Median of [220_000, 480_000, 720_000] = 480_000 USDC (no cross-day sliding window contamination)
        let mut bars = Vec::new();
        let day1_base_ms = 20_000 * 86_400_000i64;
        for h in 0..24 {
            if h == 5 || h == 6 {
                continue; // 2-hour outage gap on Day 1
            }
            bars.push(aegis_core::OhlcvBar {
                symbol: "SOL/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: day1_base_ms + h * 3_600_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 100.0, // 100 * 100 = 10_000 USDC/hr
                funding_rate_hourly: 0.0001,
                is_synthetic: 0,
            });
        }
        let day2_base_ms = 20_001 * 86_400_000i64;
        for h in 0..24 {
            bars.push(aegis_core::OhlcvBar {
                symbol: "SOL/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: day2_base_ms + h * 3_600_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 200.0, // 200 * 100 = 20_000 USDC/hr
                funding_rate_hourly: 0.0001,
                is_synthetic: 0,
            });
        }
        let day3_base_ms = 20_002 * 86_400_000i64;
        for h in 0..24 {
            bars.push(aegis_core::OhlcvBar {
                symbol: "SOL/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: day3_base_ms + h * 3_600_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 300.0, // 300 * 100 = 30_000 USDC/hr
                funding_rate_hourly: 0.0001,
                is_synthetic: 0,
            });
        }
        db.upsert_ohlcv_batch(&bars).unwrap();
        let ingestion = MarketIngestionEngine::new(db.clone());
        let median_vol =
            ingestion.compute_median_daily_volume_from_bars("SOL/USDC:USDC", 50_000.0, 7);
        assert!((median_vol - 480_000.0).abs() < 1e-6);
    }

    #[test]
    fn test_orphan_position_stop_reconciliation_and_manual_take_profit() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let exec = ExecutionEngine::new(db.clone());

        // 1. Reconcile an orphan SHORT position on TAO/USDC:USDC (not in Pit universe_assets)
        db.reconcile_on_chain_orphan_position(
            "TAO/USDC:USDC",
            "TAO",
            "SHORT",
            0.39,
            317.85,
            305.00,
            5.01,
        )
        .unwrap();

        let open_trades = db.get_open_trades().unwrap();
        let tao = open_trades
            .iter()
            .find(|t| t.symbol == "TAO/USDC:USDC")
            .expect("orphan TAO trade created");
        // Verify initial stop and chandelier stop are tight around mark_price (305.00), NOT +50% (476.78)
        assert!(
            tao.chandelier_stop_price > 305.00 && tao.chandelier_stop_price < 310.00,
            "Orphan chandelier stop {} should be tight (~0.8% above mark 305.00)",
            tao.chandelier_stop_price
        );

        // 2. Insert 1h bars for TAO/USDC:USDC with latest close = 299.10 (no 4h bars needed due to 1h fallback)
        let mut tao_bars = Vec::new();
        let base_ts = 1_762_000_000_000i64;
        for i in 0..20 {
            let close = if i == 19 { 299.10 } else { 305.00 };
            tao_bars.push(aegis_core::OhlcvBar {
                symbol: "TAO/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: base_ts + i * 3_600_000,
                open: close,
                high: close + 1.5,
                low: close - 1.5,
                close,
                volume: 1000.0,
                funding_rate_hourly: 0.0,
                is_synthetic: 0,
            });
        }
        db.upsert_ohlcv_batch(&tao_bars).unwrap();

        // Run reconcile_cycle_with_options(&[], false, true) (stops-only mode) and verify TAO/USDC:USDC is evaluated
        let _snapshot = exec.reconcile_cycle_with_options(&[], false, true).unwrap();

        let open_after = db.get_open_trades().unwrap();
        let tao_after = open_after
            .iter()
            .find(|t| t.symbol == "TAO/USDC:USDC")
            .expect("TAO still open");
        assert!(
            (tao_after.current_price - 299.10).abs() < 1e-6,
            "Orphan trade current_price must update to 299.10 (got {})",
            tao_after.current_price
        );
        let expected_upnl = (317.85 - 299.10) * 0.39;
        assert!(
            (tao_after.unrealized_pnl_usdc - expected_upnl).abs() < 1e-3,
            "Orphan trade uPnL must update to {:.4} (got {:.4})",
            expected_upnl,
            tao_after.unrealized_pnl_usdc
        );
        assert!(
            tao_after.chandelier_stop_price > 299.10 && tao_after.chandelier_stop_price < 302.00,
            "Orphan trade chandelier_stop_price must ratchet tightly above 299.10 (got {})",
            tao_after.chandelier_stop_price
        );

        // 3. Manual Take Profit closes TAO/USDC:USDC
        let close_res = exec
            .close_open_trade_manual(tao_after.trade_id, None)
            .expect("manual take profit succeeds");
        assert_eq!(close_res["status"], "CLOSED");
        assert_eq!(close_res["exit_reason"], "MANUAL_TAKE_PROFIT");
        assert!((close_res["exit_price"].as_f64().unwrap() - 299.10).abs() < 1e-6);

        // Edge case: attempting to close the same trade again must return an error
        assert!(
            exec.close_open_trade_manual(tao_after.trade_id, None)
                .is_err(),
            "Closing an already closed trade must return an error"
        );
        // Edge case: non-existent trade_id must return an error
        assert!(
            exec.close_open_trade_manual(999_999, None).is_err(),
            "Closing a non-existent trade must return an error"
        );

        // 4. Verify sync_funding_payments recomputes unrealized_pnl_usdc from mark_px instead of keeping stale uPnL
        let tao_long_id = db
            .reconcile_on_chain_orphan_position(
                "TAO/USDC:USDC",
                "TAO",
                "LONG",
                0.548,
                218.12,
                224.24,
                3.3538, // initially positive at 224.24
            )
            .unwrap();
        let next_hour_ts = base_ts + 20 * 3_600_000;
        db.upsert_ohlcv_batch(&[aegis_core::OhlcvBar {
            symbol: "TAO/USDC:USDC".to_string(),
            timeframe: "1h".to_string(),
            timestamp_ms: next_hour_ts,
            open: 204.20,
            high: 205.00,
            low: 203.50,
            close: 204.20,
            volume: 1200.0,
            funding_rate_hourly: 0.0001,
            is_synthetic: 0,
        }])
        .unwrap();
        let funding_report = exec.sync_funding_payments(Some(next_hour_ts), true);
        assert_eq!(funding_report.positions_updated, 1);
        let open_after_funding = db.get_open_trades().unwrap();
        let tao_long = open_after_funding
            .iter()
            .find(|t| t.trade_id == tao_long_id)
            .expect("TAO LONG still open");
        let expected_long_upnl = aegis_core::round_dp((204.20 - 218.12) * 0.548, 4);
        assert!(
            (tao_long.unrealized_pnl_usdc - expected_long_upnl).abs() < 1e-4,
            "sync_funding_payments must update unrealized_pnl_usdc to {} (got {})",
            expected_long_upnl,
            tao_long.unrealized_pnl_usdc
        );

        // 5. Verify close_open_pond_cell_manual cancels an ARMED Pond cell cleanly
        let armed_cell_id = db
            .upsert_pond_cell(&aegis_core::PondCell {
                cell_id: 0,
                symbol: "NEAR/USDC:USDC".to_string(),
                champion_id: 1,
                state: "ARMED".to_string(),
                cycle_opened_id: None,
                side: "SHORT".to_string(),
                option_analogue: "SHORT_CALL".to_string(),
                entry_price: Some(1.45),
                quantity: Some(500.0),
                notional_usdc: Some(725.0),
                risk_budget_usdc: Some(25.0),
                conviction_multiplier: Some(1.0),
                funding_z_entry: Some(0.0),
                funding_rate_entry: Some(0.0),
                initial_stop: Some(1.48),
                chandelier_stop: Some(1.48),
                highest_price_seen: Some(1.45),
                lowest_price_seen: Some(1.45),
                funding_accrued_usdc: 0.0,
                realized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.0,
                exit_price: None,
                exit_reason: None,
                opened_at: None,
                closed_at: None,
                cooldown_until: None,
                updated_at: "2026-11-09T09:00:00Z".to_string(),
                resting_order_id: Some("hl-paper-armed-1".to_string()),
            })
            .unwrap();
        let armed_close = exec
            .close_open_pond_cell_manual(armed_cell_id, None)
            .expect("closing ARMED Pond cell succeeds");
        assert_eq!(armed_close["status"], "CANCELLED_ARMED");
        assert!(db.get_armed_pond_cells().unwrap().is_empty());

        // 6. R7-N3 & R7-N5: build_orphan_or_pond_universe_asset preserves positive min_notional_usdc < 10.0
        let pepe_row = aegis_core::PondUniverseRow {
            symbol: "PEPE/USDC:USDC".to_string(),
            rank: 42,
            median_daily_volume_usdc: 5_000_000.0,
            tick_size: 1e-8,
            lot_step_size: 1.0,
            min_notional_usdc: 5.0,
            max_leverage: 5.0,
            status: "ACTIVE".to_string(),
            warmup_until: None,
            delist_streak: 0,
            added_at: "2026-11-09T00:00:00Z".to_string(),
            delisted_at: None,
            reference_price: 0.00001234,
            is_synthetic_volume: 0,
        };
        let pepe_asset = build_orphan_or_pond_universe_asset(
            "PEPE/USDC:USDC",
            "PEPE",
            0.00001234,
            Some(&pepe_row),
            "POND",
        );
        assert!(
            (pepe_asset.min_notional_usdc - 5.0).abs() < 1e-9,
            "build_orphan_or_pond_universe_asset must preserve min_notional_usdc = 5.0 (< 10.0) from pond_universe (R7-N3, R7-N5)"
        );
        assert_eq!(pepe_asset.lot_step_size, 1.0);
        assert!((pepe_asset.tick_size - 1e-8).abs() < 1e-12);

        // 7. R7-S4: In TESTNET/LIVE mode without valid live exchange fills or confirmed exchange absence,
        // manual close of a non-paper trade or Pond cell MUST return an error and keep the position OPEN locally.
        db.set_config_value("execution_mode", &serde_json::json!("TESTNET"))
            .unwrap();
        db.set_config_value("pond_execution_mode", &serde_json::json!("TESTNET"))
            .unwrap();
        let live_err = exec.close_open_trade_manual(tao_long_id, Some("MANUAL_TAKE_PROFIT"));
        assert!(
            live_err.is_err(),
            "Manual close in TESTNET mode must fail when exchange close/absence cannot be confirmed (R7-S4)"
        );
        assert!(
            db.get_open_trades()
                .unwrap()
                .iter()
                .any(|t| t.trade_id == tao_long_id),
            "Trade must remain OPEN in local DB when TESTNET exchange close fails (R7-S4)"
        );

        let live_cell_id = db
            .upsert_pond_cell(&aegis_core::PondCell {
                cell_id: 0,
                symbol: "NEAR/USDC:USDC".to_string(),
                champion_id: 1,
                state: "OPEN".to_string(),
                cycle_opened_id: None,
                side: "SHORT".to_string(),
                option_analogue: "SHORT_CALL".to_string(),
                entry_price: Some(1.45),
                quantity: Some(500.0),
                notional_usdc: Some(725.0),
                risk_budget_usdc: Some(25.0),
                conviction_multiplier: Some(1.0),
                funding_z_entry: Some(0.0),
                funding_rate_entry: Some(0.0),
                initial_stop: Some(1.48),
                chandelier_stop: Some(1.48),
                highest_price_seen: Some(1.45),
                lowest_price_seen: Some(1.45),
                funding_accrued_usdc: 0.0,
                realized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.0,
                exit_price: None,
                exit_reason: None,
                opened_at: Some("2026-11-09T09:00:00Z".to_string()),
                closed_at: None,
                cooldown_until: None,
                updated_at: "2026-11-09T09:00:00Z".to_string(),
                resting_order_id: Some("123456789".to_string()), // non-paper order ID
            })
            .unwrap();
        let live_cell_err = exec.close_open_pond_cell_manual(live_cell_id, None);
        assert!(
            live_cell_err.is_err(),
            "Manual Pond cell close in TESTNET mode must fail when exchange close/absence cannot be confirmed (R7-S4)"
        );
        assert!(
            db.get_open_pond_cells()
                .unwrap()
                .iter()
                .any(|c| c.cell_id == live_cell_id),
            "Pond cell must remain OPEN in local DB when TESTNET exchange close fails (R7-S4)"
        );
        assert!(
            exec.resolved_asset_cache
                .lock()
                .contains_key("TAO/USDC:USDC"),
            "ExecutionEngine::resolved_asset_cache must cache resolved orphan/universe assets (R7-N3)"
        );
    }

    #[test]
    fn test_dangling_order_cancelled_on_partial_fill_and_pit_sizing_ignores_unallocated_spot_usdc() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();
        db.set_config_value("hyperliquid_testnet", &json!(true))
            .unwrap();
        db.scale_risk_caps_for_equity(1000.0).unwrap();

        let exec = ExecutionEngine::new(db.clone());

        // 1. Suppose an untracked resting order (oid=64920976942) on NEAR partially filled (-23.1 NEAR short on-chain).
        // Even though `ex_positions` contains "NEAR" (and even after "NEAR" is imported into `local_trades`),
        // `reconcile_exchange_state` MUST flag and cancel the untracked resting order (`!is_known_resting_oid`).
        let mut on_chain = HashMap::new();
        on_chain.insert(
            "NEAR".to_string(),
            json!({"szi": -23.1, "entryPx": 3.612, "markPx": 3.612, "unrealizedPnl": 0.0}),
        );
        let open_orders = vec![json!({"coin": "NEAR", "oid": 64920976942_u64})];

        let rep1 = exec.reconcile_exchange_state(Some(&on_chain), Some(&open_orders));
        assert_eq!(rep1.status, ReconcileStatus::Reconciled);
        assert!(
            rep1.discrepancies
                .iter()
                .any(|d| d.get("type").and_then(|v| v.as_str()) == Some("DANGLING_OPEN_ORDER_CANCELLED")
                    && d.get("base_asset").and_then(|v| v.as_str()) == Some("NEAR")),
            "Untracked resting order on coin with partial-fill position must be cancelled on tick 1"
        );

        // Tick 2: NEAR is now in `local_trades` (`seen_coins` contains "NEAR") and position grew to -115.2 NEAR.
        // Because oid 64920976942 is NOT a known resting order (`known_resting_oids`), it must still be cancelled!
        on_chain.insert(
            "NEAR".to_string(),
            json!({"szi": -115.2, "entryPx": 3.612, "markPx": 3.612, "unrealizedPnl": 0.0}),
        );
        let rep2 = exec.reconcile_exchange_state(Some(&on_chain), Some(&open_orders));
        assert!(
            rep2.discrepancies
                .iter()
                .any(|d| d.get("type").and_then(|v| v.as_str()) == Some("DANGLING_OPEN_ORDER_CANCELLED")
                    && d.get("base_asset").and_then(|v| v.as_str()) == Some("NEAR")),
            "Untracked resting order on coin with existing local trade + exchange position must still be cancelled"
        );

        // Close the reconciled NEAR trade so we can test tracked resting order edge cases and new entry sizing cleanly
        let open_trades = db.get_open_trades().unwrap();
        for t in open_trades {
            db.close_trade(t.trade_id, 3.612, 0.0, 0.0, "TEST_CLEANUP")
                .unwrap();
        }

        // 1b. Suppose a tracked resting PIT order (status = PENDING, oid = hl-11111) exists on SOL:
        // - Before any fill (ex_positions is empty), if open_orders has both tracked oid 11111 AND untracked oid 22222 on SOL,
        //   oid 11111 must be spared while untracked oid 22222 MUST be cancelled as DANGLING_OPEN_ORDER_CANCELLED.
        // - When oid 11111 partially fills on-chain while still resting in open_orders, reconcile_exchange_state must NOT
        //   import a duplicate ORPHAN_ON_CHAIN_IMPORTED trade and must NOT cancel oid 11111.
        let tracked_resting = aegis_core::TradeRecord {
            trade_id: 0,
            client_order_id: "hl-tracked-sol".to_string(),
            symbol: "SOL/USDC:USDC".to_string(),
            base_asset: "SOL".to_string(),
            side: "SHORT".to_string(),
            status: "PENDING".to_string(),
            fsm_state: "OPEN_ON_BOOK".to_string(),
            entry_price: 87.358,
            current_price: 87.358,
            exit_price: None,
            quantity: 1.37,
            filled_units: 0.0,
            avg_fill_price: None,
            notional_usdc: 119.68,
            risk_budget_usdc: 5.0,
            conviction_multiplier: 1.0,
            initial_stop_price: 91.0,
            chandelier_stop_price: 91.0,
            current_stop_price: 91.0,
            highest_price_seen: 87.358,
            lowest_price_seen: 87.358,
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some("hl-11111".to_string()),
            opened_at: "2026-11-09T16:08:51Z".to_string(),
            closed_at: None,
            exit_reason: None,
        };
        let tracked_tid = db.insert_trade(&tracked_resting).unwrap();

        let empty_ex = HashMap::new();
        let orders_mixed = vec![
            json!({"coin": "SOL", "oid": 11111_u64}),
            json!({"coin": "SOL", "oid": 22222_u64}),
        ];
        let rep_mixed = exec.reconcile_exchange_state(Some(&empty_ex), Some(&orders_mixed));
        assert_eq!(rep_mixed.discrepancies.len(), 1);
        assert_eq!(
            rep_mixed.discrepancies[0].get("type").and_then(|v| v.as_str()),
            Some("DANGLING_OPEN_ORDER_CANCELLED")
        );
        assert_eq!(
            rep_mixed.discrepancies[0].get("oid").and_then(|v| v.as_u64()),
            Some(22222_u64)
        );

        let mut partial_sol_ex = HashMap::new();
        partial_sol_ex.insert(
            "SOL".to_string(),
            json!({"szi": -0.50, "entryPx": 87.358, "markPx": 87.358, "unrealizedPnl": 0.0}),
        );
        let orders_tracked_only = vec![json!({"coin": "SOL", "oid": 11111_u64})];
        let rep_partial =
            exec.reconcile_exchange_state(Some(&partial_sol_ex), Some(&orders_tracked_only));
        assert_eq!(
            rep_partial.discrepancies.len(),
            0,
            "Tracked resting order with partial fill on-chain must not trigger ORPHAN_ON_CHAIN_IMPORTED or DANGLING_OPEN_ORDER_CANCELLED"
        );
        assert!(db.get_open_trades().unwrap().is_empty());

        db.cancel_resting_trade(tracked_tid, "TEST_CLEANUP")
            .unwrap();

        // 1c. A historical CLOSED/REJECTED Pond cell that still has a leftover `resting_order_id`
        // must NOT pollute `known_resting_oids` or `seen_coins` in `reconcile_exchange_state`.
        db.upsert_pond_cell(&aegis_core::PondCell {
            cell_id: 0,
            symbol: "AVAX/USDC:USDC".to_string(),
            champion_id: 1,
            state: "REJECTED".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(25.0),
            quantity: Some(5.0),
            notional_usdc: Some(125.0),
            risk_budget_usdc: Some(5.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.5),
            funding_rate_entry: Some(0.0005),
            initial_stop: Some(26.5),
            chandelier_stop: Some(26.5),
            highest_price_seen: Some(25.0),
            lowest_price_seen: Some(25.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: Some("RESTING_OID:33333".to_string()),
            resting_order_id: Some("hl-33333".to_string()),
            opened_at: None,
            closed_at: Some("2026-11-09T16:00:00Z".to_string()),
            cooldown_until: None,
            updated_at: "2026-11-09T16:00:00Z".to_string(),
        })
        .unwrap();
        let rep_stale_pond = exec.reconcile_exchange_state(
            Some(&empty_ex),
            Some(&[json!({"coin": "AVAX", "oid": 33333_u64})]),
        );
        assert!(
            rep_stale_pond.discrepancies.iter().any(|d| {
                d.get("type").and_then(|v| v.as_str()) == Some("DANGLING_OPEN_ORDER_CANCELLED")
                    && d.get("oid").and_then(|v| v.as_u64()) == Some(33333_u64)
            }),
            "Stale REJECTED/CLOSED Pond cell with leftover resting_order_id must not shield oid 33333 from dangling-order cancellation"
        );

        // 2. Pollute `portfolio_snapshots.total_equity_usdc` with $10,972.25 (simulating $10,517.65 unallocated Testnet faucet Spot USDC)
        // while `initial_equity_usdc = 1000.0` ($500 Book A, $125 single-asset cap).
        let mut polluted_snap = db.get_latest_portfolio_snapshot().unwrap();
        polluted_snap.total_equity_usdc = 10_972.25;
        polluted_snap.available_cash_usdc = 10_972.25;
        db.save_portfolio_snapshot(&polluted_snap).unwrap();

        // Seed 30 1H bars for SOL/USDC:USDC around $100.0
        let base_ts = 1_760_000_000_000_i64;
        let mut bars = Vec::new();
        for i in 0..30 {
            bars.push(aegis_core::OhlcvBar {
                symbol: "SOL/USDC:USDC".to_string(),
                timeframe: "1h".to_string(),
                timestamp_ms: base_ts + i * 3_600_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 100_000.0,
                funding_rate_hourly: 0.0,
                is_synthetic: 0,
            });
        }
        db.upsert_ohlcv_batch(&bars).unwrap();

        // Run `reconcile_cycle_with_options` in PAPER mode (with `platform_pp50_enabled = false`, `pond_enabled = false`, `risk_caps_reference_scope = BOOK`)
        db.set_config_value("execution_mode", &json!("PAPER"))
            .unwrap();
        db.set_config_value("platform_pp50_enabled", &json!(false))
            .unwrap();
        db.set_config_value("pond_enabled", &json!(false)).unwrap();
        let eval = aegis_core::SignalEvaluation {
            eval_id: None,
            timestamp_ms: base_ts + 29 * 14_400_000,
            base_asset: "SOL".to_string(),
            exec_symbol: "SOL/USDC:USDC".to_string(),
            synth_symbol: "SOL/BTC".to_string(),
            hmm_regime: "BEAR".to_string(),
            prob_bull: 0.05,
            prob_bear: 0.90,
            prob_chop: 0.05,
            snr_score: -1.5,
            snr_synth: -1.5,
            rs_96: -0.05,
            r_usdc_8: -0.02,
            mace_score: -0.85,
            blended_conviction: -0.85,
            rs_rank: 15,
            signal_action: "SHORT".to_string(),
            funding_rate_hourly: 0.0,
            created_at: None,
        };
        let snap_after = exec
            .reconcile_cycle_with_options(&[eval], false, false)
            .unwrap();
        assert!(
            (snap_after.total_equity_usdc - 1000.0).abs() < 5.0,
            "Snapshot total_equity_usdc must be anchored to ~$1,000, got {}",
            snap_after.total_equity_usdc
        );
        let opened = db.get_open_trades().unwrap();
        assert_eq!(opened.len(), 1);
        assert!(
            opened[0].notional_usdc <= 130.0,
            "Book A single-asset notional on $500 book must be capped near $125 (never 22x = $2,742), got ${}",
            opened[0].notional_usdc
        );
    }

    #[test]
    fn test_r9_s1_n2_r8_n2_same_coin_multi_row_reconciliation_and_faucet_offset() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();
        db.set_config_value("hyperliquid_testnet", &json!(true))
            .unwrap();
        let exec = ExecutionEngine::new(db.clone());

        // 1. R9-S1: Insert two OPEN PIT trades on the same coin (ETH) with 1.0 ETH each (sum = 2.0 ETH),
        // while the exchange clearinghouseState holds only 1.0 ETH total.
        // Without R9-S1 netting, each row would see expected = 1.0 ETH (leaving sum = 2.0 ETH = 2x inflation),
        // or if exchange had 1.5 ETH, each row would resize to 1.5 ETH (sum = 3.0 ETH = 2x inflation).
        let base_eth_trade = aegis_core::TradeRecord {
            trade_id: 0,
            client_order_id: "hl-eth-dup-1".to_string(),
            symbol: "ETH/USDC:USDC".to_string(),
            base_asset: "ETH".to_string(),
            side: "LONG".to_string(),
            status: "OPEN".to_string(),
            fsm_state: "FILLED".to_string(),
            entry_price: 3_000.0,
            current_price: 3_000.0,
            exit_price: None,
            quantity: 1.0,
            filled_units: 1.0,
            avg_fill_price: Some(3_000.0),
            notional_usdc: 3_000.0,
            risk_budget_usdc: 60.0,
            conviction_multiplier: 1.0,
            initial_stop_price: 2_940.0,
            chandelier_stop_price: 2_940.0,
            current_stop_price: 2_940.0,
            highest_price_seen: 3_000.0,
            lowest_price_seen: 3_000.0,
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc: 0.0,
            fees_paid_usdc: 1.0,
            funding_accrued_usdc: 0.0,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some("hl-eth-1".to_string()),
            opened_at: "2026-11-10T00:00:00Z".to_string(),
            closed_at: None,
            exit_reason: None,
        };
        let t1 = db.insert_trade(&base_eth_trade).unwrap();
        let mut eth_trade_2 = base_eth_trade.clone();
        eth_trade_2.client_order_id = "hl-eth-dup-2".to_string();
        eth_trade_2.exchange_order_id = Some("hl-eth-2".to_string());
        let t2 = db.insert_trade(&eth_trade_2).unwrap();

        // Also insert two OPEN Pond cells on SUI (100 SUI each = 200 SUI local vs 100 SUI on-chain)
        let base_sui_cell = aegis_core::PondCell {
            cell_id: 0,
            symbol: "SUI/USDC:USDC".to_string(),
            champion_id: 1,
            state: "OPEN".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(2.0),
            quantity: Some(100.0),
            notional_usdc: Some(200.0),
            risk_budget_usdc: Some(10.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.5),
            funding_rate_entry: Some(0.0005),
            initial_stop: Some(2.1),
            chandelier_stop: Some(2.1),
            highest_price_seen: Some(2.0),
            lowest_price_seen: Some(2.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.1,
            exit_price: None,
            exit_reason: None,
            resting_order_id: None,
            opened_at: Some("2026-11-10T00:00:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-11-10T00:00:00Z".to_string(),
        };
        let c1 = db.upsert_pond_cell(&base_sui_cell).unwrap();
        let c2 = db.upsert_pond_cell(&base_sui_cell).unwrap();

        let mut on_chain = HashMap::new();
        on_chain.insert(
            "ETH".to_string(),
            json!({"szi": 1.5, "entryPx": 3000.0, "markPx": 3000.0, "unrealizedPnl": 0.0}),
        );
        on_chain.insert(
            "SUI".to_string(),
            json!({"szi": -100.0, "entryPx": 2.0, "markPx": 2.0, "unrealizedPnl": 0.0}),
        );

        let rep1 = exec.reconcile_exchange_state(Some(&on_chain), Some(&[]));
        assert_eq!(rep1.status, ReconcileStatus::Reconciled);

        // Must flag DUPLICATE_SAME_COIN_OPEN_ROWS for both PIT (ETH) and POND (SUI)
        assert!(
            rep1.discrepancies.iter().any(|d| {
                d.get("type").and_then(|v| v.as_str()) == Some("DUPLICATE_SAME_COIN_OPEN_ROWS")
                    && d.get("book").and_then(|v| v.as_str()) == Some("PIT")
                    && d.get("base_asset").and_then(|v| v.as_str()) == Some("ETH")
            }),
            "reconcile_exchange_state must emit DUPLICATE_SAME_COIN_OPEN_ROWS for duplicate OPEN PIT trades (R9-S1)"
        );
        assert!(
            rep1.discrepancies.iter().any(|d| {
                d.get("type").and_then(|v| v.as_str()) == Some("DUPLICATE_SAME_COIN_OPEN_ROWS")
                    && d.get("book").and_then(|v| v.as_str()) == Some("POND")
                    && d.get("base_asset").and_then(|v| v.as_str()) == Some("SUI")
            }),
            "reconcile_exchange_state must emit DUPLICATE_SAME_COIN_OPEN_ROWS for duplicate OPEN Pond cells (R9-S1)"
        );

        // Sum of local ETH PIT trades must equal 1.5 ETH AND each 1.0 ETH row must receive an equal 0.75 ETH share
        let open_eth: Vec<_> = db
            .get_open_trades()
            .unwrap()
            .into_iter()
            .filter(|t| t.trade_id == t1 || t.trade_id == t2)
            .collect();
        let total_eth_qty: f64 = open_eth.iter().map(|t| t.quantity).sum();
        assert!(
            (total_eth_qty - 1.5).abs() < 1e-6,
            "Total local ETH PIT quantity across duplicate rows must equal exchange 1.5 ETH (never 3.0 ETH), got {total_eth_qty} (R9-S1)"
        );
        for row in &open_eth {
            assert!(
                (row.quantity - 0.75).abs() < 1e-6,
                "Equal 1.0 ETH duplicate rows must each receive 0.75 ETH proportional share, got {} (R9-S1)",
                row.quantity
            );
        }
        let total_eth_risk: f64 = open_eth.iter().map(|t| t.risk_budget_usdc).sum();
        assert!(
            (total_eth_risk - 90.0).abs() < 1e-3,
            "Total local ETH PIT risk_budget_usdc must rescale to |3000 - 2940| * 1.5 = $90.0, got {total_eth_risk} (R9-N5)"
        );

        // Sum of local SUI Pond cells must equal 100.0 SUI AND each 100.0 SUI cell must receive 50.0 SUI
        let open_sui: Vec<_> = db
            .get_open_pond_cells()
            .unwrap()
            .into_iter()
            .filter(|c| c.cell_id == c1 || c.cell_id == c2)
            .collect();
        let total_sui_qty: f64 = open_sui.iter().map(|c| c.quantity.unwrap_or(0.0)).sum();
        assert!(
            (total_sui_qty - 100.0).abs() < 1e-6,
            "Total local SUI Pond quantity across duplicate cells must equal exchange 100.0 SUI (never 200.0 SUI), got {total_sui_qty} (R9-S1)"
        );
        for cell in &open_sui {
            assert!(
                (cell.quantity.unwrap_or(0.0) - 50.0).abs() < 1e-6,
                "Equal 100.0 SUI duplicate Pond cells must each receive 50.0 SUI proportional share, got {:?} (R9-S1)",
                cell.quantity
            );
            assert!(
                (cell.risk_budget_usdc.unwrap_or(0.0) - 5.0).abs() < 1e-3,
                "Each 100.0 -> 50.0 SUI Pond cell risk_budget_usdc must rescale proportionally from $10.0 to $5.0, got {:?} (R10-S1)",
                cell.risk_budget_usdc
            );
        }
        let total_sui_risk: f64 = open_sui
            .iter()
            .map(|c| c.risk_budget_usdc.unwrap_or(0.0))
            .sum();
        assert!(
            (total_sui_risk - 10.0).abs() < 1e-3,
            "Total local SUI Pond risk_budget_usdc must rescale from $20.0 to $10.0 after 200 -> 100 SUI reconciliation, got {total_sui_risk} (R10-S1)"
        );

        // On Tick 2, size drift must be completely converged (zero SIZE_DRIFT_RECONCILED / SIZE_DRIFT_POND_CELL_RECONCILED)
        let rep2 = exec.reconcile_exchange_state(Some(&on_chain), Some(&[]));
        assert!(
            !rep2.discrepancies.iter().any(|d| {
                let ty = d.get("type").and_then(|v| v.as_str()).unwrap_or("");
                ty == "SIZE_DRIFT_RECONCILED" || ty == "SIZE_DRIFT_POND_CELL_RECONCILED"
            }),
            "Second reconcile tick must have zero size drift after same-coin multi-row convergence (R9-S1): {:?}",
            rep2.discrepancies
        );

        // R10-S1 None branch: When on-chain entryPx is 0.0 (None branch), resizing 100.0 -> 40.0 SUI (20.0 SUI each)
        // must rescale each cell's risk_budget_usdc from $5.0 to |2.0 - 2.1| * 20.0 = $2.0 (total $4.0)
        on_chain.insert(
            "SUI".to_string(),
            json!({"szi": -40.0, "entryPx": 0.0, "markPx": 2.0, "unrealizedPnl": 0.0}),
        );
        let rep_sui_none = exec.reconcile_exchange_state(Some(&on_chain), Some(&[]));
        assert_eq!(rep_sui_none.status, ReconcileStatus::Reconciled);
        let open_sui_none: Vec<_> = db
            .get_open_pond_cells()
            .unwrap()
            .into_iter()
            .filter(|c| c.cell_id == c1 || c.cell_id == c2)
            .collect();
        for cell in &open_sui_none {
            assert!(
                (cell.quantity.unwrap_or(0.0) - 20.0).abs() < 1e-6,
                "Each SUI Pond cell must resize to 20.0 SUI on None branch, got {:?}",
                cell.quantity
            );
            assert!(
                (cell.risk_budget_usdc.unwrap_or(0.0) - 2.0).abs() < 1e-3,
                "Each SUI Pond cell risk_budget_usdc must rescale to $2.0 on None branch (R10-S1), got {:?}",
                cell.risk_budget_usdc
            );
        }

        // 1b. R9-S1 3-row proportional weighting + cross-book phantom closure:
        // Insert 3 equal 1.0 BTC OPEN PIT trades (sum = 3.0 BTC vs 1.2 BTC on-chain -> 0.4 BTC each),
        // plus 1 phantom 5.0 SOL OPEN PIT trade while Pond holds 10.0 SOL on-chain (ex_qty = 10.0, pond_co_qty = 10.0).
        let mut btc_trade = base_eth_trade.clone();
        btc_trade.symbol = "BTC/USDC:USDC".to_string();
        btc_trade.base_asset = "BTC".to_string();
        btc_trade.entry_price = 90_000.0;
        btc_trade.current_price = 90_000.0;
        btc_trade.initial_stop_price = 88_200.0;
        btc_trade.chandelier_stop_price = 88_200.0;
        btc_trade.current_stop_price = 88_200.0;
        btc_trade.notional_usdc = 90_000.0;
        btc_trade.risk_budget_usdc = 1_800.0;
        btc_trade.client_order_id = "hl-btc-dup-1".to_string();
        let b1 = db.insert_trade(&btc_trade).unwrap();
        btc_trade.client_order_id = "hl-btc-dup-2".to_string();
        let b2 = db.insert_trade(&btc_trade).unwrap();
        btc_trade.client_order_id = "hl-btc-dup-3".to_string();
        let b3 = db.insert_trade(&btc_trade).unwrap();

        let mut sol_pit_phantom = base_eth_trade.clone();
        sol_pit_phantom.symbol = "SOL/USDC:USDC".to_string();
        sol_pit_phantom.base_asset = "SOL".to_string();
        sol_pit_phantom.side = "LONG".to_string();
        sol_pit_phantom.entry_price = 150.0;
        sol_pit_phantom.current_price = 150.0;
        sol_pit_phantom.initial_stop_price = 145.0;
        sol_pit_phantom.chandelier_stop_price = 145.0;
        sol_pit_phantom.current_stop_price = 145.0;
        sol_pit_phantom.quantity = 5.0;
        sol_pit_phantom.filled_units = 5.0;
        sol_pit_phantom.notional_usdc = 750.0;
        sol_pit_phantom.risk_budget_usdc = 25.0;
        sol_pit_phantom.client_order_id = "hl-sol-pit-phantom".to_string();
        let sol_pit_id = db.insert_trade(&sol_pit_phantom).unwrap();

        let mut sol_pond_cell = base_sui_cell.clone();
        sol_pond_cell.symbol = "SOL/USDC:USDC".to_string();
        sol_pond_cell.side = "LONG".to_string();
        sol_pond_cell.option_analogue = "SHORT_PUT".to_string();
        sol_pond_cell.entry_price = Some(150.0);
        sol_pond_cell.quantity = Some(10.0);
        sol_pond_cell.notional_usdc = Some(1_500.0);
        sol_pond_cell.risk_budget_usdc = Some(50.0);
        sol_pond_cell.initial_stop = Some(145.0);
        sol_pond_cell.chandelier_stop = Some(145.0);
        let sol_pond_id = db.upsert_pond_cell(&sol_pond_cell).unwrap();

        on_chain.insert(
            "BTC".to_string(),
            json!({"szi": 1.2, "entryPx": 90000.0, "markPx": 90000.0, "unrealizedPnl": 0.0}),
        );
        on_chain.insert(
            "SOL".to_string(),
            json!({"szi": 10.0, "entryPx": 150.0, "markPx": 150.0, "unrealizedPnl": 0.0}),
        );

        let rep3 = exec.reconcile_exchange_state(Some(&on_chain), Some(&[]));
        assert_eq!(rep3.status, ReconcileStatus::Reconciled);

        let open_btc: Vec<_> = db
            .get_open_trades()
            .unwrap()
            .into_iter()
            .filter(|t| t.trade_id == b1 || t.trade_id == b2 || t.trade_id == b3)
            .collect();
        assert_eq!(open_btc.len(), 3);
        for row in &open_btc {
            assert!(
                (row.quantity - 0.4).abs() < 1e-6,
                "Each of the 3 equal 1.0 BTC rows must resize equally to 0.4 BTC, got {} (R9-S1)",
                row.quantity
            );
        }
        assert!(
            db.get_open_trades()
                .unwrap()
                .iter()
                .all(|t| t.trade_id != sol_pit_id),
            "Phantom PIT trade on SOL must be closed as PHANTOM_LOCAL_CLOSED when Pond holds the entire 10.0 SOL exchange position (R9-S1)"
        );
        let sol_pond_after = db
            .get_open_pond_cells()
            .unwrap()
            .into_iter()
            .find(|c| c.cell_id == sol_pond_id)
            .expect("SOL Pond cell must remain OPEN");
        assert!(
            (sol_pond_after.quantity.unwrap_or(0.0) - 10.0).abs() < 1e-6,
            "SOL Pond cell quantity must remain intact at 10.0 SOL after phantom PIT row closure, got {:?} (R9-S1)",
            sol_pond_after.quantity
        );

        // 2. R9-N2: Verify adjust_testnet_faucet_spot_usdc uses `testnet_faucet_offset_usdc` from ConfigMap
        // including when `5_000.0 < initial_equity_usdc <= testnet_faucet_offset_usdc`
        let cfg_default = db.load_config().unwrap();
        let (adj_tot, adj_hold) = exec.adjust_testnet_faucet_spot_usdc(
            &cfg_default,
            true,
            1_000.0,
            450.0,
            10_517.65,
            0.0,
            0.0,
        );
        assert!((adj_tot - 517.65).abs() < 1e-2);
        assert!((adj_hold - 0.0).abs() < 1e-6);

        let (adj_tot_6k, _) = exec.adjust_testnet_faucet_spot_usdc(
            &cfg_default,
            true,
            6_000.0,
            3_000.0,
            13_000.0,
            0.0,
            0.0,
        );
        assert!(
            (adj_tot_6k - 3_000.0).abs() < 1e-2,
            "adjust_testnet_faucet_spot_usdc must subtract 10k faucet offset when init_eq=6000 <= configured_offset=10000 (R9-N2), got {adj_tot_6k}"
        );

        db.set_config_value("testnet_faucet_offset_usdc", &json!(5_000.0))
            .unwrap();
        let cfg_custom = db.load_config().unwrap();
        let (adj_tot_custom, _) = exec.adjust_testnet_faucet_spot_usdc(
            &cfg_custom,
            true,
            1_000.0,
            450.0,
            5_517.65,
            0.0,
            0.0,
        );
        assert!(
            (adj_tot_custom - 517.65).abs() < 1e-2,
            "adjust_testnet_faucet_spot_usdc must honor custom testnet_faucet_offset_usdc=5000.0 (R9-N2)"
        );

        // 3. R8-N2: Verify resolved_asset_cache TTL expiry refreshes stale cached UniverseAsset
        let stale_asset = build_orphan_or_pond_universe_asset(
            "BTC/USDC:USDC",
            "BTC",
            90_000.0,
            None,
            "ORPHAN",
        );
        exec.resolved_asset_cache
            .lock()
            .insert("BTC/USDC:USDC".to_string(), (stale_asset, 0));
        let refreshed = exec.resolve_trade_asset("BTC/USDC:USDC", "BTC", 90_000.0);
        assert_ne!(
            refreshed.tier_name, "ORPHAN",
            "Expired entry in resolved_asset_cache (cached_at_ms=0) must be refreshed from universe_assets (R8-N2)"
        );

        // 4. R9-N6: HTTP wire-mock verification of `cancel_resting_order` response parsing
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let pk = "0x1111111111111111111111111111111111111111111111111111111111111111";
        db.set_config_value("private_key", &json!(pk)).unwrap();
        exec.asset_index_cache
            .lock()
            .insert((true, "ETH".to_string()), (0_u32, db.now_ms()));

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let stop_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_clone = stop_flag.clone();
        let responses = [
            r#"{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}"#,
            r#"{"status":"ok","response":{"type":"cancel","data":{"statuses":[{"error":"Order was already canceled"}]}}}"#,
            r#"{"status":"ok","response":{"type":"cancel","data":{"statuses":[]}}}"#,
            r#"{"status":"ok","response":{"type":"cancel","data":{"statuses":[{"unexpected":1}]}}}"#,
        ];
        let handle = std::thread::spawn(move || {
            let mut idx = 0usize;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed)
                && std::time::Instant::now() < deadline
                && idx < responses.len()
            {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let _ =
                            stream.set_read_timeout(Some(std::time::Duration::from_millis(200)));
                        let mut buf = [0u8; 4096];
                        let mut total = Vec::new();
                        while let Ok(n) = stream.read(&mut buf) {
                            if n == 0 {
                                break;
                            }
                            total.extend_from_slice(&buf[..n]);
                            if let Some(hdr_end) =
                                total.windows(4).position(|w| w == b"\r\n\r\n")
                            {
                                if total.len() > hdr_end + 4 {
                                    break;
                                }
                            }
                        }
                        let body = responses[idx];
                        idx += 1;
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(resp.as_bytes());
                        let _ = stream.flush();
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let _ = stream.read(&mut buf);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        db.set_config_value("hyperliquid_testnet_url", &json!(format!("http://{addr}")))
            .unwrap();
        assert!(
            exec.cancel_resting_order("ETH/USDC:USDC", "hl-90001")
                .unwrap(),
            "statuses: [\"success\"] must return Ok(true) (R9-N6)"
        );
        assert!(
            exec.cancel_resting_order("ETH/USDC:USDC", "hl-90002")
                .unwrap(),
            "statuses: [{{\"error\": \"Order was already canceled\"}}] must return Ok(true) (R9-N6)"
        );
        assert!(
            !exec
                .cancel_resting_order("ETH/USDC:USDC", "hl-90003")
                .unwrap(),
            "Empty statuses: [] must return Ok(false) (R9-N6)"
        );
        assert!(
            !exec
                .cancel_resting_order("ETH/USDC:USDC", "hl-90004")
                .unwrap(),
            "Malformed statuses: [{{\"unexpected\": 1}}] must return Ok(false) (R9-N6)"
        );
        stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = handle.join();
    }

    #[test]
    fn test_r11_s1_oracle_clamped_ioc_retry_scenarios() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let wallet = aegis_core::derive_evm_address_from_private_key(pk).unwrap();
        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();
        db.set_config_value("hyperliquid_testnet", &json!(true))
            .unwrap();
        db.set_config_value("private_key", &json!(pk)).unwrap();
        db.set_config_value("wallet_address", &json!(wallet))
            .unwrap();

        let exec = ExecutionEngine::new(db.clone());
        exec.asset_index_cache
            .lock()
            .insert((true, "SOL".to_string()), (0_u32, db.now_ms()));

        let mut sol_asset = db
            .get_usdc_perp_universe()
            .unwrap()
            .into_iter()
            .find(|a| a.symbol == "SOL/USDC:USDC")
            .unwrap();
        sol_asset.tick_size = 0.01;
        sol_asset.lot_step_size = 0.01;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let stop_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_clone = stop_flag.clone();
        let captured_orders = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<serde_json::Value>::new()));
        let captured_clone = captured_orders.clone();

        // Sequence of HTTP responses across the 4 scenarios:
        // Scenario 1 (BUY no-match, live_mid*1.05 > oraclePx*1.0395):
        //   1. /exchange initial IOC -> error "Order could not immediately match against any resting orders"
        //   2. /info metaAndAssetCtxs -> oraclePx=100.0, midPx=100.0
        //   3. /exchange retry IOC -> filled at 103.95
        // Scenario 2 (SELL no-match, live_mid*0.95 < oraclePx*0.9605):
        //   4. /exchange initial IOC -> error "Order could not immediately match against any resting orders"
        //   5. /info metaAndAssetCtxs -> oraclePx=100.0, midPx=100.0
        //   6. /exchange retry IOC -> filled at 96.05
        // Scenario 3 (BUY oracle-band rejection, 0.3% slippage around live_mid clamped to oracle band):
        //   7. /exchange initial IOC -> error "Order price too far from oracle"
        //   8. /info metaAndAssetCtxs -> oraclePx=100.0, midPx=103.8 (103.8 * 1.003 = 104.1114 > 103.95 -> clamped to 103.95)
        //   9. /exchange retry IOC -> filled at 103.95
        //   10. /exchange initial IOC -> error "Order price too far from oracle"
        //   11. /info metaAndAssetCtxs -> oraclePx=100.0, midPx=100.0 (100.0 * 1.003 = 100.3 <= 103.95 -> unclamped 100.3)
        //   12. /exchange retry IOC -> filled at 100.3
        // Scenario 4 (metaAndAssetCtxs fails / returns no oraclePx -> fallback to allMids and unclamped live_mid * (1.0 + slip_mult)):
        //   13. /exchange initial IOC -> error "Order could not immediately match against any resting orders"
        //   14. /info metaAndAssetCtxs -> {} (empty/missing)
        //   15. /info allMids -> {"SOL": "200.0"}
        //   16. /exchange retry IOC -> filled at 210.0
        let responses = vec![
            // Scenario 1
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order could not immediately match against any resting orders. asset=0"}]}}}"#,
            r#"[{"universe":[{"name":"SOL","szDecimals":2}]},[{"oraclePx":"100.0","midPx":"100.0"}]]"#,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"1.0","avgPx":"103.95","oid":7001}}]}}}"#,
            // Scenario 2
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order could not immediately match against any resting orders. asset=0"}]}}}"#,
            r#"[{"universe":[{"name":"SOL","szDecimals":2}]},[{"oraclePx":"100.0","midPx":"100.0"}]]"#,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"1.0","avgPx":"96.05","oid":7002}}]}}}"#,
            // Scenario 3a (BUY clamped 0.3% oracle retry)
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order price too far from oracle"}]}}}"#,
            r#"[{"universe":[{"name":"SOL","szDecimals":2}]},[{"oraclePx":"100.0","midPx":"103.8"}]]"#,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"1.0","avgPx":"103.95","oid":7003}}]}}}"#,
            // Scenario 3b (SELL clamped 0.3% oracle retry: midPx=96.2 * 0.997 = 95.9114 < 96.05 -> clamped to 96.05)
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order price too far from oracle"}]}}}"#,
            r#"[{"universe":[{"name":"SOL","szDecimals":2}]},[{"oraclePx":"100.0","midPx":"96.2"}]]"#,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"1.0","avgPx":"96.05","oid":7004}}]}}}"#,
            // Scenario 3c (BUY unclamped 0.3% oracle retry within band: 100.0 * 1.003 = 100.3)
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order price too far from oracle"}]}}}"#,
            r#"[{"universe":[{"name":"SOL","szDecimals":2}]},[{"oraclePx":"100.0","midPx":"100.0"}]]"#,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"1.0","avgPx":"100.3","oid":7005}}]}}}"#,
            // Scenario 3d (SELL unclamped 0.3% oracle retry within band: 100.0 * 0.997 = 99.7)
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order price too far from oracle"}]}}}"#,
            r#"[{"universe":[{"name":"SOL","szDecimals":2}]},[{"oraclePx":"100.0","midPx":"100.0"}]]"#,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"1.0","avgPx":"99.7","oid":7006}}]}}}"#,
            // Scenario 3e (metaAndAssetCtxs missing SOL -> allMids fallback with 0.3% oracle slip: 200.0 * 1.003 = 200.6)
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order price too far from oracle"}]}}}"#,
            r#"[{"universe":[{"name":"BTC","szDecimals":4}]},[{"oraclePx":"90000.0","midPx":"90000.0"}]]"#,
            r#"{"SOL":"200.0"}"#,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"1.0","avgPx":"200.6","oid":7007}}]}}}"#,
            // Scenario 4 (metaAndAssetCtxs fails on no-match -> fallback to allMids with 5% slip: 200.0 * 1.05 = 210.0)
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order could not immediately match against any resting orders. asset=0"}]}}}"#,
            r#"{}"#,
            r#"{"SOL":"200.0"}"#,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"1.0","avgPx":"210.0","oid":7008}}]}}}"#,
        ];

        let handle = std::thread::spawn(move || {
            let mut idx = 0usize;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed)
                && std::time::Instant::now() < deadline
                && idx < responses.len()
            {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let _ =
                            stream.set_read_timeout(Some(std::time::Duration::from_millis(300)));
                        let mut buf = [0u8; 8192];
                        let mut total = Vec::new();
                        while let Ok(n) = stream.read(&mut buf) {
                            if n == 0 {
                                break;
                            }
                            total.extend_from_slice(&buf[..n]);
                            if let Some(hdr_end) =
                                total.windows(4).position(|w| w == b"\r\n\r\n")
                            {
                                let body_start = hdr_end + 4;
                                let hdr_str = String::from_utf8_lossy(&total[..hdr_end]);
                                let content_len = hdr_str
                                    .lines()
                                    .find_map(|l| {
                                        let lower = l.to_ascii_lowercase();
                                        lower
                                            .strip_prefix("content-length:")
                                            .and_then(|v| v.trim().parse::<usize>().ok())
                                    })
                                    .unwrap_or(0);
                                if total.len() >= body_start + content_len {
                                    let req_body = &total[body_start..body_start + content_len];
                                    if let Ok(val) =
                                        serde_json::from_slice::<serde_json::Value>(req_body)
                                    {
                                        if val.get("action").is_some() {
                                            captured_clone.lock().push(val);
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        let body = responses[idx];
                        idx += 1;
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(resp.as_bytes());
                        let _ = stream.flush();
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let _ = stream.read(&mut buf);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        let endpoint = format!("http://{addr}");
        db.set_config_value("hyperliquid_testnet_url", &json!(endpoint))
            .unwrap();
        db.set_config_value("hyperliquid_testnet_info_url", &json!(endpoint))
            .unwrap();
        let cfg = db.load_config().unwrap();

        // Scenario 1: BUY no-match -> clamped to oraclePx * (1.0 + HL_ORACLE_BAND_PCT) = 103.95
        let res1 = exec.submit_hyperliquid_order_live(
            &sol_asset,
            "LONG",
            1.0,
            100.0,
            100.0,
            false,
            false,
            "coid-r11-s1-1",
            &cfg,
            true,
        );
        assert_eq!(res1.fsm_state, "FILLED");
        assert!((res1.avg_fill_price - 103.95).abs() < 1e-6);

        // Scenario 2: SELL no-match -> clamped to oraclePx * (1.0 - HL_ORACLE_BAND_PCT) = 96.05
        exec.asset_index_cache
            .lock()
            .insert((true, "SOL".to_string()), (0_u32, db.now_ms()));
        let res2 = exec.submit_hyperliquid_order_live(
            &sol_asset,
            "SHORT",
            1.0,
            100.0,
            100.0,
            false,
            false,
            "coid-r11-s1-2",
            &cfg,
            true,
        );
        assert_eq!(res2.fsm_state, "FILLED");
        assert!((res2.avg_fill_price - 96.05).abs() < 1e-6);

        // Scenario 3a: BUY "Price too far from oracle" with midPx=103.8 -> 0.3% slip clamped to 103.95
        exec.asset_index_cache
            .lock()
            .insert((true, "SOL".to_string()), (0_u32, db.now_ms()));
        let res3a = exec.submit_hyperliquid_order_live(
            &sol_asset,
            "LONG",
            1.0,
            106.0,
            100.0,
            false,
            false,
            "coid-r11-s1-3a",
            &cfg,
            true,
        );
        assert_eq!(res3a.fsm_state, "FILLED");
        assert!((res3a.avg_fill_price - 103.95).abs() < 1e-6);

        // Scenario 3b: SELL "Price too far from oracle" with midPx=96.2 -> 0.3% slip clamped to 96.05
        exec.asset_index_cache
            .lock()
            .insert((true, "SOL".to_string()), (0_u32, db.now_ms()));
        let res3b = exec.submit_hyperliquid_order_live(
            &sol_asset,
            "SHORT",
            1.0,
            94.0,
            100.0,
            false,
            false,
            "coid-r11-s1-3b",
            &cfg,
            true,
        );
        assert_eq!(res3b.fsm_state, "FILLED");
        assert!((res3b.avg_fill_price - 96.05).abs() < 1e-6);

        // Scenario 3c: BUY "Price too far from oracle" with midPx=100.0 -> 0.3% slip = 100.3
        exec.asset_index_cache
            .lock()
            .insert((true, "SOL".to_string()), (0_u32, db.now_ms()));
        let res3c = exec.submit_hyperliquid_order_live(
            &sol_asset,
            "LONG",
            1.0,
            106.0,
            100.0,
            false,
            false,
            "coid-r11-s1-3c",
            &cfg,
            true,
        );
        assert_eq!(res3c.fsm_state, "FILLED");
        assert!((res3c.avg_fill_price - 100.3).abs() < 1e-6);

        // Scenario 3d: SELL "Price too far from oracle" with midPx=100.0 -> 0.3% slip = 99.7
        exec.asset_index_cache
            .lock()
            .insert((true, "SOL".to_string()), (0_u32, db.now_ms()));
        let res3d = exec.submit_hyperliquid_order_live(
            &sol_asset,
            "SHORT",
            1.0,
            94.0,
            100.0,
            false,
            false,
            "coid-r11-s1-3d",
            &cfg,
            true,
        );
        assert_eq!(res3d.fsm_state, "FILLED");
        assert!((res3d.avg_fill_price - 99.7).abs() < 1e-6);

        // Scenario 3e: BUY "Price too far from oracle" where metaAndAssetCtxs omits SOL -> allMids fallback 200.0 * 1.003 = 200.6
        exec.asset_index_cache
            .lock()
            .insert((true, "SOL".to_string()), (0_u32, db.now_ms()));
        let res3e = exec.submit_hyperliquid_order_live(
            &sol_asset,
            "LONG",
            1.0,
            210.0,
            200.0,
            false,
            false,
            "coid-r11-s1-3e",
            &cfg,
            true,
        );
        assert_eq!(res3e.fsm_state, "FILLED");
        assert!((res3e.avg_fill_price - 200.6).abs() < 1e-6);

        // Scenario 4: metaAndAssetCtxs fails -> fallback to allMids (200.0) and unclamped 200.0 * 1.05 = 210.0
        exec.asset_index_cache
            .lock()
            .insert((true, "SOL".to_string()), (0_u32, db.now_ms()));
        let res4 = exec.submit_hyperliquid_order_live(
            &sol_asset,
            "LONG",
            1.0,
            195.0,
            195.0,
            false,
            false,
            "coid-r11-s1-4",
            &cfg,
            true,
        );
        assert_eq!(res4.fsm_state, "FILLED");
        assert!((res4.avg_fill_price - 210.0).abs() < 1e-6);

        stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = handle.join();

        let orders = captured_orders.lock();
        assert_eq!(
            orders.len(),
            16,
            "Expected 8 initial + 8 retry order wire payloads"
        );
        let retry_px = |idx: usize| -> String {
            orders[idx]["action"]["orders"][0]["p"]
                .as_str()
                .unwrap_or("")
                .to_string()
        };
        assert_eq!(
            retry_px(1),
            format_hl_wire_price(100.0 * (1.0 + HL_ORACLE_BAND_PCT), 0.01, 2),
            "Scenario 1 retry OrderWire.p must be clamped to oraclePx * 1.0395 = 103.95 (R11-S1)"
        );
        assert_eq!(retry_px(1), "103.95");
        assert_eq!(
            retry_px(3),
            format_hl_wire_price(100.0 * (1.0 - HL_ORACLE_BAND_PCT), 0.01, 2),
            "Scenario 2 retry OrderWire.p must be clamped to oraclePx * 0.9605 = 96.05 (R11-S1)"
        );
        assert_eq!(retry_px(3), "96.05");
        assert_eq!(
            retry_px(5),
            "103.95",
            "Scenario 3a BUY retry OrderWire.p must clamp 0.3% slippage around live_mid=103.8 to oracle band 103.95 (R11-S1)"
        );
        assert_eq!(
            retry_px(7),
            "96.05",
            "Scenario 3b SELL retry OrderWire.p must clamp 0.3% slippage around live_mid=96.2 to oracle band 96.05 (R11-S1)"
        );
        assert_eq!(
            retry_px(9),
            format_hl_wire_price(100.0 * (1.0 + HL_ORACLE_RETRY_SLIP_PCT), 0.01, 2),
            "Scenario 3c BUY retry OrderWire.p must use 0.3% slippage around live_mid=100.0 -> 100.3 (R11-S1)"
        );
        assert_eq!(retry_px(9), "100.3");
        assert_eq!(
            retry_px(11),
            format_hl_wire_price(100.0 * (1.0 - HL_ORACLE_RETRY_SLIP_PCT), 0.01, 2),
            "Scenario 3d SELL retry OrderWire.p must use 0.3% slippage around live_mid=100.0 -> 99.7 (R11-S1)"
        );
        assert_eq!(retry_px(11), "99.7");
        assert_eq!(
            retry_px(13),
            format_hl_wire_price(200.0 * (1.0 + HL_ORACLE_RETRY_SLIP_PCT), 0.01, 2),
            "Scenario 3e oracle retry OrderWire.p must fall back to allMids=200.0 * 1.003 = 200.6 when metaAndAssetCtxs omits coin (R11-S1)"
        );
        assert_eq!(retry_px(13), "200.6");
        assert_eq!(
            retry_px(15),
            format_hl_wire_price(200.0 * (1.0 + HL_NO_MATCH_RETRY_SLIP_PCT), 0.01, 2),
            "Scenario 4 retry OrderWire.p must fall back to allMids=200.0 * 1.05 = 210 (R11-S1)"
        );
        assert_eq!(retry_px(15), "210");
    }
