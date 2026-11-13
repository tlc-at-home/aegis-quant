//! Dynamic 80-Coin Universe Lifecycle Manager (`aegis-pond::universe`).
//!
//! Manages the top-80 Hyperliquid USDC perpetual universe (`pond_universe`):
//! - 24-hour refresh ranking by 7-day median daily volume (`>= $50,000`).
//! - 168-hour `WARMUP` window for newly discovered listings before entering `ACTIVE`.
//! - Delisting hysteresis: rank > 95 for 2 consecutive refreshes transitions to `DELISTING`,
//!   and once no open cells remain, transitions to `RETIRED`.

use aegis_core::{now_iso8601_utc, PondUniverseRow, Result};
use aegis_db::BookScopedDb;
use aegis_execution::MarketIngestionEngine;
use chrono::{Duration, NaiveDateTime, Utc};
use std::collections::{HashMap, HashSet};

pub(crate) fn parse_utc_timestamp(s: &str) -> Option<NaiveDateTime> {
    aegis_core::parse_utc_timestamp(s)
}

/// Dynamic 80-coin Pond universe manager.
#[derive(Clone)]
pub struct PondUniverseManager {
    pub scoped_db: BookScopedDb,
    pub ingestion: MarketIngestionEngine,
}

impl PondUniverseManager {
    pub fn new(scoped_db: BookScopedDb, ingestion: MarketIngestionEngine) -> Self {
        Self {
            scoped_db,
            ingestion,
        }
    }

    /// Bootstraps or refreshes the 80-coin Pond universe with hysteresis and 168h warmup tracking.
    pub fn refresh_universe(
        &self,
        candidates_override: Option<Vec<PondUniverseRow>>,
    ) -> Result<Vec<PondUniverseRow>> {
        self.scoped_db.check_table_access("pond_universe")?;
        self.scoped_db.check_table_access("pond_cells")?;

        let cfg = self.scoped_db.load_pond_config()?;
        let target_size = cfg.pond_universe_target_size.max(1);
        let min_vol = if cfg.pond_execution_mode.eq_ignore_ascii_case("TESTNET") {
            cfg.pond_min_daily_volume_usdc
                .min(cfg.pond_testnet_min_vol_cap_usdc)
        } else {
            cfg.pond_min_daily_volume_usdc
        };
        let hysteresis_rank = cfg.pond_universe_delist_rank as i64;
        let warmup_hours = cfg.pond_universe_warmup_hours.max(1);

        let existing_rows = self.scoped_db.get_pond_universe(false)?;
        let existing_map: HashMap<String, PondUniverseRow> = existing_rows
            .into_iter()
            .map(|r| (r.symbol.clone(), r))
            .collect();

        let open_cells = self.scoped_db.get_open_pond_cells()?;
        let open_cell_symbols: HashSet<String> = open_cells.into_iter().map(|c| c.symbol).collect();

        let has_override = candidates_override.is_some();
        let discovered = match candidates_override {
            Some(list) => list,
            None => self
                .ingestion
                .discover_pond_universe_candidates(target_size, min_vol),
        };

        let now = Utc::now();
        let now_str = now_iso8601_utc();
        let mut updated_symbols = HashSet::new();
        let mut final_rows = Vec::new();

        let is_live_discovery = discovered.iter().any(|c| c.is_synthetic_volume == 0);
        let has_existing_live_active = existing_map
            .values()
            .any(|r| r.status != "RETIRED" && r.is_synthetic_volume == 0);
        if !has_override && !is_live_discovery && has_existing_live_active {
            tracing::warn!(
                pond_execution_mode = %cfg.pond_execution_mode,
                "Pond universe discovery returned synthetic fallback while live universe rows exist; preserving existing live universe (R15-QA(d))"
            );
            let mut preserved: Vec<PondUniverseRow> = existing_map.into_values().collect();
            preserved.sort_by_key(|r| r.rank);
            return Ok(preserved);
        }
        let is_initial_bootstrap = existing_map.is_empty()
            || (is_live_discovery
                && existing_map
                    .values()
                    .filter(|r| r.status != "RETIRED")
                    .all(|r| r.is_synthetic_volume != 0));

        for cand in discovered {
            let prev = existing_map.get(&cand.symbol);
            let is_existing_live =
                is_live_discovery && prev.map(|p| p.is_synthetic_volume == 0).unwrap_or(false);
            if cand.median_daily_volume_usdc < min_vol && !is_existing_live {
                continue;
            }
            updated_symbols.insert(cand.symbol.clone());

            let (added_at, warmup_until_str) = match prev {
                Some(p)
                    if !(is_live_discovery && p.is_synthetic_volume != 0)
                        && !(is_initial_bootstrap && p.status == "RETIRED") =>
                {
                    (
                        p.added_at.clone(),
                        p.warmup_until.clone().unwrap_or_else(|| now_str.clone()),
                    )
                }
                Some(p) => (
                    p.added_at.clone(),
                    cand.warmup_until.clone().unwrap_or_else(|| {
                        (now - Duration::hours(warmup_hours + 12))
                            .format("%Y-%m-%dT%H:%M:%SZ")
                            .to_string()
                    }),
                ),
                None => {
                    if is_initial_bootstrap {
                        (
                            now_str.clone(),
                            (now - Duration::hours(warmup_hours + 12))
                                .format("%Y-%m-%dT%H:%M:%SZ")
                                .to_string(),
                        )
                    } else {
                        (
                            now_str.clone(),
                            (now + Duration::hours(warmup_hours))
                                .format("%Y-%m-%dT%H:%M:%SZ")
                                .to_string(),
                        )
                    }
                }
            };

            let is_warmup_complete = parse_utc_timestamp(&warmup_until_str)
                .map(|dt| dt <= now.naive_utc())
                .unwrap_or(true);

            let below_rank = cand.rank > hysteresis_rank || cand.median_daily_volume_usdc < min_vol;
            let delist_streak = if below_rank {
                prev.map(|p| p.delist_streak + 1).unwrap_or(1)
            } else {
                0
            };

            let (status, delisted_at) = if delist_streak >= 2 {
                if open_cell_symbols.contains(&cand.symbol) {
                    ("DELISTING".to_string(), None)
                } else {
                    ("RETIRED".to_string(), Some(now_str.clone()))
                }
            } else if !is_warmup_complete {
                ("WARMUP".to_string(), None)
            } else {
                ("ACTIVE".to_string(), None)
            };

            final_rows.push(PondUniverseRow {
                symbol: cand.symbol,
                rank: cand.rank,
                median_daily_volume_usdc: cand.median_daily_volume_usdc,
                tick_size: cand.tick_size,
                lot_step_size: cand.lot_step_size,
                min_notional_usdc: cand.min_notional_usdc,
                max_leverage: cand.max_leverage,
                reference_price: cand.reference_price,
                status,
                warmup_until: Some(warmup_until_str),
                delist_streak,
                added_at,
                delisted_at,
                is_synthetic_volume: cand.is_synthetic_volume,
            });
        }

        // Handle previously tracked symbols absent from the current top list
        for (sym, prev) in existing_map {
            if updated_symbols.contains(&sym) || prev.status == "RETIRED" {
                continue;
            }
            let delist_streak = if is_live_discovery && prev.is_synthetic_volume != 0 {
                2
            } else {
                prev.delist_streak + 1
            };
            let (status, delisted_at) = if delist_streak >= 2 {
                if open_cell_symbols.contains(&sym) {
                    ("DELISTING".to_string(), None)
                } else {
                    ("RETIRED".to_string(), Some(now_str.clone()))
                }
            } else {
                (prev.status.clone(), prev.delisted_at.clone())
            };
            final_rows.push(PondUniverseRow {
                symbol: sym,
                rank: prev.rank.max(hysteresis_rank + 5),
                median_daily_volume_usdc: prev.median_daily_volume_usdc,
                tick_size: prev.tick_size,
                lot_step_size: prev.lot_step_size,
                min_notional_usdc: prev.min_notional_usdc,
                max_leverage: prev.max_leverage,
                reference_price: prev.reference_price,
                status,
                warmup_until: prev.warmup_until,
                delist_streak,
                added_at: prev.added_at,
                delisted_at,
                is_synthetic_volume: prev.is_synthetic_volume,
            });
        }

        self.scoped_db.upsert_pond_universe_batch(&final_rows)?;
        final_rows.sort_by_key(|r| r.rank);
        Ok(final_rows)
    }

    /// Marks any `DELISTING` asset that no longer has open cells as `RETIRED`.
    pub fn finalize_delisted_retirements(&self) -> Result<usize> {
        self.scoped_db.check_table_access("pond_universe")?;
        self.scoped_db.check_table_access("pond_cells")?;
        let all = self.scoped_db.get_pond_universe(false)?;
        let delisting: Vec<PondUniverseRow> = all
            .into_iter()
            .filter(|r| r.status == "DELISTING")
            .collect();
        if delisting.is_empty() {
            return Ok(0);
        }
        let open_cells = self.scoped_db.get_open_pond_cells()?;
        let open_syms: HashSet<String> = open_cells.into_iter().map(|c| c.symbol).collect();
        let now_str = now_iso8601_utc();
        let mut to_update = Vec::new();
        for mut row in delisting {
            if !open_syms.contains(&row.symbol) {
                row.status = "RETIRED".to_string();
                row.delisted_at = Some(now_str.clone());
                to_update.push(row);
            }
        }
        let count = to_update.len();
        if count > 0 {
            self.scoped_db.upsert_pond_universe_batch(&to_update)?;
        }
        Ok(count)
    }
}
