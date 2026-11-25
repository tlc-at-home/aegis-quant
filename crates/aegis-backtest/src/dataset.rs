//! Synthetic Canonical Dataset & Causal DB Historical Dataset Assembler (`aegis-backtest::dataset`).

use aegis_core::indicators::timeframe_to_ms;
use aegis_core::{round_dp, AegisError, ExchangeAsset, OhlcvBar, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};

/// Normalized historical OHLCV + funding dataset ready for `BacktestEngine::run_walk_forward`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalDataset {
    pub source: String,
    pub timeframe: String,
    #[serde(default)]
    pub eval_start_ms: Option<i64>,
    pub assets: Vec<ExchangeAsset>,
    pub bars_by_asset: HashMap<i64, Vec<OhlcvBar>>,
}

/// Counts distinct bar open timestamps strictly preceding `cutoff_ts_ms` across all assets.
pub fn count_timeline_bars_before(
    bars_by_asset: &HashMap<i64, Vec<OhlcvBar>>,
    cutoff_ts_ms: i64,
) -> usize {
    let mut ts_set = BTreeSet::new();
    for bars in bars_by_asset.values() {
        for bar in bars {
            if bar.timestamp_ms < cutoff_ts_ms {
                ts_set.insert(bar.timestamp_ms);
            }
        }
    }
    ts_set.len()
}

/// Generates a deterministic multi-asset Hyperliquid perpetual dataset (`BTC-USDC`, `ETH-USDC`,
/// `SOL-USDC`, `HYPE-USDC`, `SUI-USDC`, `AVAX-USDC`, `LINK-USDC`, `TAO-USDC`) for walk-forward verification.
pub fn generate_canonical_dataset(
    days: usize,
    warmup_bars: usize,
) -> (Vec<ExchangeAsset>, HashMap<i64, Vec<OhlcvBar>>) {
    let total_bars = (days.max(5) * 6) + warmup_bars.max(20);
    let specs = [
        (1i64, "BTC", 92_000.0, 0.0022, 0.000010, 65_000_000.0),
        (2i64, "ETH", 3_150.0, 0.0018, 0.000012, 42_000_000.0),
        (3i64, "SOL", 185.0, 0.0035, -0.000025, 34_000_000.0),
        (4i64, "HYPE", 26.5, 0.0042, -0.000040, 22_000_000.0),
        (5i64, "SUI", 3.20, 0.0030, 0.000015, 16_000_000.0),
        (6i64, "AVAX", 34.0, -0.0015, 0.000045, 14_000_000.0),
        (7i64, "LINK", 18.5, 0.0020, 0.000008, 12_500_000.0),
        (8i64, "TAO", 510.0, 0.0038, -0.000030, 11_000_000.0),
    ];

    let start_ts = 1_700_000_000_000i64;
    let step_ms = 4 * 3_600_000i64;
    let mut assets = Vec::with_capacity(specs.len());
    let mut bars_by_asset = HashMap::with_capacity(specs.len());

    for (id, coin, base_px, drift, base_funding, day_vol) in specs {
        let mut hasher = Sha256::new();
        hasher.update(format!("AEGIS_V2_BT:{coin}:{id}").as_bytes());
        let digest = hasher.finalize();
        let mut rng = u64::from_le_bytes(digest[0..8].try_into().unwrap_or([1; 8]));

        let mut px = base_px;
        let mut bars = Vec::with_capacity(total_bars);
        for t in 0..total_bars {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u = ((rng >> 11) as f64) / ((1u64 << 53) as f64) - 0.5;
            let cycle_wave = ((t as f64) * 0.11 + (id as f64)).sin() * 0.006;
            let ret = drift + cycle_wave + u * 0.014;
            let open = px;
            px = (px * (1.0 + ret)).max(base_px * 0.25);
            let close = px;
            let high = open.max(close) * (1.0 + 0.006 + u.abs() * 0.005);
            let low = open.min(close) * (1.0 - 0.006 - u.abs() * 0.005);
            let vol_surge = if t % 17 == (id as usize % 17) {
                2.3
            } else {
                1.0 + u.abs() * 0.4
            };
            let ts = start_ts + (t as i64) * step_ms;

            bars.push(OhlcvBar {
                id: (t + 1) as i64,
                asset_id: id,
                timeframe: "4h".to_string(),
                timestamp_ms: ts,
                open: round_dp(open, 4),
                high: round_dp(high, 4),
                low: round_dp(low, 4),
                close: round_dp(close, 4),
                volume: round_dp((day_vol / 6.0) * vol_surge, 2),
                funding_rate: base_funding,
                created_at_ms: ts,
                updated_at_ms: ts,
            });
        }

        let last_close = bars.last().map(|b| b.close).unwrap_or(base_px);
        let prev_close = bars
            .get(bars.len().saturating_sub(7))
            .map(|b| b.close)
            .unwrap_or(base_px);

        assets.push(ExchangeAsset {
            id,
            symbol: format!("{coin}-USDC"),
            coin: coin.to_string(),
            asset_index: id as u32,
            sz_decimals: 2,
            tick_size: if base_px < 10.0 { 0.001 } else { 0.01 },
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price: last_close,
            mid_price: last_close,
            oracle_price: last_close,
            funding_rate_hourly: base_funding,
            day_volume_usdc: day_vol,
            open_interest_usdc: day_vol * 0.45,
            prev_day_price: prev_close,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
            created_at_ms: start_ts,
            updated_at_ms: start_ts,
        });
        bars_by_asset.insert(id, bars);
    }

    (assets, bars_by_asset)
}

/// Assembles a strictly causal [`HistoricalDataset`] from MariaDB `exchange_assets`, `ohlcv_bars`,
/// and `funding_events` (`R14-M15`).
///
/// - Merges reconciled `funding_events` `(event_time_ms, funding_rate)` strictly causally into
///   `[bar_open, bar_close)` whenever `bar.funding_rate == 0.0` (never reading future bars/events).
/// - Fails loudly with [`AegisError::Validation`] when `assets` is empty or no asset satisfies `min_bars`
///   (never silently falling back to synthetic candles).
pub fn assemble_db_dataset(
    assets: Vec<ExchangeAsset>,
    mut bars_by_asset: HashMap<i64, Vec<OhlcvBar>>,
    mut funding_events: HashMap<i64, Vec<(i64, f64)>>,
    min_bars: usize,
) -> Result<HistoricalDataset> {
    if assets.is_empty() {
        return Err(AegisError::Validation(
            "Cannot assemble DB backtest dataset: zero exchange_assets provided".to_string(),
        ));
    }
    let min_req = min_bars.max(1);

    let mut timeframe = String::new();
    let mut max_bars_found = 0usize;
    let mut qualified_assets = 0usize;

    for asset in &assets {
        let Some(bars) = bars_by_asset.get_mut(&asset.id) else {
            continue;
        };
        bars.sort_by_key(|b| b.timestamp_ms);
        bars.dedup_by_key(|b| b.timestamp_ms);

        if timeframe.is_empty() {
            if let Some(first_tf) = bars
                .first()
                .map(|b| b.timeframe.trim())
                .filter(|s| !s.is_empty())
            {
                timeframe = first_tf.to_string();
            }
        }
        max_bars_found = max_bars_found.max(bars.len());
        if bars.len() >= min_req {
            qualified_assets += 1;
        }

        if let Some(events) = funding_events.get_mut(&asset.id) {
            events.sort_by_key(|&(ts, _)| ts);
            for idx in 0..bars.len() {
                if bars[idx].funding_rate.abs() > 1e-12 {
                    continue;
                }
                let bar_open = bars[idx].timestamp_ms;
                let bar_dur_ms = timeframe_to_ms(&bars[idx].timeframe)
                    .or_else(|| {
                        bars.get(idx + 1)
                            .map(|next| (next.timestamp_ms - bar_open).max(60_000))
                    })
                    .unwrap_or(14_400_000);
                let bar_close = bar_open.saturating_add(bar_dur_ms);

                let lo = events.partition_point(|&(ts, _)| ts < bar_open);
                let hi = events.partition_point(|&(ts, _)| ts < bar_close);
                if hi > lo {
                    let mut sum = 0.0;
                    let mut cnt = 0usize;
                    for &(_, rate) in &events[lo..hi] {
                        if rate.is_finite() {
                            sum += rate;
                            cnt += 1;
                        }
                    }
                    if cnt > 0 {
                        bars[idx].funding_rate = sum / (cnt as f64);
                    }
                }
            }
        }
    }

    if qualified_assets == 0 {
        return Err(AegisError::Validation(format!(
            "Insufficient historical DB bars: required at least {min_req} bars on >= 1 asset, max found {max_bars_found}"
        )));
    }

    if timeframe.is_empty() {
        timeframe = "4h".to_string();
    }

    let valid_assets: Vec<ExchangeAsset> = assets
        .into_iter()
        .filter(|a| bars_by_asset.get(&a.id).is_some_and(|b| !b.is_empty()))
        .collect();

    Ok(HistoricalDataset {
        source: "db".to_string(),
        timeframe,
        eval_start_ms: None,
        assets: valid_assets,
        bars_by_asset,
    })
}
