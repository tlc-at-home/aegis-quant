//! Multi-sleeve signals & target weights repository (`aegis-db::signals_repo`).

use crate::engine::DatabaseManager;
use crate::row_decode::{
    decimal_param, finite_double_param, row_get_f64, row_get_i32, row_get_i64, row_get_opt_string,
    row_get_string, DEC_20_10_MAX_ABS, DEC_20_8_MAX_ABS, DEC_24_8_MAX_ABS,
};
use aegis_core::{now_ms, AegisError, Result, SleeveSignal};
use mysql::params;
use mysql::prelude::*;

#[derive(Debug, Clone)]
pub struct SignalUpsertInput {
    pub asset_id: i64,
    pub mark_price: f64,
    pub day_volume_usdc: f64,
    pub funding_rate_hourly: f64,
    pub annualized_vol: f64,
    pub market_beta: f64,
    pub s1_mom_score: f64,
    pub s1_carry_score: f64,
    pub s2_trend_alignment: i32,
    pub s2_vol_surge_ratio: f64,
    pub s3_funding_zscore: f64,
    pub s3_flush_atr_mult: f64,
    pub w_s1_carry_mom: f64,
    pub w_s2_trend_breakout: f64,
    pub w_s3_event_reversion: f64,
    pub raw_combined_weight: f64,
    pub target_weight: f64,
    pub current_weight: f64,
    pub delta_weight: f64,
    pub event_trigger: Option<String>,
    pub rebalance_action: String,
}

struct PreparedSignalRow<'a> {
    input: &'a SignalUpsertInput,
    mark_price: String,
    day_volume_usdc: String,
    funding_rate_hourly: String,
    annualized_vol: f64,
    market_beta: f64,
    s1_mom_score: f64,
    s1_carry_score: f64,
    s2_vol_surge_ratio: f64,
    s3_funding_zscore: f64,
    s3_flush_atr_mult: f64,
    w_s1_carry_mom: String,
    w_s2_trend_breakout: String,
    w_s3_event_reversion: String,
    raw_combined_weight: String,
    target_weight: String,
    current_weight: String,
    delta_weight: String,
}

fn prepare_signal_row(s: &SignalUpsertInput) -> Result<PreparedSignalRow<'_>> {
    Ok(PreparedSignalRow {
        input: s,
        mark_price: decimal_param(s.mark_price, 8, DEC_20_8_MAX_ABS)?,
        day_volume_usdc: decimal_param(s.day_volume_usdc, 8, DEC_24_8_MAX_ABS)?,
        funding_rate_hourly: decimal_param(s.funding_rate_hourly, 10, DEC_20_10_MAX_ABS)?,
        annualized_vol: finite_double_param(s.annualized_vol, "annualized_vol")?,
        market_beta: finite_double_param(s.market_beta, "market_beta")?,
        s1_mom_score: finite_double_param(s.s1_mom_score, "s1_mom_score")?,
        s1_carry_score: finite_double_param(s.s1_carry_score, "s1_carry_score")?,
        s2_vol_surge_ratio: finite_double_param(s.s2_vol_surge_ratio, "s2_vol_surge_ratio")?,
        s3_funding_zscore: finite_double_param(s.s3_funding_zscore, "s3_funding_zscore")?,
        s3_flush_atr_mult: finite_double_param(s.s3_flush_atr_mult, "s3_flush_atr_mult")?,
        w_s1_carry_mom: decimal_param(s.w_s1_carry_mom, 8, DEC_20_8_MAX_ABS)?,
        w_s2_trend_breakout: decimal_param(s.w_s2_trend_breakout, 8, DEC_20_8_MAX_ABS)?,
        w_s3_event_reversion: decimal_param(s.w_s3_event_reversion, 8, DEC_20_8_MAX_ABS)?,
        raw_combined_weight: decimal_param(s.raw_combined_weight, 8, DEC_20_8_MAX_ABS)?,
        target_weight: decimal_param(s.target_weight, 8, DEC_20_8_MAX_ABS)?,
        current_weight: decimal_param(s.current_weight, 8, DEC_20_8_MAX_ABS)?,
        delta_weight: decimal_param(s.delta_weight, 8, DEC_20_8_MAX_ABS)?,
    })
}

const SIGNAL_UPSERT_SQL: &str = r#"INSERT INTO sleeve_signals (
       asset_id, mark_price, day_volume_usdc, funding_rate_hourly, annualized_vol,
       market_beta, s1_mom_score, s1_carry_score, s2_trend_alignment, s2_vol_surge_ratio,
       s3_funding_zscore, s3_flush_atr_mult, w_s1_carry_mom, w_s2_trend_breakout,
       w_s3_event_reversion, raw_combined_weight, target_weight, current_weight,
       delta_weight, event_trigger, rebalance_action, created_at_ms, updated_at_ms
   ) VALUES (
       :aid, :mark, :vol, :fr, :avol,
       :beta, :s1_mom, :s1_carry, :s2_align, :s2_surge,
       :s3_fz, :s3_flush, :w_s1, :w_s2,
       :w_s3, :raw_w, :tgt_w, :cur_w,
       :delta_w, :trig, :action, :ts, :ts
   )
   ON DUPLICATE KEY UPDATE
       mark_price = VALUES(mark_price),
       day_volume_usdc = VALUES(day_volume_usdc),
       funding_rate_hourly = VALUES(funding_rate_hourly),
       annualized_vol = VALUES(annualized_vol),
       market_beta = VALUES(market_beta),
       s1_mom_score = VALUES(s1_mom_score),
       s1_carry_score = VALUES(s1_carry_score),
       s2_trend_alignment = VALUES(s2_trend_alignment),
       s2_vol_surge_ratio = VALUES(s2_vol_surge_ratio),
       s3_funding_zscore = VALUES(s3_funding_zscore),
       s3_flush_atr_mult = VALUES(s3_flush_atr_mult),
       w_s1_carry_mom = VALUES(w_s1_carry_mom),
       w_s2_trend_breakout = VALUES(w_s2_trend_breakout),
       w_s3_event_reversion = VALUES(w_s3_event_reversion),
       raw_combined_weight = VALUES(raw_combined_weight),
       target_weight = VALUES(target_weight),
       current_weight = VALUES(current_weight),
       delta_weight = VALUES(delta_weight),
       event_trigger = VALUES(event_trigger),
       rebalance_action = VALUES(rebalance_action),
       updated_at_ms = VALUES(updated_at_ms)"#;

impl DatabaseManager {
    /// Upserts per-asset multi-sleeve signal metrics and target portfolio weights into `sleeve_signals`.
    pub fn upsert_sleeve_signal(&self, s: &SignalUpsertInput) -> Result<()> {
        let p = prepare_signal_row(s)?;
        let ts = now_ms();
        let mut conn = self.get_conn()?;

        conn.exec_drop(
            SIGNAL_UPSERT_SQL,
            params! {
                "aid" => p.input.asset_id,
                "mark" => &p.mark_price,
                "vol" => &p.day_volume_usdc,
                "fr" => &p.funding_rate_hourly,
                "avol" => p.annualized_vol,
                "beta" => p.market_beta,
                "s1_mom" => p.s1_mom_score,
                "s1_carry" => p.s1_carry_score,
                "s2_align" => p.input.s2_trend_alignment,
                "s2_surge" => p.s2_vol_surge_ratio,
                "s3_fz" => p.s3_funding_zscore,
                "s3_flush" => p.s3_flush_atr_mult,
                "w_s1" => &p.w_s1_carry_mom,
                "w_s2" => &p.w_s2_trend_breakout,
                "w_s3" => &p.w_s3_event_reversion,
                "raw_w" => &p.raw_combined_weight,
                "tgt_w" => &p.target_weight,
                "cur_w" => &p.current_weight,
                "delta_w" => &p.delta_weight,
                "trig" => &p.input.event_trigger,
                "action" => &p.input.rebalance_action,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("upsert_sleeve_signal failed: {e}")))?;
        Ok(())
    }

    /// Batch upserts all multi-sleeve signals inside a single MariaDB transaction.
    pub fn upsert_sleeve_signals_batch(&self, signals: &[SignalUpsertInput]) -> Result<usize> {
        if signals.is_empty() {
            return Ok(0);
        }
        let mut prepared = Vec::with_capacity(signals.len());
        for s in signals {
            prepared.push(prepare_signal_row(s)?);
        }

        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!("upsert_sleeve_signals_batch tx failed: {e}"))
            })?;

        let stmt = tx.prep(SIGNAL_UPSERT_SQL).map_err(|e| {
            AegisError::Database(format!("upsert_sleeve_signals_batch prep failed: {e}"))
        })?;

        tx.exec_batch(
            &stmt,
            prepared.iter().map(|p| {
                params! {
                    "aid" => p.input.asset_id,
                    "mark" => &p.mark_price,
                    "vol" => &p.day_volume_usdc,
                    "fr" => &p.funding_rate_hourly,
                    "avol" => p.annualized_vol,
                    "beta" => p.market_beta,
                    "s1_mom" => p.s1_mom_score,
                    "s1_carry" => p.s1_carry_score,
                    "s2_align" => p.input.s2_trend_alignment,
                    "s2_surge" => p.s2_vol_surge_ratio,
                    "s3_fz" => p.s3_funding_zscore,
                    "s3_flush" => p.s3_flush_atr_mult,
                    "w_s1" => &p.w_s1_carry_mom,
                    "w_s2" => &p.w_s2_trend_breakout,
                    "w_s3" => &p.w_s3_event_reversion,
                    "raw_w" => &p.raw_combined_weight,
                    "tgt_w" => &p.target_weight,
                    "cur_w" => &p.current_weight,
                    "delta_w" => &p.delta_weight,
                    "trig" => &p.input.event_trigger,
                    "action" => &p.input.rebalance_action,
                    "ts" => ts,
                }
            }),
        )
        .map_err(|e| {
            AegisError::Database(format!("upsert_sleeve_signals_batch exec failed: {e}"))
        })?;
        drop(stmt);

        tx.commit().map_err(|e| {
            AegisError::Database(format!("upsert_sleeve_signals_batch commit failed: {e}"))
        })?;
        Ok(signals.len())
    }

    /// Lists all `sleeve_signals` ordered by absolute target weight descending.
    pub fn list_sleeve_signals(&self) -> Result<Vec<SleeveSignal>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT id, asset_id, mark_price, day_volume_usdc, funding_rate_hourly,
                          annualized_vol, market_beta, s1_mom_score, s1_carry_score,
                          s2_trend_alignment, s2_vol_surge_ratio, s3_funding_zscore,
                          s3_flush_atr_mult, w_s1_carry_mom, w_s2_trend_breakout,
                          w_s3_event_reversion, raw_combined_weight, target_weight,
                          current_weight, delta_weight, event_trigger, rebalance_action,
                          created_at_ms, updated_at_ms
                   FROM sleeve_signals
                   ORDER BY ABS(target_weight) DESC, day_volume_usdc DESC"#,
            )
            .map_err(|e| AegisError::Database(format!("list_sleeve_signals failed: {e}")))?;

        Ok(rows
            .iter()
            .map(|r| SleeveSignal {
                id: row_get_i64(r, 0, 0),
                asset_id: row_get_i64(r, 1, 0),
                mark_price: row_get_f64(r, 2, 0.0),
                day_volume_usdc: row_get_f64(r, 3, 0.0),
                funding_rate_hourly: row_get_f64(r, 4, 0.0),
                annualized_vol: row_get_f64(r, 5, 0.0),
                market_beta: row_get_f64(r, 6, 0.0),
                s1_mom_score: row_get_f64(r, 7, 0.0),
                s1_carry_score: row_get_f64(r, 8, 0.0),
                s2_trend_alignment: row_get_i32(r, 9, 0),
                s2_vol_surge_ratio: row_get_f64(r, 10, 1.0),
                s3_funding_zscore: row_get_f64(r, 11, 0.0),
                s3_flush_atr_mult: row_get_f64(r, 12, 0.0),
                w_s1_carry_mom: row_get_f64(r, 13, 0.0),
                w_s2_trend_breakout: row_get_f64(r, 14, 0.0),
                w_s3_event_reversion: row_get_f64(r, 15, 0.0),
                raw_combined_weight: row_get_f64(r, 16, 0.0),
                target_weight: row_get_f64(r, 17, 0.0),
                current_weight: row_get_f64(r, 18, 0.0),
                delta_weight: row_get_f64(r, 19, 0.0),
                event_trigger: row_get_opt_string(r, 20),
                rebalance_action: row_get_string(r, 21, "HOLD"),
                created_at_ms: row_get_i64(r, 22, 0),
                updated_at_ms: row_get_i64(r, 23, 0),
            })
            .collect())
    }
}
