//! Multi-sleeve signals & target weights repository (`aegis-db::signals_repo`).

use crate::engine::DatabaseManager;
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

impl DatabaseManager {
    /// Upserts per-asset multi-sleeve signal metrics and target portfolio weights into `sleeve_signals`.
    pub fn upsert_sleeve_signal(&self, s: &SignalUpsertInput) -> Result<()> {
        let ts = now_ms();
        let mut conn = self.get_conn()?;

        conn.exec_drop(
            r#"INSERT INTO sleeve_signals (
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
                   updated_at_ms = VALUES(updated_at_ms)"#,
            params! {
                "aid" => s.asset_id,
                "mark" => s.mark_price,
                "vol" => s.day_volume_usdc,
                "fr" => s.funding_rate_hourly,
                "avol" => s.annualized_vol,
                "beta" => s.market_beta,
                "s1_mom" => s.s1_mom_score,
                "s1_carry" => s.s1_carry_score,
                "s2_align" => s.s2_trend_alignment,
                "s2_surge" => s.s2_vol_surge_ratio,
                "s3_fz" => s.s3_funding_zscore,
                "s3_flush" => s.s3_flush_atr_mult,
                "w_s1" => s.w_s1_carry_mom,
                "w_s2" => s.w_s2_trend_breakout,
                "w_s3" => s.w_s3_event_reversion,
                "raw_w" => s.raw_combined_weight,
                "tgt_w" => s.target_weight,
                "cur_w" => s.current_weight,
                "delta_w" => s.delta_weight,
                "trig" => &s.event_trigger,
                "action" => &s.rebalance_action,
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
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!("upsert_sleeve_signals_batch tx failed: {e}"))
            })?;

        let stmt = tx
            .prep(
                r#"INSERT INTO sleeve_signals (
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
                       updated_at_ms = VALUES(updated_at_ms)"#,
            )
            .map_err(|e| {
                AegisError::Database(format!("upsert_sleeve_signals_batch prep failed: {e}"))
            })?;

        tx.exec_batch(
            &stmt,
            signals.iter().map(|s| {
                params! {
                    "aid" => s.asset_id,
                    "mark" => s.mark_price,
                    "vol" => s.day_volume_usdc,
                    "fr" => s.funding_rate_hourly,
                    "avol" => s.annualized_vol,
                    "beta" => s.market_beta,
                    "s1_mom" => s.s1_mom_score,
                    "s1_carry" => s.s1_carry_score,
                    "s2_align" => s.s2_trend_alignment,
                    "s2_surge" => s.s2_vol_surge_ratio,
                    "s3_fz" => s.s3_funding_zscore,
                    "s3_flush" => s.s3_flush_atr_mult,
                    "w_s1" => s.w_s1_carry_mom,
                    "w_s2" => s.w_s2_trend_breakout,
                    "w_s3" => s.w_s3_event_reversion,
                    "raw_w" => s.raw_combined_weight,
                    "tgt_w" => s.target_weight,
                    "cur_w" => s.current_weight,
                    "delta_w" => s.delta_weight,
                    "trig" => &s.event_trigger,
                    "action" => &s.rebalance_action,
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
            .into_iter()
            .map(|r| SleeveSignal {
                id: r.get(0).unwrap_or_default(),
                asset_id: r.get(1).unwrap_or_default(),
                mark_price: r.get(2).unwrap_or_default(),
                day_volume_usdc: r.get(3).unwrap_or_default(),
                funding_rate_hourly: r.get(4).unwrap_or_default(),
                annualized_vol: r.get(5).unwrap_or_default(),
                market_beta: r.get(6).unwrap_or_default(),
                s1_mom_score: r.get(7).unwrap_or_default(),
                s1_carry_score: r.get(8).unwrap_or_default(),
                s2_trend_alignment: r.get(9).unwrap_or_default(),
                s2_vol_surge_ratio: r.get(10).unwrap_or(1.0),
                s3_funding_zscore: r.get(11).unwrap_or_default(),
                s3_flush_atr_mult: r.get(12).unwrap_or_default(),
                w_s1_carry_mom: r.get(13).unwrap_or_default(),
                w_s2_trend_breakout: r.get(14).unwrap_or_default(),
                w_s3_event_reversion: r.get(15).unwrap_or_default(),
                raw_combined_weight: r.get(16).unwrap_or_default(),
                target_weight: r.get(17).unwrap_or_default(),
                current_weight: r.get(18).unwrap_or_default(),
                delta_weight: r.get(19).unwrap_or_default(),
                event_trigger: r.get::<Option<String>, _>(20).unwrap_or(None),
                rebalance_action: r.get(21).unwrap_or_else(|| "HOLD".to_string()),
                created_at_ms: r.get(22).unwrap_or_default(),
                updated_at_ms: r.get(23).unwrap_or_default(),
            })
            .collect())
    }
}
