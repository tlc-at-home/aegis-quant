//! Causal fill sorting, windowed `userFillsByTime` caching, FIFO lot + funding replay with
//! `close_epoch_id` tracking, and `EXCHANGE_RECONCILED_FLAT` exit fill resolution
//! (`aegis-execution::reconcile::ledger`, `R14-H4`, `P1-Acct`).

use crate::http_client::HyperliquidHttpClient;
use crate::ws_client::WsStateCache;
use aegis_core::{
    normalize_hyperliquid_symbol, parse_hl_f64, parse_hl_f64_opt, parse_hl_i64_opt,
    parse_hl_u64_opt, ConfigMap, HeldAssetView, OrderSide, Result,
};
use aegis_db::FundingEventRecord;
use serde_json::Value;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

const CAUSAL_CHAIN_MAX_DFS_STEPS: usize = 4096;
const USER_FILLS_WATERMARK_OVERLAP_MS: i64 = 600_000;
const USER_FILLS_INITIAL_LOOKBACK_MS: i64 = 30 * 86_400_000;
const USER_FILLS_CACHE_RETENTION_MS: i64 = 35 * 86_400_000;
pub(crate) const LEDGER_REPLAY_MIN_INTERVAL_MS: i64 = 60_000;

#[derive(Clone, Debug)]
pub(crate) struct ParsedHlFill {
    pub coin: String,
    pub side_str: String,
    pub px: f64,
    pub sz: f64,
    pub fee: f64,
    pub closed_pnl: f64,
    pub dir: String,
    pub time_ms: i64,
    pub oid: u64,
    pub tid: u64,
    pub start_position: Option<f64>,
}

#[derive(Default)]
pub(crate) struct ReconcileLedgerCache {
    pub last_fill_watermark_ms: i64,
    pub last_rest_verify_ms: i64,
    pub cached_fills: Vec<ParsedHlFill>,
    pub seen_fill_keys: HashSet<(i64, u64, u64, String, String, u64, u64)>,
    pub last_replay_ms: i64,
    pub prev_open_szi_by_coin: HashMap<String, f64>,
}

fn fill_signed_sz(fill: &ParsedHlFill) -> f64 {
    if fill.side_str == "B" {
        fill.sz
    } else {
        -fill.sz
    }
}

fn szi_approx_eq(a: f64, b: f64) -> bool {
    let tol = 1e-7_f64.max(a.abs().max(b.abs()) * 1e-10);
    (a - b).abs() <= tol
}

pub(crate) fn fill_is_closing_dir(fill: &ParsedHlFill) -> bool {
    fill.closed_pnl.abs() > 1e-9 || fill.dir.starts_with("Close") || fill.dir.contains('>')
}

fn dfs_causal_chain(
    sp: &[f64],
    end: &[f64],
    used: &mut [bool],
    path: &mut Vec<usize>,
    cur_szi: f64,
    steps: &mut usize,
    max_steps: usize,
) -> bool {
    if path.len() == sp.len() {
        return true;
    }
    if *steps >= max_steps {
        return false;
    }
    *steps += 1;

    for idx in 0..sp.len() {
        if used[idx] || !szi_approx_eq(sp[idx], cur_szi) {
            continue;
        }
        used[idx] = true;
        path.push(idx);
        if dfs_causal_chain(sp, end, used, path, end[idx], steps, max_steps) {
            return true;
        }
        path.pop();
        used[idx] = false;
        if *steps >= max_steps {
            return false;
        }
    }
    false
}

fn find_causal_chain_from_start(group: &[ParsedHlFill], start_szi: f64) -> Option<Vec<usize>> {
    let sp: Vec<f64> = group
        .iter()
        .map(|f| f.start_position)
        .collect::<Option<Vec<_>>>()?;
    let end: Vec<f64> = group
        .iter()
        .zip(sp.iter())
        .map(|(f, &s)| s + fill_signed_sz(f))
        .collect();
    let mut used = vec![false; group.len()];
    let mut path = Vec::with_capacity(group.len());
    let mut steps = 0usize;
    dfs_causal_chain(
        &sp,
        &end,
        &mut used,
        &mut path,
        start_szi,
        &mut steps,
        CAUSAL_CHAIN_MAX_DFS_STEPS,
    )
    .then_some(path)
}

fn resolve_same_ms_group(
    group: &[ParsedHlFill],
    running_szi: Option<f64>,
    next_group: Option<&[ParsedHlFill]>,
) -> Option<Vec<usize>> {
    let sp: Vec<f64> = group
        .iter()
        .map(|f| f.start_position)
        .collect::<Option<Vec<_>>>()?;
    let end: Vec<f64> = group
        .iter()
        .zip(sp.iter())
        .map(|(f, &s)| s + fill_signed_sz(f))
        .collect();
    let next_has_positions = next_group
        .is_some_and(|ng| !ng.is_empty() && ng.iter().all(|f| f.start_position.is_some()));

    let matches_next = |chain: &[usize]| -> bool {
        let Some(last_idx) = chain.last().copied() else {
            return false;
        };
        let Some(ng) = next_group else {
            return false;
        };
        next_has_positions && find_causal_chain_from_start(ng, end[last_idx]).is_some()
    };

    if let Some(s0) = running_szi {
        let mut best: Option<(bool, Vec<usize>)> = None;
        for first_idx in 0..group.len() {
            if !szi_approx_eq(sp[first_idx], s0) {
                continue;
            }
            let mut used = vec![false; group.len()];
            used[first_idx] = true;
            let mut path = Vec::with_capacity(group.len());
            path.push(first_idx);
            let mut steps = 0usize;
            if dfs_causal_chain(
                &sp,
                &end,
                &mut used,
                &mut path,
                end[first_idx],
                &mut steps,
                CAUSAL_CHAIN_MAX_DFS_STEPS,
            ) {
                let next_ok = matches_next(&path);
                let better = match &best {
                    None => true,
                    Some((best_next, best_path)) => {
                        (next_ok, Reverse(&path)) > (*best_next, Reverse(best_path))
                    }
                };
                if better {
                    best = Some((next_ok, path));
                }
            }
        }
        if let Some((_, best_path)) = best {
            return Some(best_path);
        }
    }

    let mut best: Option<((bool, bool, bool), Vec<usize>)> = None;
    for first_idx in 0..group.len() {
        let mut used = vec![false; group.len()];
        used[first_idx] = true;
        let mut path = Vec::with_capacity(group.len());
        path.push(first_idx);
        let mut steps = 0usize;
        if dfs_causal_chain(
            &sp,
            &end,
            &mut used,
            &mut path,
            end[first_idx],
            &mut steps,
            CAUSAL_CHAIN_MAX_DFS_STEPS,
        ) {
            let next_ok = matches_next(&path);
            let zero_in_degree =
                !(0..group.len()).any(|j| j != first_idx && szi_approx_eq(end[j], sp[first_idx]));
            let starts_flat_open =
                szi_approx_eq(sp[first_idx], 0.0) && !fill_is_closing_dir(&group[first_idx]);
            let score = (next_ok, zero_in_degree, starts_flat_open);
            let better = match &best {
                None => true,
                Some((best_score, best_path)) => {
                    (score, Reverse(&path)) > (*best_score, Reverse(best_path))
                }
            };
            if better {
                best = Some((score, path));
            }
        }
    }
    best.map(|(_, path)| path)
}

pub(crate) fn sort_fills_causally(fills: &mut [ParsedHlFill]) {
    if fills.len() <= 1 {
        return;
    }
    fills.sort_by_key(|f| (f.time_ms, f.tid));

    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while i < fills.len() {
        let t = fills[i].time_ms;
        let mut j = i + 1;
        while j < fills.len() && fills[j].time_ms == t {
            j += 1;
        }
        ranges.push((i, j));
        i = j;
    }

    let mut running_szi: Option<f64> = None;
    for idx in 0..ranges.len() {
        let (start, end_idx) = ranges[idx];
        let next_group = ranges.get(idx + 1).map(|&(ns, ne)| &fills[ns..ne]);
        if end_idx - start >= 2 {
            if let Some(order) =
                resolve_same_ms_group(&fills[start..end_idx], running_szi, next_group)
            {
                let original = fills[start..end_idx].to_vec();
                for (dst_pos, &src_pos) in order.iter().enumerate() {
                    fills[start + dst_pos] = original[src_pos].clone();
                }
            }
        }
        let last = &fills[end_idx - 1];
        running_szi = if let Some(sp) = last.start_position {
            Some(sp + fill_signed_sz(last))
        } else {
            running_szi.map(|prev| {
                prev + fills[start..end_idx]
                    .iter()
                    .map(fill_signed_sz)
                    .sum::<f64>()
            })
        };
    }
}

#[derive(Default)]
pub(crate) struct AggClosingOrder {
    pub coin: String,
    pub close_epoch_id: String,
    pub side: Option<OrderSide>,
    pub total_sz: f64,
    pub notional_sum: f64,
    pub gross_closed_pnl: f64,
    pub close_fees_usdc: f64,
    pub open_fees_usdc: f64,
    pub open_time_ms: Option<i64>,
    pub min_time_ms: i64,
    pub max_time_ms: i64,
    pub funding_accrued_usdc: f64,
}

#[derive(Clone, Debug)]
pub(crate) struct ParsedHlFunding {
    pub coin: String,
    pub time_ms: i64,
    pub usdc: f64,
    pub szi: f64,
    pub funding_rate: f64,
    pub dedup_key: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct OpenEpochRealizedSummary {
    pub partial_closed_qty: f64,
    pub gross_closed_pnl: f64,
    pub close_fees_usdc: f64,
    pub open_fills_fees_usdc: f64,
    pub has_complete_open_fills: bool,
}

#[derive(Default)]
pub(crate) struct FifoFundingReplayOutput {
    pub closed_orders: Vec<(u64, AggClosingOrder)>,
    pub open_epoch_booked_funding_by_coin: HashMap<String, f64>,
    pub open_epoch_realized_by_coin: HashMap<String, OpenEpochRealizedSummary>,
    pub funding_events_with_remaining: Vec<(ParsedHlFunding, f64)>,
}

struct OpenLot {
    rem_sz: f64,
    orig_sz: f64,
    fee: f64,
    funding_accrued: f64,
    funding_by_ev: Vec<(usize, f64)>,
    time_ms: i64,
}

fn is_open_or_add_fill(fill: &ParsedHlFill, prev_szi: f64) -> bool {
    let eff_prev = if prev_szi.abs() <= 1e-9 {
        0.0
    } else {
        prev_szi
    };
    (eff_prev == 0.0 && !fill_is_closing_dir(fill))
        || (eff_prev != 0.0 && eff_prev.signum() == fill_signed_sz(fill).signum())
}

struct CoinReplayState {
    coin_upper: String,
    epoch_seq: u64,
    open_lots: VecDeque<OpenLot>,
    prev_szi: f64,
    rem_usdc: Vec<f64>,
    active_epoch_ev_indices: Vec<usize>,
    open_epoch_booked_funding: f64,
    active_epoch_open_fees_usdc: f64,
    active_epoch_has_initial_open_fill: bool,
}

impl CoinReplayState {
    fn new(coin_upper: String, funding_len: usize) -> Self {
        Self {
            coin_upper,
            epoch_seq: 1,
            open_lots: VecDeque::new(),
            prev_szi: 0.0,
            rem_usdc: vec![0.0; funding_len],
            active_epoch_ev_indices: Vec::new(),
            open_epoch_booked_funding: 0.0,
            active_epoch_open_fees_usdc: 0.0,
            active_epoch_has_initial_open_fill: false,
        }
    }

    fn apply_funding(&mut self, ev_idx: usize, ev: &ParsedHlFunding) {
        let total_open: f64 = self.open_lots.iter().map(|l| l.rem_sz).sum();
        if total_open > 1e-9 {
            for lot in self.open_lots.iter_mut() {
                let share = ev.usdc * (lot.rem_sz / total_open);
                lot.funding_accrued += share;
                lot.funding_by_ev.push((ev_idx, share));
            }
            self.rem_usdc[ev_idx] = ev.usdc;
            self.active_epoch_ev_indices.push(ev_idx);
        } else {
            self.rem_usdc[ev_idx] = 0.0;
        }
    }

    fn apply_fill(
        &mut self,
        fill: &ParsedHlFill,
        agg_by_key: &mut BTreeMap<(String, u64, u64), AggClosingOrder>,
    ) {
        if self.prev_szi.abs() <= 1e-9 {
            self.prev_szi = 0.0;
        }
        let signed_sz = fill_signed_sz(fill);
        let is_closing_dir = fill_is_closing_dir(fill);

        if is_open_or_add_fill(fill, self.prev_szi) {
            if self.prev_szi == 0.0 {
                self.active_epoch_has_initial_open_fill = true;
            }
            self.active_epoch_open_fees_usdc += fill.fee.max(0.0);
            self.open_lots.push_back(OpenLot {
                rem_sz: fill.sz,
                orig_sz: fill.sz,
                fee: fill.fee,
                funding_accrued: 0.0,
                funding_by_ev: Vec::new(),
                time_ms: fill.time_ms,
            });
            self.prev_szi += signed_sz;
            return;
        }

        let had_open_pos = self.prev_szi.abs() > 1e-9;
        let (closed_sz, closed_pos_side) = if had_open_pos {
            (
                self.prev_szi.abs().min(fill.sz),
                if self.prev_szi > 0.0 {
                    OrderSide::Long
                } else {
                    OrderSide::Short
                },
            )
        } else {
            let fallback_side = if fill.dir.starts_with("Close Long")
                || fill.dir.starts_with("Long >")
                || (!fill.dir.contains("Short") && fill.side_str == "A")
            {
                OrderSide::Long
            } else {
                OrderSide::Short
            };
            (fill.sz, fallback_side)
        };
        let flipped_open_sz = (fill.sz - closed_sz).max(0.0);
        let close_fee = if fill.sz > 0.0 {
            fill.fee * (closed_sz / fill.sz)
        } else {
            0.0
        };
        let open_fee = (fill.fee - close_fee).max(0.0);

        let mut matched_open_fee = 0.0_f64;
        let mut matched_funding = 0.0_f64;
        let mut earliest_open_time_ms: Option<i64> = None;
        let mut rem_to_close = closed_sz;
        while rem_to_close > 1e-9 {
            let Some(front) = self.open_lots.front_mut() else {
                break;
            };
            let take = rem_to_close.min(front.rem_sz);
            let frac = if front.rem_sz > 0.0 {
                (take / front.rem_sz).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let matched_funding_part = front.funding_accrued * frac;
            front.funding_accrued -= matched_funding_part;
            matched_funding += matched_funding_part;
            for (ev_i, lot_share) in &mut front.funding_by_ev {
                let ev_part = *lot_share * frac;
                *lot_share -= ev_part;
                self.rem_usdc[*ev_i] -= ev_part;
                if self.rem_usdc[*ev_i].abs() <= 1e-12 {
                    self.rem_usdc[*ev_i] = 0.0;
                }
            }
            if front.orig_sz > 0.0 {
                matched_open_fee += front.fee * (take / front.orig_sz);
            }
            earliest_open_time_ms =
                Some(earliest_open_time_ms.map_or(front.time_ms, |t| t.min(front.time_ms)));
            front.rem_sz -= take;
            rem_to_close -= take;
            if front.rem_sz <= 1e-9 {
                self.open_lots.pop_front();
            }
        }

        if closed_sz > 1e-9 && (is_closing_dir || had_open_pos) {
            let epoch_id = format!("{}:{}", self.coin_upper, self.epoch_seq);
            let entry = agg_by_key
                .entry((fill.coin.clone(), fill.oid, self.epoch_seq))
                .or_insert_with(|| AggClosingOrder {
                    coin: fill.coin.clone(),
                    close_epoch_id: epoch_id,
                    side: Some(closed_pos_side),
                    min_time_ms: fill.time_ms,
                    max_time_ms: fill.time_ms,
                    ..Default::default()
                });
            entry.total_sz += closed_sz;
            entry.notional_sum += fill.px * closed_sz;
            entry.gross_closed_pnl += fill.closed_pnl;
            entry.close_fees_usdc += close_fee;
            entry.open_fees_usdc += matched_open_fee;
            entry.funding_accrued_usdc += matched_funding;
            if let Some(ot) = earliest_open_time_ms {
                entry.open_time_ms = Some(entry.open_time_ms.map_or(ot, |cur| cur.min(ot)));
            }
            entry.min_time_ms = entry.min_time_ms.min(fill.time_ms);
            entry.max_time_ms = entry.max_time_ms.max(fill.time_ms);
        }

        let open_sz_after: f64 = self.open_lots.iter().map(|l| l.rem_sz).sum();
        if open_sz_after <= 1e-9 || flipped_open_sz > 1e-9 {
            for &ev_i in &self.active_epoch_ev_indices {
                self.rem_usdc[ev_i] = 0.0;
            }
            self.active_epoch_ev_indices.clear();
            self.open_epoch_booked_funding = 0.0;
            self.active_epoch_open_fees_usdc = 0.0;
            self.active_epoch_has_initial_open_fill = false;
            self.epoch_seq = self.epoch_seq.saturating_add(1);
        } else {
            self.open_epoch_booked_funding += matched_funding;
        }

        if flipped_open_sz > 1e-9 {
            self.active_epoch_has_initial_open_fill = true;
            self.active_epoch_open_fees_usdc += open_fee;
            self.open_lots.push_back(OpenLot {
                rem_sz: flipped_open_sz,
                orig_sz: flipped_open_sz,
                fee: open_fee,
                funding_accrued: 0.0,
                funding_by_ev: Vec::new(),
                time_ms: fill.time_ms,
            });
        }

        if had_open_pos || fill.start_position.is_some() {
            self.prev_szi += signed_sz;
            if self.prev_szi.abs() <= 1e-9 {
                self.prev_szi = 0.0;
            }
        } else if flipped_open_sz > 1e-9 {
            self.prev_szi = if signed_sz > 0.0 {
                flipped_open_sz
            } else {
                -flipped_open_sz
            };
        } else {
            self.prev_szi = 0.0;
        }
    }
}

pub(crate) fn compute_fifo_closed_orders_and_funding(
    parsed_fills_by_coin: &BTreeMap<String, Vec<ParsedHlFill>>,
    funding_by_coin: &HashMap<String, Vec<ParsedHlFunding>>,
    open_szi_by_coin: &HashMap<String, f64>,
) -> FifoFundingReplayOutput {
    let mut all_coins: BTreeSet<String> = parsed_fills_by_coin.keys().cloned().collect();
    all_coins.extend(funding_by_coin.keys().cloned());

    let mut agg_by_key: BTreeMap<(String, u64, u64), AggClosingOrder> = BTreeMap::new();
    let open_epoch_booked_funding_by_coin: HashMap<String, f64> = HashMap::new();
    let mut open_epoch_realized_by_coin: HashMap<String, OpenEpochRealizedSummary> = HashMap::new();
    let mut funding_events_with_remaining: Vec<(ParsedHlFunding, f64)> = Vec::new();

    for coin_upper in all_coins {
        let fills: &[ParsedHlFill] = parsed_fills_by_coin
            .get(&coin_upper)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let funding_raw: &[ParsedHlFunding] = funding_by_coin
            .get(&coin_upper)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let live_open_szi = open_szi_by_coin.get(&coin_upper).copied().unwrap_or(0.0);
        let is_coin_currently_open = live_open_szi.abs() > 1e-9;

        let mut fund_indices: Vec<usize> = (0..funding_raw.len()).collect();
        fund_indices.sort_by_key(|&idx| (funding_raw[idx].time_ms, idx));

        let mut state = CoinReplayState::new(coin_upper.clone(), funding_raw.len());
        let earliest_funding_ts = fund_indices.first().map(|&idx| funding_raw[idx].time_ms);

        if let Some(first_fill) = fills.first() {
            let inferred_sp = first_fill.start_position.or_else(|| {
                if is_coin_currently_open {
                    let net_fills: f64 = fills.iter().map(fill_signed_sz).sum();
                    let sp = live_open_szi - net_fills;
                    (sp.abs() > 1e-9).then_some(sp)
                } else {
                    None
                }
            });
            if let Some(sp) = inferred_sp {
                if sp.abs() > 1e-9 {
                    let earliest_ts = earliest_funding_ts
                        .map_or(first_fill.time_ms, |ft| first_fill.time_ms.min(ft));
                    state.open_lots.push_back(OpenLot {
                        rem_sz: sp.abs(),
                        orig_sz: sp.abs(),
                        fee: 0.0,
                        funding_accrued: 0.0,
                        funding_by_ev: Vec::new(),
                        time_ms: earliest_ts.saturating_sub(1),
                    });
                    state.prev_szi = sp;
                }
            }
        } else if !fund_indices.is_empty() && is_coin_currently_open {
            let earliest_ts = earliest_funding_ts.unwrap_or(0);
            state.open_lots.push_back(OpenLot {
                rem_sz: live_open_szi.abs(),
                orig_sz: live_open_szi.abs(),
                fee: 0.0,
                funding_accrued: 0.0,
                funding_by_ev: Vec::new(),
                time_ms: earliest_ts.saturating_sub(1),
            });
            state.prev_szi = live_open_szi;
        }

        let mut f_idx = 0usize;
        let mut e_idx = 0usize;
        while f_idx < fills.len() || e_idx < fund_indices.len() {
            let cur_ms = match (fills.get(f_idx), fund_indices.get(e_idx)) {
                (Some(f), Some(&ev_i)) => f.time_ms.min(funding_raw[ev_i].time_ms),
                (Some(f), None) => f.time_ms,
                (None, Some(&ev_i)) => funding_raw[ev_i].time_ms,
                (None, None) => break,
            };
            while f_idx < fills.len()
                && fills[f_idx].time_ms == cur_ms
                && is_open_or_add_fill(&fills[f_idx], state.prev_szi)
            {
                state.apply_fill(&fills[f_idx], &mut agg_by_key);
                f_idx += 1;
            }
            while e_idx < fund_indices.len() && funding_raw[fund_indices[e_idx]].time_ms == cur_ms {
                let ev_i = fund_indices[e_idx];
                state.apply_funding(ev_i, &funding_raw[ev_i]);
                e_idx += 1;
            }
            while f_idx < fills.len() && fills[f_idx].time_ms == cur_ms {
                state.apply_fill(&fills[f_idx], &mut agg_by_key);
                f_idx += 1;
            }
        }

        if is_coin_currently_open {
            // W3 Single Booking Rule: Do NOT emit `CLOSED` `hl-closed-*` rows for partial trims
            // belonging to the still-open position epoch (`epoch == state.epoch_seq`), and keep
            // 100% of active open-epoch funding on the OPEN row + `funding_events.remaining_usdc`.
            let active_epoch = state.epoch_seq;
            let mut active_keys = Vec::new();
            let mut summary = OpenEpochRealizedSummary {
                open_fills_fees_usdc: state.active_epoch_open_fees_usdc,
                has_complete_open_fills: state.active_epoch_has_initial_open_fill,
                ..Default::default()
            };
            for (k, agg) in &agg_by_key {
                if k.0.eq_ignore_ascii_case(&coin_upper) && k.2 == active_epoch {
                    summary.partial_closed_qty += agg.total_sz;
                    summary.gross_closed_pnl += agg.gross_closed_pnl;
                    summary.close_fees_usdc += agg.close_fees_usdc;
                    active_keys.push(k.clone());
                }
            }
            for k in active_keys {
                agg_by_key.remove(&k);
            }
            if summary.partial_closed_qty > 1e-9 || summary.open_fills_fees_usdc > 1e-9 {
                open_epoch_realized_by_coin.insert(coin_upper.clone(), summary);
            }
            for &ev_i in &state.active_epoch_ev_indices {
                state.rem_usdc[ev_i] = funding_raw[ev_i].usdc;
            }
        } else {
            // Coin is confirmed flat on Hyperliquid; settle any residual open-lot funding to 0.
            for &ev_i in &state.active_epoch_ev_indices {
                state.rem_usdc[ev_i] = 0.0;
            }
        }

        for &ev_i in &fund_indices {
            funding_events_with_remaining.push((funding_raw[ev_i].clone(), state.rem_usdc[ev_i]));
        }
    }

    let mut closed_orders: Vec<(u64, AggClosingOrder)> = agg_by_key
        .into_iter()
        .map(|((_, oid, _), agg)| (oid, agg))
        .collect();
    closed_orders.sort_by_key(|(_, agg)| agg.max_time_ms);

    FifoFundingReplayOutput {
        closed_orders,
        open_epoch_booked_funding_by_coin,
        open_epoch_realized_by_coin,
        funding_events_with_remaining,
    }
}

pub(crate) fn derive_open_position_opened_at_ms(
    fills: &[ParsedHlFill],
    current_szi: f64,
) -> Option<i64> {
    if fills.is_empty() || current_szi.abs() <= 1e-9 {
        return None;
    }
    let mut pos_after = current_szi;
    let mut oldest_open_ts: Option<i64> = None;
    for f in fills.iter().rev() {
        let fill_signed = if f.side_str == "B" { f.sz } else { -f.sz };
        if fill_signed * current_szi > 0.0 {
            oldest_open_ts = Some(f.time_ms);
        }
        let pos_before = pos_after - fill_signed;
        if pos_before.abs() <= 1e-7 || (pos_before * current_szi) <= 0.0 {
            break;
        }
        pos_after = pos_before;
    }
    oldest_open_ts
}

pub(crate) fn funding_dedup_key(
    f_time: i64,
    coin_upper: &str,
    usdc: f64,
    szi: f64,
    raw_hash: Option<&str>,
    n_suffix: Option<&str>,
) -> String {
    let mut dedup_key = match raw_hash {
        Some(h) => match n_suffix {
            Some(n) => format!("{f_time}:{coin_upper}:{h}:{n}"),
            None => format!("{f_time}:{coin_upper}:{h}"),
        },
        None => match n_suffix {
            Some(n) => format!("{f_time}:{coin_upper}:{usdc:.6}:{szi:.6}:{n}"),
            None => format!("{f_time}:{coin_upper}:{usdc:.6}:{szi:.6}"),
        },
    };
    if dedup_key.chars().count() > 128 {
        dedup_key = dedup_key.chars().take(128).collect();
    }
    dedup_key
}

pub(crate) fn parse_raw_fills(raw_fills: &[Value], fallback_ts: i64) -> Vec<ParsedHlFill> {
    let mut out = Vec::with_capacity(raw_fills.len());
    for fill in raw_fills {
        let raw_coin = fill
            .get("coin")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if raw_coin.is_empty() {
            continue;
        }
        let Ok((_, coin)) = normalize_hyperliquid_symbol(raw_coin) else {
            continue;
        };
        let px = parse_hl_f64(fill.get("px"));
        let sz = parse_hl_f64(fill.get("sz"));
        if px <= 0.0 || sz <= 0.0 {
            continue;
        }
        let fill_time = parse_hl_i64_opt(fill.get("time"))
            .filter(|&t| t > 0)
            .unwrap_or(fallback_ts);
        let tid = parse_hl_u64_opt(fill.get("tid")).unwrap_or(0);
        let oid = parse_hl_u64_opt(fill.get("oid"))
            .filter(|&id| id > 0)
            .or((tid > 0).then_some(tid))
            .unwrap_or(fill_time as u64);
        out.push(ParsedHlFill {
            coin,
            side_str: fill
                .get("side")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string(),
            px,
            sz,
            fee: parse_hl_f64(fill.get("fee")),
            closed_pnl: parse_hl_f64(fill.get("closedPnl")),
            dir: fill
                .get("dir")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string(),
            time_ms: fill_time,
            oid,
            tid,
            start_position: parse_hl_f64_opt(fill.get("startPosition")),
        });
    }
    out
}

const WS_FILLS_REST_VERIFY_INTERVAL_MS: i64 = 300_000;

fn merge_raw_fills_into_cache(
    cache: &mut ReconcileLedgerCache,
    raw_fills: &[Value],
    earliest_local_open_ms: Option<i64>,
    ts: i64,
) -> BTreeMap<String, Vec<ParsedHlFill>> {
    let parsed = parse_raw_fills(raw_fills, ts);
    let mut max_fill_ts = 0i64;
    for f in parsed {
        max_fill_ts = max_fill_ts.max(f.time_ms);
        let dedup_key = (
            f.time_ms,
            f.oid,
            f.tid,
            f.coin.to_ascii_uppercase(),
            f.side_str.clone(),
            f.sz.to_bits(),
            f.px.to_bits(),
        );
        if cache.seen_fill_keys.insert(dedup_key) {
            cache.cached_fills.push(f);
        }
    }

    if max_fill_ts > 0 {
        cache.last_fill_watermark_ms = cache.last_fill_watermark_ms.max(max_fill_ts);
    }

    let min_keep_ts = earliest_local_open_ms
        .map(|t| t.saturating_sub(86_400_000))
        .unwrap_or_else(|| ts.saturating_sub(USER_FILLS_CACHE_RETENTION_MS));
    if cache.cached_fills.len() > 2000 {
        cache.cached_fills.retain(|f| f.time_ms >= min_keep_ts);
        cache.seen_fill_keys.clear();
        for f in &cache.cached_fills {
            cache.seen_fill_keys.insert((
                f.time_ms,
                f.oid,
                f.tid,
                f.coin.to_ascii_uppercase(),
                f.side_str.clone(),
                f.sz.to_bits(),
                f.px.to_bits(),
            ));
        }
    }

    let mut by_coin: BTreeMap<String, Vec<ParsedHlFill>> = BTreeMap::new();
    for f in &cache.cached_fills {
        by_coin
            .entry(f.coin.to_ascii_uppercase())
            .or_default()
            .push(f.clone());
    }
    for fills in by_coin.values_mut() {
        sort_fills_causally(fills);
    }
    by_coin
}

/// Fetches windowed `userFillsByTime` or consumes fresh User WS `userFills` ring when connected &
/// gap-filled (`R14-H4` Phase 2), falling back to REST on reconnect gap-fill or periodic 5m verify.
#[allow(clippy::too_many_arguments)]
pub(crate) fn fetch_windowed_fills_with_ws_unlocked(
    http: &HyperliquidHttpClient,
    wallet: &str,
    cache: &mut ReconcileLedgerCache,
    ws_cache: Option<&WsStateCache>,
    force_rest: bool,
    ws_disconnected_since_ms: i64,
    earliest_local_open_ms: Option<i64>,
    ts: i64,
    cfg: &ConfigMap,
) -> Result<BTreeMap<String, Vec<ParsedHlFill>>> {
    if !force_rest {
        if let Some(ws) = ws_cache {
            if ws.is_user_ws_ready_for_reconcile()
                && cache.last_rest_verify_ms > 0
                && ts.saturating_sub(cache.last_rest_verify_ms) < WS_FILLS_REST_VERIFY_INTERVAL_MS
            {
                let (ws_fills, _) = ws.get_recent_user_fills();
                return Ok(merge_raw_fills_into_cache(
                    cache,
                    &ws_fills,
                    earliest_local_open_ms,
                    ts,
                ));
            }
        }
    }

    let mut start_ms = if cache.last_fill_watermark_ms > 0 {
        cache
            .last_fill_watermark_ms
            .saturating_sub(USER_FILLS_WATERMARK_OVERLAP_MS)
            .max(0)
    } else {
        let base_lookback = ts.saturating_sub(USER_FILLS_INITIAL_LOOKBACK_MS).max(0);
        earliest_local_open_ms
            .map(|t| t.saturating_sub(86_400_000).min(base_lookback).max(0))
            .unwrap_or(base_lookback)
    };
    if ws_disconnected_since_ms > 0 {
        start_ms = start_ms
            .min(ws_disconnected_since_ms.saturating_sub(60_000))
            .max(0);
    }

    let raw_fills = http.fetch_user_fills_by_time(wallet, start_ms, None, false, cfg)?;
    let mut by_coin = merge_raw_fills_into_cache(cache, &raw_fills, earliest_local_open_ms, ts);
    if let Some(ws) = ws_cache {
        if ws.is_user_ws_connected() {
            let (ws_fills, _) = ws.get_recent_user_fills();
            if !ws_fills.is_empty() {
                by_coin = merge_raw_fills_into_cache(cache, &ws_fills, earliest_local_open_ms, ts);
            }
        }
    }
    cache.last_fill_watermark_ms = cache.last_fill_watermark_ms.max(ts);
    cache.last_rest_verify_ms = ts;
    Ok(by_coin)
}

/// Converts DB-persisted `FundingEventRecord` rows into `ParsedHlFunding` grouped by uppercase coin.
pub(crate) fn group_db_funding_records(
    records: &[FundingEventRecord],
) -> HashMap<String, Vec<ParsedHlFunding>> {
    let mut by_coin: HashMap<String, Vec<ParsedHlFunding>> = HashMap::new();
    for r in records {
        let coin_upper = r.coin.trim().to_ascii_uppercase();
        if coin_upper.is_empty() || r.event_time_ms <= 0 || r.usdc.abs() <= 1e-12 {
            continue;
        }
        by_coin
            .entry(coin_upper)
            .or_default()
            .push(ParsedHlFunding {
                coin: r.coin.clone(),
                time_ms: r.event_time_ms,
                usdc: r.usdc,
                szi: r.position_szi,
                funding_rate: r.funding_rate,
                dedup_key: r.dedup_key.clone(),
            });
    }
    for list in by_coin.values_mut() {
        list.sort_by_key(|ev| ev.time_ms);
    }
    by_coin
}

/// Resolves actual closing exit VWAP price and fee from `parsed_fills` for `EXCHANGE_RECONCILED_FLAT`
/// (`P1-Acct`), falling back to `(h.mark_price, 0.0)` when fills are unavailable.
pub(crate) fn resolve_flat_exit_fill_stats(
    h: &HeldAssetView,
    fills_for_coin: Option<&Vec<ParsedHlFill>>,
) -> (f64, f64) {
    let Some(fills) = fills_for_coin else {
        return (h.mark_price, 0.0);
    };
    let expected_close_side = match OrderSide::parse(&h.side) {
        Ok(OrderSide::Long) => "A",
        Ok(OrderSide::Short) => "B",
        Err(_) => return (h.mark_price, 0.0),
    };
    let min_time_ms = h
        .opened_at_ms
        .unwrap_or(h.created_at_ms)
        .saturating_sub(5_000);
    let mut rem_qty = h.quantity.abs();
    if !rem_qty.is_finite() || rem_qty <= 1e-9 {
        return (h.mark_price, 0.0);
    }

    let mut notional_sum = 0.0_f64;
    let mut qty_sum = 0.0_f64;
    let mut fee_sum = 0.0_f64;
    let mut matched_any = false;

    for f in fills.iter().rev() {
        if f.time_ms < min_time_ms {
            break;
        }
        let matches_closing = f.side_str.eq_ignore_ascii_case(expected_close_side)
            && (fill_is_closing_dir(f) || !matched_any);
        if !matches_closing {
            if matched_any {
                break;
            }
            continue;
        }
        matched_any = true;
        let take = rem_qty.min(f.sz);
        let frac = if f.sz > 0.0 {
            (take / f.sz).clamp(0.0, 1.0)
        } else {
            0.0
        };
        notional_sum += f.px * take;
        qty_sum += take;
        fee_sum += f.fee * frac;
        rem_qty -= take;
        if rem_qty <= 1e-8 {
            break;
        }
    }

    if qty_sum > 1e-9 && notional_sum > 0.0 {
        (notional_sum / qty_sum, fee_sum.max(0.0))
    } else {
        (h.mark_price, 0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    fn make_fill(
        coin: &str,
        side_str: &str,
        px: f64,
        sz: f64,
        fee: f64,
        closed_pnl: f64,
        dir: &str,
        time_ms: i64,
        oid: u64,
        tid: u64,
        start_position: Option<f64>,
    ) -> ParsedHlFill {
        ParsedHlFill {
            coin: coin.to_string(),
            side_str: side_str.to_string(),
            px,
            sz,
            fee,
            closed_pnl,
            dir: dir.to_string(),
            time_ms,
            oid,
            tid,
            start_position,
        }
    }

    #[test]
    fn test_sort_fills_causally_chip_same_ms_non_monotonic_tid() {
        let same_ms = 1_795_023_372_707i64;
        let mut fills_a = vec![
            make_fill(
                "CHIP",
                "B",
                0.0278,
                2773.0,
                0.03,
                0.0,
                "Open Long",
                same_ms,
                9001,
                100,
                Some(812.0),
            ),
            make_fill(
                "CHIP",
                "B",
                0.0277,
                812.0,
                0.01,
                0.0,
                "Open Long",
                same_ms,
                9001,
                200,
                Some(0.0),
            ),
            make_fill(
                "CHIP",
                "A",
                0.0285,
                3585.0,
                0.05,
                2.80,
                "Close Long",
                same_ms + 60_000,
                9002,
                300,
                Some(3585.0),
            ),
        ];
        sort_fills_causally(&mut fills_a);
        assert_eq!(fills_a[0].tid, 200);
        assert_eq!(fills_a[1].tid, 100);
        assert_eq!(fills_a[2].tid, 300);

        let mut map_a = BTreeMap::new();
        map_a.insert("CHIP".to_string(), fills_a);
        let closed_a =
            compute_fifo_closed_orders_and_funding(&map_a, &HashMap::new(), &HashMap::new())
                .closed_orders;
        assert_eq!(closed_a.len(), 1);
        assert!((closed_a[0].1.total_sz - 3585.0).abs() < 1e-9);
        assert_eq!(closed_a[0].1.close_epoch_id, "CHIP:1");
    }

    #[test]
    fn test_multi_ioc_close_epoch_id_grouping_and_flip_increment() {
        let mut fills = BTreeMap::new();
        fills.insert(
            "SOL".to_string(),
            vec![
                make_fill(
                    "SOL",
                    "B",
                    100.0,
                    10.0,
                    0.1,
                    0.0,
                    "Open Long",
                    1000,
                    1,
                    1,
                    Some(0.0),
                ),
                // Partial IOC close #1 (still 4.0 open -> epoch 1)
                make_fill(
                    "SOL",
                    "A",
                    102.0,
                    6.0,
                    0.05,
                    12.0,
                    "Close Long",
                    2000,
                    2,
                    2,
                    Some(10.0),
                ),
                // Partial IOC close #2 + flip to short (closes remaining 4.0 in epoch 1, opens 5.0 short -> epoch advances to 2)
                make_fill(
                    "SOL",
                    "A",
                    103.0,
                    9.0,
                    0.09,
                    12.0,
                    "Long > Short",
                    3000,
                    3,
                    3,
                    Some(4.0),
                ),
                // Close short in epoch 2
                make_fill(
                    "SOL",
                    "B",
                    99.0,
                    5.0,
                    0.05,
                    20.0,
                    "Close Short",
                    4000,
                    4,
                    4,
                    Some(-5.0),
                ),
            ],
        );

        let out = compute_fifo_closed_orders_and_funding(&fills, &HashMap::new(), &HashMap::new());
        assert_eq!(out.closed_orders.len(), 3);
        assert_eq!(out.closed_orders[0].1.close_epoch_id, "SOL:1");
        assert_eq!(out.closed_orders[1].1.close_epoch_id, "SOL:1");
        assert_eq!(out.closed_orders[2].1.close_epoch_id, "SOL:2");
    }

    #[test]
    fn test_resolve_flat_exit_fill_stats_vwap_and_fee() {
        let held = HeldAssetView {
            position_id: 1,
            asset_id: 1,
            symbol: "BTC-USDC".to_string(),
            coin: "BTC".to_string(),
            client_order_id: "btc-test-1".to_string(),
            side: "LONG".to_string(),
            quantity: 1.0,
            committed_notional_usdc: 88_000.0,
            portfolio_weight: 0.5,
            entry_price: 90_000.0,
            mark_price: 88_000.0,
            protective_stop_px: 88_500.0,
            entry_r0: 1_500.0,
            protective_stop_oid: None,
            stop_distance_pct: None,
            take_profit_px: 95_000.0,
            take_profit_oid: None,
            tp_distance_pct: None,
            highest_price_seen: 90_000.0,
            lowest_price_seen: 88_000.0,
            unrealized_pnl_usdc: -2_000.0,
            unrealized_pnl_pct: -2.22,
            realized_pnl_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            fees_paid_usdc: 10.0,
            s1_weight_contrib: 0.0,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            latest_target_weight: 0.5,
            latest_delta_weight: 0.0,
            created_at_ms: 1_000,
            opened_at_ms: Some(1_000),
            updated_at_ms: 2_000,
        };
        let fills = vec![
            make_fill(
                "BTC",
                "B",
                90_000.0,
                1.0,
                10.0,
                0.0,
                "Open Long",
                1_000,
                1,
                1,
                Some(0.0),
            ),
            make_fill(
                "BTC",
                "A",
                88_200.0,
                0.4,
                8.0,
                -720.0,
                "Close Long",
                2_000,
                2,
                2,
                Some(1.0),
            ),
            make_fill(
                "BTC",
                "A",
                88_000.0,
                0.6,
                12.0,
                -1_200.0,
                "Close Long",
                2_500,
                3,
                3,
                Some(0.6),
            ),
        ];
        let (px, fee) = resolve_flat_exit_fill_stats(&held, Some(&fills));
        assert!((px - 88_080.0).abs() < 1e-6);
        assert!((fee - 20.0).abs() < 1e-6);
    }

    #[test]
    fn test_w3_single_booking_rule_suppresses_open_epoch_closed_orders_until_flat() {
        let mut fills_by_coin: BTreeMap<String, Vec<ParsedHlFill>> = BTreeMap::new();
        fills_by_coin.insert(
            "SOL".to_string(),
            vec![
                make_fill(
                    "SOL",
                    "B",
                    100.00,
                    3.48,
                    0.1218,
                    0.0,
                    "Open Long",
                    1_000,
                    7001,
                    1,
                    Some(0.0),
                ),
                make_fill(
                    "SOL",
                    "A",
                    102.50,
                    1.04,
                    0.0373,
                    2.60,
                    "Close Long",
                    2_000,
                    7002,
                    2,
                    Some(3.48),
                ),
                make_fill(
                    "SOL",
                    "A",
                    103.00,
                    0.72,
                    0.0260,
                    2.16,
                    "Close Long",
                    3_000,
                    7003,
                    3,
                    Some(2.44),
                ),
                make_fill(
                    "SOL",
                    "A",
                    101.80,
                    0.23,
                    0.0082,
                    0.414,
                    "Close Long",
                    4_000,
                    7004,
                    4,
                    Some(1.72),
                ),
            ],
        );
        let mut funding_by_coin: HashMap<String, Vec<ParsedHlFunding>> = HashMap::new();
        funding_by_coin.insert(
            "SOL".to_string(),
            vec![
                ParsedHlFunding {
                    coin: "SOL".to_string(),
                    time_ms: 1_500,
                    usdc: -0.1840,
                    szi: 3.48,
                    funding_rate: 0.0005,
                    dedup_key: "f1".to_string(),
                },
                ParsedHlFunding {
                    coin: "SOL".to_string(),
                    time_ms: 3_500,
                    usdc: 0.0620,
                    szi: 1.72,
                    funding_rate: -0.0003,
                    dedup_key: "f2".to_string(),
                },
            ],
        );

        // 1. While 1.49 SOL remains OPEN: 0 closed_orders emitted; open_epoch_realized_by_coin holds exact trims
        let open_szi: HashMap<String, f64> = [("SOL".to_string(), 1.49)].into_iter().collect();
        let out_open =
            compute_fifo_closed_orders_and_funding(&fills_by_coin, &funding_by_coin, &open_szi);
        assert!(
            out_open.closed_orders.is_empty(),
            "Active OPEN epoch must emit 0 closed_orders"
        );
        assert!(out_open.open_epoch_booked_funding_by_coin.is_empty());
        let summary = out_open.open_epoch_realized_by_coin.get("SOL").unwrap();
        assert!((summary.partial_closed_qty - 1.99).abs() < 1e-9);
        assert!((summary.gross_closed_pnl - 5.174).abs() < 1e-9);
        assert!((summary.close_fees_usdc - 0.0715).abs() < 1e-9);
        assert!((summary.open_fills_fees_usdc - 0.1218).abs() < 1e-9);
        assert!(summary.has_complete_open_fills);
        let rem_funding_sum: f64 = out_open
            .funding_events_with_remaining
            .iter()
            .map(|(_, rem)| *rem)
            .sum();
        assert!((rem_funding_sum - (-0.1220)).abs() < 1e-9);

        // 2. Final 1.49 SOL closes to 0.0: all 4 slices emit under SOL:1 with exact lifecycle conservation
        fills_by_coin.get_mut("SOL").unwrap().push(make_fill(
            "SOL",
            "A",
            104.20,
            1.49,
            0.0543,
            6.258,
            "Close Long",
            5_000,
            7005,
            5,
            Some(1.49),
        ));
        let out_flat = compute_fifo_closed_orders_and_funding(
            &fills_by_coin,
            &funding_by_coin,
            &HashMap::new(),
        );
        assert_eq!(out_flat.closed_orders.len(), 4);
        assert!(out_flat
            .closed_orders
            .iter()
            .all(|(_, a)| a.close_epoch_id == "SOL:1"));
        let net_sum: f64 = out_flat
            .closed_orders
            .iter()
            .map(|(_, a)| {
                a.gross_closed_pnl - a.open_fees_usdc - a.close_fees_usdc + a.funding_accrued_usdc
            })
            .sum();
        let expected_net =
            (2.60 + 2.16 + 0.414 + 6.258) - (0.1218 + 0.0373 + 0.0260 + 0.0082 + 0.0543) - 0.1220;
        assert!((net_sum - expected_net).abs() < 1e-9);
        assert!(out_flat
            .funding_events_with_remaining
            .iter()
            .all(|(_, rem)| rem.abs() < 1e-12));
    }
}
