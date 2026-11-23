/* ============================================================================
   AEGIS QUANT v2.0 — 100% DOM-NODE DATA TABLES, DIRTY-CHECKING & OUTAGE SAFETY
   ============================================================================ */

import {
  state,
  getEl,
  computeFingerprint,
  isSectionDirty,
  syncTableSortHeaders,
  sortItemsForTable,
  attachRowSortData,
} from '/js/state.js';
import {
  toFiniteNumber,
  fmtUsd,
  fmtSignedUsd,
  fmtSignedPct,
  pnlToneClass,
  fmtPrice,
  fmtPct,
  fmtLocalTime,
  fmtLocalShortTime,
  formatCooldownRemaining,
  computeOpenPnlPct,
  computeClosedEntryNotional,
  computeClosedPnlPct,
  computeVaultPnlPct,
  isPreCapHolding,
  getConfigFloatFromDash,
  h,
  populatePnlElement,
  createAddressChip,
  createTableSmall,
  createSleeveTag,
  createEmptyRow,
} from '/js/format.js';

export const VAULT_SORT_EXTRACTORS = {
  name: (v) => String(v?.name || v?.vault_address || ''),
  equity_usdc: (v) => toFiniteNumber(v?.equity_usdc),
  pnl_usdc: (v) => toFiniteNumber(v?.pnl_usdc),
  all_time_pnl_usdc: (v) => toFiniteNumber(v?.all_time_pnl_usdc),
  apr_pct: (v) => toFiniteNumber(v?.apr_pct),
  days_following: (v) => toFiniteNumber(v?.days_following),
  locked_until_ms: (v) => {
    if (!v) return null;
    if (v.is_locked) {
      const ts = toFiniteNumber(v.locked_until_ms);
      return ts !== null && ts > 0 ? ts : null;
    }
    return 0;
  },
};

const PRIORITY_ORDERED_SORT_EXTRACTORS = {
  symbol: (r) => r.symbol,
  state: (r) => r.state,
  side: (r) => r.side,
  price: (r) => r.price,
  mid_dist_bps: (r) => r.mid_dist_bps,
  reserved_usdc: (r) => r.reserved_usdc,
  chases: (r) => r.chases,
};

const HELD_SORT_EXTRACTORS = {
  symbol: (heldItem) => String(heldItem?.symbol ?? ''),
  side: (heldItem) => String(heldItem?.side ?? ''),
  committed_notional_usdc: (heldItem) => toFiniteNumber(heldItem?.committed_notional_usdc),
  entry_price: (heldItem) => toFiniteNumber(heldItem?.entry_price),
  protective_stop_px: (heldItem) => {
    const px = toFiniteNumber(heldItem?.protective_stop_px);
    return px !== null && px > 0 ? px : null;
  },
  take_profit_px: (heldItem) => {
    const px = toFiniteNumber(heldItem?.take_profit_px);
    return px !== null && px > 0 ? px : null;
  },
  unrealized_pnl_usdc: (heldItem) => toFiniteNumber(heldItem?.unrealized_pnl_usdc),
  sleeve_mix: (heldItem) =>
    Math.abs(toFiniteNumber(heldItem?.s1_weight_contrib) ?? 0) +
    Math.abs(toFiniteNumber(heldItem?.s2_weight_contrib) ?? 0) +
    Math.abs(toFiniteNumber(heldItem?.s3_weight_contrib) ?? 0),
};

const CLOSED_SORT_EXTRACTORS = {
  closed_at_ms: (p) => {
    const ts = toFiniteNumber(p?.closed_at_ms ?? p?.updated_at_ms);
    return ts !== null && ts > 0 ? ts : null;
  },
  symbol: (p) => String(p?._resolved_symbol ?? p?.symbol ?? ''),
  side: (p) => String(p?.side ?? ''),
  entry_notional_usdc: (p) => computeClosedEntryNotional(p),
  entry_price: (p) => {
    const px = toFiniteNumber(p?.entry_price);
    return px !== null && px > 0 ? px : null;
  },
  realized_pnl_usdc: (p) => toFiniteNumber(p?.realized_pnl_usdc),
  fees_funding_usdc: (p) => {
    const funding = toFiniteNumber(p?.funding_accrued_usdc) ?? 0;
    const fees = toFiniteNumber(p?.fees_paid_usdc) ?? 0;
    return funding - fees;
  },
  exit_reason: (p) => {
    const r = String(p?.exit_reason || '').trim();
    return r || null;
  },
};

const COOLDOWN_SORT_EXTRACTORS = {
  symbol: (c) => String(c?.symbol ?? ''),
  cooldown_type: (c) => String(c?.cooldown_type ?? ''),
  remaining_seconds: (c) => toFiniteNumber(c?.remaining_seconds),
  expires_at_ms: (c) => {
    const ts = toFiniteNumber(c?.expires_at_ms);
    return ts !== null && ts > 0 ? ts : null;
  },
  reason: (c) => {
    const r = String(c?.reason || '').trim();
    return r || null;
  },
};

const S1_SORT_EXTRACTORS = {
  symbol: (r) => String(r?.symbol ?? ''),
  mark_price: (r) => toFiniteNumber(r?.mark_price),
  day_volume_usdc: (r) => toFiniteNumber(r?.day_volume_usdc),
  funding_rate_hourly: (r) => toFiniteNumber(r?.funding_rate_hourly),
  funding_apr_pct: (r) => toFiniteNumber(r?.funding_apr_pct),
  annualized_vol: (r) => toFiniteNumber(r?.annualized_vol),
  market_beta: (r) => toFiniteNumber(r?.market_beta),
  s1_mom_score: (r) => toFiniteNumber(r?.s1_mom_score),
  s1_carry_score: (r) => toFiniteNumber(r?.s1_carry_score),
  w_s1_carry_mom: (r) => toFiniteNumber(r?.w_s1_carry_mom),
  target_weight: (r) => toFiniteNumber(r?.target_weight),
};

const S2_SORT_EXTRACTORS = {
  symbol: (r) => String(r?.symbol ?? ''),
  mark_price: (r) => toFiniteNumber(r?.mark_price),
  day_volume_usdc: (r) => toFiniteNumber(r?.day_volume_usdc),
  annualized_vol: (r) => toFiniteNumber(r?.annualized_vol),
  s2_trend_alignment: (r) => toFiniteNumber(r?.s2_trend_alignment),
  s2_vol_surge_ratio: (r) => toFiniteNumber(r?.s2_vol_surge_ratio),
  event_trigger: (r) => (r?.event_trigger ? String(r.event_trigger) : null),
  w_s2_trend_breakout: (r) => toFiniteNumber(r?.w_s2_trend_breakout),
  target_weight: (r) => toFiniteNumber(r?.target_weight),
  current_weight: (r) => toFiniteNumber(r?.current_weight),
};

const S3_SORT_EXTRACTORS = {
  symbol: (r) => String(r?.symbol ?? ''),
  mark_price: (r) => toFiniteNumber(r?.mark_price),
  day_volume_usdc: (r) => toFiniteNumber(r?.day_volume_usdc),
  funding_apr_pct: (r) => toFiniteNumber(r?.funding_apr_pct),
  s3_funding_zscore: (r) => toFiniteNumber(r?.s3_funding_zscore),
  s3_flush_atr_mult: (r) => toFiniteNumber(r?.s3_flush_atr_mult),
  event_trigger: (r) => (r?.event_trigger ? String(r.event_trigger) : null),
  w_s3_event_reversion: (r) => toFiniteNumber(r?.w_s3_event_reversion),
  target_weight: (r) => toFiniteNumber(r?.target_weight),
  current_weight: (r) => toFiniteNumber(r?.current_weight),
};

const SCANNER_SORT_EXTRACTORS = {
  symbol: (a) => String(a?.symbol ?? ''),
  coin: (a) => String(a?.coin ?? ''),
  mark_price: (a) => toFiniteNumber(a?.mark_price),
  mid_price: (a) => toFiniteNumber(a?.mid_price),
  oracle_price: (a) => toFiniteNumber(a?.oracle_price),
  day_volume_usdc: (a) => toFiniteNumber(a?.day_volume_usdc),
  open_interest_usdc: (a) => toFiniteNumber(a?.open_interest_usdc),
  funding_rate_hourly: (a) => toFiniteNumber(a?.funding_rate_hourly),
  max_leverage: (a) => toFiniteNumber(a?.max_leverage),
  eligibility_rank: (a) =>
    (a?.is_priority_watched ? 4 : 0) +
    (a?.is_core_eligible ? 2 : 0) +
    (a?.is_event_eligible ? 1 : 0),
};

const AUDIT_SORT_EXTRACTORS = {
  created_at_ms: (a) => {
    const ts = toFiniteNumber(a?.created_at_ms);
    return ts !== null && ts > 0 ? ts : null;
  },
  client_order_id: (a) => String(a?.client_order_id ?? ''),
  side: (a) => String(a?.side ?? ''),
  transition: (a) => `${a?.from_state ?? ''} -> ${a?.to_state ?? ''}`,
  limit_price: (a) => {
    const lp = toFiniteNumber(a?.limit_price);
    if (lp !== null && lp > 0) return lp;
    const fp = toFiniteNumber(a?.avg_fill_price);
    return fp !== null && fp > 0 ? fp : null;
  },
  committed_usdc: (a) => {
    const comm = toFiniteNumber(a?.committed_usdc);
    if (comm !== null && comm > 0) return comm;
    return toFiniteNumber(a?.reserved_usdc);
  },
  reason: (a) => {
    const r = String(a?.reason || '').trim();
    return r || null;
  },
};

function hasDegradedSection(dash, sectionName) {
  return Array.isArray(dash?.degraded_sections) && dash.degraded_sections.includes(sectionName);
}

function hasLiveDataRows(tbody) {
  if (!tbody) return false;
  return Array.from(tbody.rows).some((r) => r.dataset.emptyRow !== 'true');
}

export function renderVaultPanel(dash, force = false) {
  const tbody = getEl('tbody-vaults');
  if (!tbody) return;

  const vi = (dash && dash.vault_info) || {};
  const eb = (dash && dash.equity_basis) || {};
  const sortSpec = state.tableSort['table-vaults'];
  const fp = computeFingerprint([
    vi,
    eb.perp_wallet_equity_usdc,
    eb.perp_margin_equity_usdc,
    eb.is_unified_account,
    eb.spot_usdc,
    dash?.snapshot?.total_equity_usdc,
    sortSpec,
  ]);
  if (!force && !isSectionDirty('vaults', fp)) return;

  const perpEq =
    toFiniteNumber(eb.perp_wallet_equity_usdc) ??
    toFiniteNumber(dash?.snapshot?.total_equity_usdc) ??
    0;
  const perpMarginEq = toFiniteNumber(eb.perp_margin_equity_usdc) ?? 0;
  const isUnified = Boolean(eb.is_unified_account);
  const spotEq = toFiniteNumber(eb.spot_usdc) ?? 0;
  const totalVaultEq = toFiniteNumber(vi.total_vault_equity_usdc) ?? 0;
  const combinedEq =
    toFiniteNumber(vi.combined_portfolio_equity_usdc) ??
    (isUnified ? perpEq + totalVaultEq : perpEq + spotEq + totalVaultEq);
  const rawVaults = Array.isArray(vi.vaults) ? vi.vaults : [];
  const vaultCount = toFiniteNumber(vi.vault_count) ?? rawVaults.length;
  const configuredAddr = String(vi.configured_vault_address || '').trim();
  const syncedAtMs = toFiniteNumber(vi.synced_at_ms) ?? 0;

  const setTxt = (id, text) => {
    const el = getEl(id);
    if (el) el.textContent = text;
  };

  const cfgBadge = getEl('vault-configured-badge');
  if (cfgBadge) {
    if (configuredAddr) {
      cfgBadge.hidden = false;
      cfgBadge.replaceChildren(
        h('span', { text: 'Configured:' }),
        createAddressChip(configuredAddr)
      );
    } else {
      cfgBadge.hidden = true;
      cfgBadge.replaceChildren();
    }
  }

  const syncPill = getEl('vault-sync-pill');
  if (syncPill) {
    const countLabel = `${vaultCount} ${vaultCount === 1 ? 'VAULT' : 'VAULTS'}`;
    const shortSync = syncedAtMs > 0 ? fmtLocalShortTime(syncedAtMs) : '';
    syncPill.textContent = shortSync ? `${countLabel} · ${shortSync}` : countLabel;
    syncPill.title = syncedAtMs > 0 ? `Last synced: ${fmtLocalTime(syncedAtMs)}` : 'No vault sync recorded yet';
  }

  const closedCount = rawVaults.filter((v) => Boolean(v?.is_closed)).length;
  const activeCount = Math.max(0, rawVaults.length - closedCount);
  const lockedCount = rawVaults.filter((v) => Boolean(v?.is_locked)).length;

  setTxt('vault-total-equity', fmtUsd(totalVaultEq, 2));
  setTxt(
    'vault-count-sub',
    closedCount > 0
      ? `${activeCount} Active · ${closedCount} Closed`
      : `${vaultCount} ${vaultCount === 1 ? 'Active Vault' : 'Active Vaults'}`
  );
  setTxt(
    'vault-synced-sub',
    syncedAtMs > 0 ? `Synced ${fmtLocalTime(syncedAtMs)}` : 'Awaiting vault sync'
  );

  setTxt(
    'vault-combined-title',
    isUnified
      ? 'Combined Wallet Equity (Unified + Vault)'
      : 'Combined Wallet Equity (Perp + Spot + Vault)'
  );
  setTxt('vault-combined-equity', fmtUsd(combinedEq, 2));
  setTxt(
    'vault-combined-breakdown',
    isUnified
      ? perpMarginEq > 0.005
        ? `Unified ${fmtUsd(perpEq, 2)} (Perp Margin ${fmtUsd(perpMarginEq, 2)}) + Vault ${fmtUsd(totalVaultEq, 2)}`
        : `Unified ${fmtUsd(perpEq, 2)} + Vault ${fmtUsd(totalVaultEq, 2)}`
      : spotEq > 0.005
      ? `Perp ${fmtUsd(perpEq, 2)} + Spot ${fmtUsd(spotEq, 2)} + Vault ${fmtUsd(totalVaultEq, 2)}`
      : `Perp ${fmtUsd(perpEq, 2)} + Vault ${fmtUsd(totalVaultEq, 2)}`
  );

  const sumPnl = rawVaults.reduce((acc, v) => acc + (toFiniteNumber(v?.pnl_usdc) ?? 0), 0);
  const sumAllTime = rawVaults.reduce(
    (acc, v) => acc + (toFiniteNumber(v?.all_time_pnl_usdc) ?? 0),
    0
  );
  const aggBasis = totalVaultEq - sumPnl;
  const aggPnlPct = aggBasis > 1e-6 ? (sumPnl / aggBasis) * 100.0 : null;
  const aggAtBasis = totalVaultEq - sumAllTime;
  const aggAtPct = aggAtBasis > 1e-6 ? (sumAllTime / aggAtBasis) * 100.0 : null;

  let weightedApr = null;
  if (rawVaults.length > 0) {
    let weightSum = 0;
    let aprWeightedSum = 0;
    let aprSimpleSum = 0;
    let aprCount = 0;
    for (const v of rawVaults) {
      const apr = toFiniteNumber(v?.apr_pct);
      const eq = toFiniteNumber(v?.equity_usdc) ?? 0;
      if (apr !== null) {
        aprSimpleSum += apr;
        aprCount += 1;
        if (eq > 0) {
          weightSum += eq;
          aprWeightedSum += apr * eq;
        }
      }
    }
    if (weightSum > 0) {
      weightedApr = aprWeightedSum / weightSum;
    } else if (aprCount > 0) {
      weightedApr = aprSimpleSum / aprCount;
    }
  }

  const totalPnlEl = getEl('vault-total-pnl');
  if (totalPnlEl) {
    populatePnlElement(
      totalPnlEl,
      rawVaults.length > 0 ? sumPnl : 0,
      rawVaults.length > 0 ? aggPnlPct : null,
      'vault-kpi-value'
    );
  }

  const allTimeEl = getEl('vault-alltime-pnl');
  if (allTimeEl) {
    const atPctStr = rawVaults.length > 0 ? fmtSignedPct(aggAtPct, 2) : null;
    allTimeEl.textContent =
      rawVaults.length > 0
        ? `All-Time: ${fmtSignedUsd(sumAllTime, 2)}${atPctStr ? ` (${atPctStr})` : ''}`
        : 'All-Time: $0.00';
  }

  const avgAprStr = weightedApr !== null ? fmtSignedPct(weightedApr, 2) : null;
  setTxt('vault-avg-apr', avgAprStr ? `APR: ${avgAprStr}` : 'APR: —');
  setTxt(
    'vault-lock-summary',
    rawVaults.length === 0
      ? 'No active deposits'
      : lockedCount > 0
      ? `${lockedCount} Locked · ${rawVaults.length - lockedCount} Unlocked`
      : 'All Unlocked'
  );

  const sortedVaults = sortItemsForTable(rawVaults, 'table-vaults', VAULT_SORT_EXTRACTORS);
  tbody.dataset.truncated = 'false';

  if (sortedVaults.length === 0) {
    const tr = h('tr', { dataset: { emptyRow: 'true' } });
    const td = h('td', { colSpan: 7, className: 'table-empty-cell' });
    if (configuredAddr) {
      td.append(
        document.createTextNode('No active Hyperliquid vault deposits synced yet · Configured Vault: '),
        createAddressChip(configuredAddr)
      );
      if (syncedAtMs > 0) {
        td.append(document.createTextNode(` · Last checked ${fmtLocalTime(syncedAtMs)}`));
      }
    } else {
      td.textContent =
        'No active Hyperliquid vault deposits synced yet — vault funds remain segregated from perp margin.';
    }
    tr.appendChild(td);
    tbody.replaceChildren(tr);
    return;
  }

  const rows = sortedVaults.map((v) => {
    const nameSpan = h('span', {
      className: 'sym-badge',
      text: String(v?.name || 'Hyperliquid Vault'),
      title: v?.leader ? `Leader: ${String(v.leader)}` : '',
    });
    const nameWrap = h('div', { className: 'vault-name-cell' }, nameSpan);
    if (v?.is_closed) {
      nameWrap.appendChild(h('span', { className: 'vault-closed-badge', text: 'CLOSED' }));
    }
    if (v?.vault_address) {
      nameWrap.appendChild(createAddressChip(v.vault_address));
    }
    nameWrap.appendChild(
      h('button', {
        type: 'button',
        className: 'vault-row-inspect-btn',
        text: 'Facts',
        dataset: {
          action: 'inspect-vault',
          vaultAddress: String(v?.vault_address || ''),
        },
        attrs: {
          'aria-label': `Inspect live facts for ${String(v?.name || v?.vault_address || 'Hyperliquid Vault')}`,
        },
      })
    );

    const tdName = h('td', null, nameWrap);
    const tdEq = h('td', {
      className: 'num-cell cell-committed',
      text: fmtUsd(v?.equity_usdc ?? 0, 2),
    });

    const tdPnl = document.createElement('td');
    populatePnlElement(tdPnl, v?.pnl_usdc, computeVaultPnlPct(v, 'pnl_pct', 'pnl_usdc'));

    const tdAtPnl = document.createElement('td');
    populatePnlElement(
      tdAtPnl,
      v?.all_time_pnl_usdc,
      computeVaultPnlPct(v, 'all_time_pnl_pct', 'all_time_pnl_usdc')
    );

    const aprVal = toFiniteNumber(v?.apr_pct);
    const aprStr = fmtSignedPct(aprVal, 2);
    const tdApr = h('td', {
      className: `num-cell ${pnlToneClass(aprVal, 0.005)}`,
      text: aprStr ?? '—',
    });

    const daysVal = toFiniteNumber(v?.days_following);
    const tdDays = h('td', {
      className: 'num-cell',
      text: daysVal !== null ? `${Math.max(0, Math.floor(daysVal))}d` : '—',
    });

    const lockedUntil = toFiniteNumber(v?.locked_until_ms) ?? 0;
    const pill = v?.is_locked
      ? h('span', {
          className: 'status-pill working',
          text: lockedUntil > 0 ? `Locked · ${fmtLocalTime(lockedUntil)}` : 'Locked',
        })
      : h('span', {
          className: 'status-pill open',
          text: 'Unlocked',
          title: lockedUntil > 0 ? `Unlocked since ${fmtLocalTime(lockedUntil)}` : '',
        });
    const tdLock = h('td', null, pill);

    return h('tr', null, tdName, tdEq, tdPnl, tdAtPnl, tdApr, tdDays, tdLock);
  });

  tbody.replaceChildren(...rows);
  attachRowSortData(tbody, sortedVaults, VAULT_SORT_EXTRACTORS);
}

export function renderPriorityAndWorkingTable(dash, forceRender = false) {
  const tbody = getEl('tbody-priority-ordered');
  const toggleBtn = getEl('btn-toggle-watch-all');
  if (!tbody) return;
  if (!forceRender && state.closeInFlightCount > 0) return;

  // R14-M22: On DB outage without v_assets_ordered array, keep dimmed last-known rows or show error banner
  if (dash?.database_reachable === false && !Array.isArray(dash?.v_assets_ordered)) {
    if (hasLiveDataRows(tbody)) {
      tbody.classList.add('tbody-stale-outage');
    } else {
      tbody.classList.remove('tbody-stale-outage');
      tbody.replaceChildren(
        createEmptyRow(
          8,
          '⚠ DATABASE UNAVAILABLE — WORKING ORDERS UNKNOWN (DO NOT ASSUME EMPTY)',
          'table-error-cell'
        )
      );
    }
    return;
  }

  if (hasDegradedSection(dash, 'v_assets_ordered')) {
    tbody.classList.remove('tbody-stale-outage');
    tbody.replaceChildren(
      createEmptyRow(
        8,
        '⚠ v_assets_ordered query unavailable — working order state degraded',
        'table-error-cell'
      )
    );
    return;
  }

  tbody.classList.remove('tbody-stale-outage');

  const sortSpec = state.tableSort['table-priority-ordered'];
  const fp = computeFingerprint([
    dash?.v_assets_ordered,
    dash?.v_priority_watchlist,
    dash?.v_assets_held?.map((x) => x.asset_id),
    state.showAllPriorityWatch,
    Array.from(state.closingPositionIds),
    Array.from(state.closingSymbols),
    sortSpec,
  ]);
  if (!forceRender && !isSectionDirty('priority', fp)) return;

  const held = (dash && dash.v_assets_held) || [];
  const heldAssetIds = new Set(held.map((heldItem) => Number(heldItem.asset_id)));

  const ordered = ((dash && dash.v_assets_ordered) || []).filter(
    (o) =>
      !state.closingPositionIds.has(Number(o.position_id)) &&
      !state.closingSymbols.has(String(o.symbol))
  );
  const prio = (dash && dash.v_priority_watchlist) || [];

  const orderedByAsset = new Map();
  ordered.forEach((o) => orderedByAsset.set(Number(o.asset_id), o));

  const passiveWatched = prio.filter((p) => {
    const aid = Number(p.asset_id);
    const st = String(p.position_status || '').toUpperCase();
    const pid = Number(p.position_id || 0);
    return (
      !orderedByAsset.has(aid) &&
      !heldAssetIds.has(aid) &&
      st !== 'OPEN' &&
      st !== 'WORKING' &&
      !state.closingPositionIds.has(pid) &&
      !state.closingSymbols.has(String(p.symbol))
    );
  });
  const heldInWatch = prio.filter((p) => {
    const aid = Number(p.asset_id);
    const st = String(p.position_status || '').toUpperCase();
    return heldAssetIds.has(aid) || st === 'OPEN';
  }).length;
  const splitNoteEl = getEl('watch-split-note');
  if (splitNoteEl) {
    if (heldInWatch > 0) {
      splitNoteEl.textContent = `${passiveWatched.length} Watching · ${heldInWatch} Held`;
      splitNoteEl.hidden = false;
    } else {
      splitNoteEl.textContent = '';
      splitNoteEl.hidden = true;
    }
  }

  const maxPassiveDefault = 6;

  if (toggleBtn) {
    const showBtn = passiveWatched.length > maxPassiveDefault;
    toggleBtn.hidden = !showBtn;
    if (showBtn) {
      toggleBtn.textContent = state.showAllPriorityWatch
        ? `Show Top ${maxPassiveDefault}`
        : `Show All Watchlist (${ordered.length + passiveWatched.length})`;
    }
  }

  const unifiedAll = [
    ...ordered.map((o) => ({
      kind: 'working',
      item: o,
      symbol: String(o.symbol ?? ''),
      state: String(o.fsm_state || 'WORKING_ALO'),
      side: String(o.side ?? ''),
      price: toFiniteNumber(o.limit_price) ?? toFiniteNumber(o.mark_price),
      mid_dist_bps: toFiniteNumber(o.distance_from_mid_bps),
      reserved_usdc: toFiniteNumber(o.reserved_margin_usdc),
      chases: toFiniteNumber(o.chase_count) ?? 0,
    })),
    ...passiveWatched.map((p) => ({
      kind: 'watch',
      item: p,
      symbol: String(p.symbol ?? ''),
      state: String(p.event_trigger || 'PRIORITY_WATCH'),
      side: null,
      price: toFiniteNumber(p.mark_price),
      mid_dist_bps: 0,
      reserved_usdc: 0,
      chases: null,
    })),
  ];

  const isSorted = Boolean(sortSpec);
  let visibleEntries;
  let truncated = false;

  if (isSorted) {
    const sortedAll = sortItemsForTable(
      unifiedAll,
      'table-priority-ordered',
      PRIORITY_ORDERED_SORT_EXTRACTORS
    );
    const cap = state.showAllPriorityWatch
      ? sortedAll.length
      : ordered.length + maxPassiveDefault;
    visibleEntries = sortedAll.slice(0, Math.max(ordered.length, cap));
    truncated = sortedAll.length > visibleEntries.length;
  } else {
    syncTableSortHeaders('table-priority-ordered');
    const visiblePassiveCount = state.showAllPriorityWatch
      ? passiveWatched.length
      : Math.min(passiveWatched.length, maxPassiveDefault);
    visibleEntries = [
      ...unifiedAll.slice(0, ordered.length),
      ...unifiedAll.slice(ordered.length, ordered.length + visiblePassiveCount),
    ];
    truncated = passiveWatched.length > visiblePassiveCount;
  }

  tbody.dataset.truncated = truncated ? 'true' : 'false';

  if (visibleEntries.length === 0) {
    tbody.replaceChildren(
      createEmptyRow(
        8,
        'No active WORKING orders or priority triggers — monitoring 1s watchlist stream...'
      )
    );
    return;
  }

  const rows = visibleEntries.map((entry) => {
    if (entry.kind === 'working') {
      const o = entry.item;
      const sideCls = String(o.side).toUpperCase() === 'SHORT' ? 'short' : 'long';
      const hasTpsl = Number(o.protective_stop_px || 0) > 0 || Number(o.take_profit_px || 0) > 0;
      const tdPrice = h(
        'td',
        { className: 'num-cell' },
        `${fmtPrice(o.limit_price)} `,
        createTableSmall('cell-muted-sub', `/ ${fmtPrice(o.mark_price)}`)
      );
      if (hasTpsl) {
        tdPrice.append(
          document.createElement('br'),
          createTableSmall(
            'cell-muted-sub',
            `SL ${fmtPrice(o.protective_stop_px)} · TP ${fmtPrice(o.take_profit_px)}`
          )
        );
      }

      return h(
        'tr',
        null,
        h('td', null, h('span', { className: 'sym-badge', text: String(o.symbol ?? '') })),
        h(
          'td',
          null,
          h('span', {
            className: 'status-pill working',
            text: String(o.fsm_state || 'WORKING_ALO'),
          })
        ),
        h(
          'td',
          null,
          h('span', { className: `side-pill ${sideCls}`, text: String(o.side ?? '') })
        ),
        tdPrice,
        h('td', {
          className: 'num-cell text-cyan-bold',
          text: `${Number(o.distance_from_mid_bps || 0).toFixed(2)} bps`,
        }),
        h('td', {
          className: 'num-cell text-amber-bold',
          text: fmtUsd(o.reserved_margin_usdc),
        }),
        h(
          'td',
          { className: 'num-cell' },
          h('span', { className: 'tab-count', text: String(Number(o.chase_count || 0)) })
        ),
        h(
          'td',
          null,
          h('button', {
            type: 'button',
            className: 'close-btn',
            text: 'Cancel',
            dataset: {
              action: 'close-position',
              positionId: String(Number(o.position_id)),
              symbol: String(o.symbol ?? ''),
              cooldown: 'false',
            },
          })
        )
      );
    }

    const p = entry.item;
    const statusLabel = String(p.event_trigger || 'PRIORITY_WATCH');
    return h(
      'tr',
      null,
      h('td', null, h('span', { className: 'sym-badge', text: String(p.symbol ?? '') })),
      h('td', null, h('span', { className: 'status-pill watch', text: statusLabel })),
      h('td', null, h('span', { className: 'pnl-flat', text: '—' })),
      h(
        'td',
        { className: 'num-cell' },
        `${fmtPrice(p.mark_price)} `,
        createTableSmall('cell-muted-sub', `/ ${fmtPrice(p.mid_price || p.mark_price)}`)
      ),
      h('td', { className: 'num-cell', text: '0.00 bps' }),
      h('td', { className: 'num-cell', text: '$0.00' }),
      h('td', null, h('span', { className: 'tab-count', text: '1s' })),
      h('td', null, h('span', { className: 'pnl-flat', text: '1s Watch' }))
    );
  });

  tbody.replaceChildren(...rows);
  attachRowSortData(tbody, visibleEntries, PRIORITY_ORDERED_SORT_EXTRACTORS);
}

export function renderOpenHoldingsTable(dash, forceRender = false) {
  const tbody = getEl('tbody-assets-held');
  const pill = getEl('held-count-pill');
  const closeAllBtn = getEl('btn-close-all-open');
  if (!tbody) return;
  if (!forceRender && state.closeInFlightCount > 0) return;

  // R14-M22: Never show misleading "No active OPEN holdings" when DB is unreachable or v_assets_held degraded
  if (dash?.database_reachable === false && !Array.isArray(dash?.v_assets_held)) {
    if (hasLiveDataRows(tbody)) {
      tbody.classList.add('tbody-stale-outage');
    } else {
      tbody.classList.remove('tbody-stale-outage');
      tbody.replaceChildren(
        createEmptyRow(
          9,
          '⚠ DATABASE UNAVAILABLE — LIVE POSITIONS UNKNOWN (DO NOT ASSUME FLAT)',
          'table-error-cell'
        )
      );
    }
    return;
  }

  if (hasDegradedSection(dash, 'v_assets_held')) {
    tbody.classList.remove('tbody-stale-outage');
    tbody.replaceChildren(
      createEmptyRow(
        9,
        '⚠ v_assets_held query unavailable — live open positions unknown (DO NOT ASSUME FLAT)',
        'table-error-cell'
      )
    );
    if (closeAllBtn) closeAllBtn.hidden = true;
    return;
  }

  tbody.classList.remove('tbody-stale-outage');

  const sortSpec = state.tableSort['table-assets-held'];
  const pmHealth = dash?.priority_monitor_health || {};
  const fp = computeFingerprint([
    dash?.v_assets_held,
    dash?.risk_capital_cap_updated_at_ms,
    dash?.max_open_positions,
    dash?.active_cap_open_count,
    dash?.pre_cap_open_count,
    dash?.execution_mode,
    pmHealth.failed_symbols,
    pmHealth.unfilled_stop_breach_symbols,
    Array.from(state.closingPositionIds),
    Array.from(state.closingSymbols),
    sortSpec,
  ]);
  if (!forceRender && !isSectionDirty('held', fp)) return;

  const rawHeld = (dash && dash.v_assets_held) || [];
  const heldFiltered = rawHeld.filter(
    (heldItem) =>
      !state.closingPositionIds.has(Number(heldItem.position_id)) &&
      !state.closingSymbols.has(String(heldItem.symbol))
  );
  const held = sortItemsForTable(heldFiltered, 'table-assets-held', HELD_SORT_EXTRACTORS);
  tbody.dataset.truncated = 'false';

  const capEpochMs = Number(dash?.risk_capital_cap_updated_at_ms ?? 0);
  const maxOpenPositions = Math.max(
    1,
    Number(dash?.max_open_positions ?? getConfigFloatFromDash(dash, 'max_open_positions', 6))
  );
  const hasServerCounts =
    typeof dash?.active_cap_open_count === 'number' &&
    typeof dash?.pre_cap_open_count === 'number' &&
    held.length === rawHeld.length;
  const preCapCount = hasServerCounts
    ? dash.pre_cap_open_count
    : held.filter((heldItem) => isPreCapHolding(heldItem, capEpochMs)).length;
  const activeCapCount = hasServerCounts
    ? dash.active_cap_open_count
    : held.length - preCapCount;
  const totalHeldCount = activeCapCount + preCapCount;

  if (pill) {
    pill.textContent =
      preCapCount > 0
        ? `${activeCapCount}/${maxOpenPositions} Cap · ${preCapCount} Pre-Cap (${totalHeldCount} Total)`
        : `${activeCapCount}/${maxOpenPositions} Cap`;
    pill.classList.toggle('cap-exceeded', activeCapCount > maxOpenPositions);
  }
  if (closeAllBtn) {
    closeAllBtn.hidden = held.length === 0;
  }

  if (held.length === 0) {
    tbody.replaceChildren(
      createEmptyRow(
        9,
        'No active OPEN holdings — capital remains in Available Cash until exchange fill confirmation.'
      )
    );
    return;
  }

  const breachSet = new Set(
    (pmHealth.unfilled_stop_breach_symbols || []).map((s) => String(s).toUpperCase())
  );
  const failedSet = new Set(
    (pmHealth.failed_symbols || []).map((s) => String(s).toUpperCase())
  );
  const execMode = String(dash?.execution_mode || '').toUpperCase();
  const isLiveOrTestnet = execMode === 'LIVE' || execMode === 'TESTNET';
  const nowTsMs = toFiniteNumber(dash?.timestamp_ms) ?? Date.now();

  const rows = held.map((heldItem) => {
    const isPreCap = isPreCapHolding(heldItem, capEpochMs);
    const sideStr = String(heldItem.side ?? '');
    const sideCls = sideStr.toUpperCase() === 'SHORT' ? 'short' : 'long';
    const isShort = sideCls === 'short';
    const upnl = toFiniteNumber(heldItem.unrealized_pnl_usdc) ?? 0;
    const upnlPct = computeOpenPnlPct(heldItem);
    const stopPx = toFiniteNumber(heldItem.protective_stop_px);
    const markPx = toFiniteNumber(heldItem.mark_price);
    let stopDistDisplay = '—';
    let isStopBreached = false;
    if (stopPx !== null && stopPx > 0 && markPx !== null && markPx > 0) {
      const signedDist = isShort
        ? ((stopPx - markPx) / markPx) * 100.0
        : ((markPx - stopPx) / markPx) * 100.0;
      if (signedDist < -0.005) {
        isStopBreached = true;
        stopDistDisplay = `BREACHED (${signedDist.toFixed(2)}%)`;
      } else {
        stopDistDisplay = `(-${Math.abs(signedDist).toFixed(2)}%)`;
      }
    } else if (heldItem.stop_distance_pct !== null && heldItem.stop_distance_pct !== undefined) {
      stopDistDisplay = `(${Number(heldItem.stop_distance_pct).toFixed(2)}%)`;
    }
    const tpNum = toFiniteNumber(heldItem.tp_distance_pct);
    const tpDist =
      tpNum !== null
        ? `(${tpNum >= 0 ? '+' : ''}${tpNum.toFixed(2)}%)`
        : '(—)';
    const slOid = heldItem.protective_stop_oid ? String(heldItem.protective_stop_oid).trim() : '';
    const tpOid = heldItem.take_profit_oid ? String(heldItem.take_profit_oid).trim() : '';

    const symUpper = String(heldItem.symbol ?? '').toUpperCase();
    const coinUpper = String(heldItem.coin ?? '').toUpperCase();
    const hasBreachAlert = breachSet.has(symUpper) || breachSet.has(coinUpper);
    const hasMonitorErr = failedSet.has(symUpper) || failedSet.has(coinUpper);
    const originTs = toFiniteNumber(heldItem.opened_at_ms ?? heldItem.created_at_ms) ?? 0;
    const missingSlOidStale =
      isLiveOrTestnet && !slOid && originTs > 0 && nowTsMs - originTs > 15_000;
    const rowAlert = hasBreachAlert || hasMonitorErr || missingSlOidStale;

    const tr = document.createElement('tr');
    const rowClasses = [];
    if (isPreCap) rowClasses.push('holding-pre-cap');
    if (rowAlert) rowClasses.push('row-surveillance-alert');
    if (rowClasses.length > 0) tr.className = rowClasses.join(' ');

    const symWrap = h(
      'div',
      { className: 'holding-symbol-cell' },
      h('span', { className: 'sym-badge', text: String(heldItem.symbol ?? '') })
    );
    if (isPreCap) {
      symWrap.appendChild(h('span', { className: 'pre-cap-badge', text: 'PRE-CAP' }));
    }
    if (hasBreachAlert) {
      symWrap.appendChild(
        h('span', {
          className: 'status-pill surveillance-pill-critical',
          text: 'BREACH RESTING',
        })
      );
    } else if (hasMonitorErr) {
      symWrap.appendChild(
        h('span', {
          className: 'status-pill surveillance-pill-critical',
          text: 'MONITOR ERR',
        })
      );
    } else if (missingSlOidStale) {
      symWrap.appendChild(
        h('span', {
          className: 'status-pill working',
          text: 'NO SL OID',
        })
      );
    }

    const tdSym = h('td', null, symWrap);
    const tdSide = h(
      'td',
      null,
      h('span', { className: `side-pill ${sideCls}`, text: sideStr })
    );

    const tdCommitted = h(
      'td',
      { className: 'num-cell cell-committed' },
      `${fmtUsd(heldItem.committed_notional_usdc)} `,
      createTableSmall('cell-muted-sub', `(${fmtPct(heldItem.portfolio_weight || 0)})`)
    );

    const tdEntryMark = h(
      'td',
      { className: 'num-cell' },
      `${fmtPrice(heldItem.entry_price)} `,
      createTableSmall('cell-mark-price', `→ ${fmtPrice(heldItem.mark_price)}`)
    );

    const slBadge = createSleeveTag(
      slOid ? 's1' : 's3',
      slOid ? `Exchange SL Order: ${slOid}` : 'SL Order Pending',
      slOid ? 'SL' : 'SL?'
    );
    const tdSl = h(
      'td',
      { className: 'num-cell' },
      `${fmtPrice(heldItem.protective_stop_px)} `,
      createTableSmall(isStopBreached ? 'cell-sl-breached' : 'cell-sl-dist', stopDistDisplay),
      ' ',
      slBadge
    );

    const tpPxText = Number(heldItem.take_profit_px || 0) > 0 ? fmtPrice(heldItem.take_profit_px) : '—';
    const tpBadge = createSleeveTag(
      tpOid ? 's2' : 's3',
      tpOid ? `Exchange TP Order: ${tpOid}` : 'TP Order Pending',
      tpOid ? 'TP' : 'TP?'
    );
    const tdTp = h(
      'td',
      { className: 'num-cell' },
      `${tpPxText} `,
      createTableSmall('cell-tp-dist', tpDist),
      ' ',
      tpBadge
    );

    const tdPnl = document.createElement('td');
    populatePnlElement(tdPnl, upnl, upnlPct);

    const tdSleeves = h(
      'td',
      null,
      h(
        'div',
        { className: 'sleeve-mix-pills' },
        createSleeveTag('s1', 'Sleeve 1 Carry-Momentum', `S1 ${fmtPct(heldItem.s1_weight_contrib || 0)}`),
        createSleeveTag('s2', 'Sleeve 2 Trend-Breakout', `S2 ${fmtPct(heldItem.s2_weight_contrib || 0)}`),
        createSleeveTag('s3', 'Sleeve 3 Event-Reversion', `S3 ${fmtPct(heldItem.s3_weight_contrib || 0)}`)
      )
    );

    const tdAction = h(
      'td',
      null,
      h('button', {
        type: 'button',
        className: 'close-btn',
        text: 'Close',
        dataset: {
          action: 'close-position',
          positionId: String(Number(heldItem.position_id)),
          symbol: String(heldItem.symbol ?? ''),
          cooldown: 'true',
        },
      })
    );

    tr.append(tdSym, tdSide, tdCommitted, tdEntryMark, tdSl, tdTp, tdPnl, tdSleeves, tdAction);
    return tr;
  });

  tbody.replaceChildren(...rows);
  attachRowSortData(tbody, held, HELD_SORT_EXTRACTORS);
}

export function renderClosedPositionsTable(dash, force = false) {
  const tbody = getEl('tbody-closed-positions');
  const pill = getEl('closed-count-pill');
  if (!tbody) return;

  const query = (getEl('closed-search-input')?.value || '').trim().toUpperCase();
  const sortSpec = state.tableSort['table-closed-positions'];
  const rawClosed =
    (dash && (dash.recent_closed_positions || dash.closed_positions)) || [];
  const fp = computeFingerprint([rawClosed, query, sortSpec]);
  if (!force && !isSectionDirty('closed', fp)) return;

  if (hasDegradedSection(dash, 'recent_closed_positions') && rawClosed.length === 0) {
    tbody.replaceChildren(
      createEmptyRow(8, '⚠ Closed trades section temporarily unavailable', 'table-error-cell')
    );
    return;
  }

  const assetById = new Map();
  ((dash && dash.exchange_assets) || []).forEach((a) => {
    if (a && a.id != null) assetById.set(Number(a.id), String(a.symbol || a.coin || ''));
  });

  const enriched = rawClosed.map((p) => {
    const resolvedSym =
      String(p?.symbol || '').trim() ||
      assetById.get(Number(p?.asset_id)) ||
      (p?.coin ? `${p.coin}-USDC` : '—');
    return { ...p, _resolved_symbol: resolvedSym };
  });

  const filtered = query
    ? enriched.filter((p) => {
        const sym = String(p._resolved_symbol || '').toUpperCase();
        const coin = String(p.coin || '').toUpperCase();
        const reason = String(p.exit_reason || '').toUpperCase();
        const side = String(p.side || '').toUpperCase();
        return (
          sym.includes(query) ||
          coin.includes(query) ||
          reason.includes(query) ||
          side.includes(query)
        );
      })
    : enriched;

  if (pill) {
    pill.textContent =
      query && filtered.length !== enriched.length
        ? `${filtered.length}/${enriched.length} CLOSED`
        : `${enriched.length} CLOSED`;
  }

  const sorted = sortItemsForTable(filtered, 'table-closed-positions', CLOSED_SORT_EXTRACTORS);
  tbody.dataset.truncated = 'false';

  if (sorted.length === 0) {
    tbody.replaceChildren(
      createEmptyRow(
        8,
        query ? 'No closed trades matching search filter.' : 'No closed trades recorded yet.'
      )
    );
    return;
  }

  const rows = sorted.map((p) => {
    const sideStr = String(p.side || 'LONG');
    const notional = computeClosedEntryNotional(p);
    const entryPx = toFiniteNumber(p.entry_price);
    const exitPx = toFiniteNumber(p.exit_price ?? p.current_price);
    const tdRpnl = document.createElement('td');
    populatePnlElement(tdRpnl, p.realized_pnl_usdc, computeClosedPnlPct(p));
    const netFeesFunding =
      (toFiniteNumber(p.funding_accrued_usdc) ?? 0) - (toFiniteNumber(p.fees_paid_usdc) ?? 0);
    const reasonText = String(p.exit_reason || '').trim() || '—';

    return h(
      'tr',
      null,
      h('td', {
        className: 'num-cell',
        text: fmtLocalTime(p.closed_at_ms ?? p.updated_at_ms),
      }),
      h('td', null, h('span', { className: 'sym-badge', text: String(p._resolved_symbol || '—') })),
      h(
        'td',
        null,
        h('span', {
          className: `side-pill ${sideStr.toUpperCase() === 'SHORT' ? 'short' : 'long'}`,
          text: sideStr,
        })
      ),
      h('td', {
        className: 'num-cell cell-committed',
        text: notional !== null && notional > 0 ? fmtUsd(notional, 2) : '—',
      }),
      h(
        'td',
        { className: 'num-cell' },
        `${entryPx !== null && entryPx > 0 ? fmtPrice(entryPx) : '—'} `,
        createTableSmall(
          'cell-mark-price',
          `→ ${exitPx !== null && exitPx > 0 ? fmtPrice(exitPx) : '—'}`
        )
      ),
      tdRpnl,
      h('td', {
        className: `num-cell ${pnlToneClass(netFeesFunding, 0.001)}`,
        text: fmtSignedUsd(netFeesFunding, 2),
      }),
      h('td', { className: 'reason-cell', title: reasonText, text: reasonText })
    );
  });

  tbody.replaceChildren(...rows);
  attachRowSortData(tbody, sorted, CLOSED_SORT_EXTRACTORS);
}

export function renderCooldownsPanel(dash, force = false) {
  const tbody = getEl('tbody-cooldowns');
  const pillEl = getEl('cooldown-count-pill');
  if (!tbody) return;

  const rawCooldowns = (dash && dash.v_assets_in_cooldown) || [];
  const sortSpec = state.tableSort['table-cooldowns'];
  const fp = computeFingerprint([rawCooldowns, sortSpec]);
  if (!force && !isSectionDirty('cooldowns', fp)) return;

  if (pillEl) {
    pillEl.textContent = `${rawCooldowns.length} ACTIVE`;
  }

  const cooldowns = sortItemsForTable(rawCooldowns, 'table-cooldowns', COOLDOWN_SORT_EXTRACTORS);
  tbody.dataset.truncated = 'false';

  if (cooldowns.length === 0) {
    tbody.replaceChildren(createEmptyRow(5, 'No assets currently in cooldown.'));
    return;
  }

  const rows = cooldowns.map((c) => {
    const reasonStr = String(c.reason || '').trim() || '—';
    return h(
      'tr',
      null,
      h('td', null, h('span', { className: 'sym-badge', text: String(c.symbol ?? '') })),
      h('td', null, h('span', { className: 'sp-tag s3', text: String(c.cooldown_type || 'COOLDOWN') })),
      h('td', {
        className: 'num-cell cell-sl-dist',
        text: `${formatCooldownRemaining(c.remaining_seconds)} left`,
      }),
      h('td', { className: 'num-cell', text: fmtLocalTime(c.expires_at_ms) }),
      h('td', { className: 'reason-cell', title: reasonStr, text: reasonStr })
    );
  });

  tbody.replaceChildren(...rows);
  attachRowSortData(tbody, cooldowns, COOLDOWN_SORT_EXTRACTORS);
}

function renderSleeveS1Table(s1, force) {
  const tbS1 = getEl('tbody-sleeve-s1');
  if (!tbS1) return;
  const sortSpec = state.tableSort['table-sleeve-s1'];
  const fp = computeFingerprint([s1, sortSpec]);
  if (!force && !isSectionDirty('tab-s1', fp)) return;

  const sorted = sortItemsForTable(s1, 'table-sleeve-s1', S1_SORT_EXTRACTORS);
  const sliced = sorted.slice(0, 60);
  tbS1.dataset.truncated = sorted.length > sliced.length ? 'true' : 'false';

  if (sliced.length === 0) {
    tbS1.replaceChildren(createEmptyRow(11, 'No Sleeve 1 assets matching filter.'));
    return;
  }

  const rows = sliced.map((r) => {
    const wCls =
      r.w_s1_carry_mom > 0.001
        ? 'pnl-pos'
        : r.w_s1_carry_mom < -0.001
        ? 'pnl-neg'
        : 'pnl-flat';
    return h(
      'tr',
      null,
      h('td', null, h('span', { className: 'sym-badge', text: String(r.symbol ?? '') })),
      h('td', { className: 'num-cell', text: fmtPrice(r.mark_price) }),
      h('td', { className: 'num-cell', text: fmtUsd(r.day_volume_usdc, 0) }),
      h('td', {
        className: 'num-cell',
        text: `${(Number(r.funding_rate_hourly || 0) * 100).toFixed(4)}%`,
      }),
      h('td', {
        className: `num-cell ${r.funding_apr_pct >= 0 ? 'pnl-pos' : 'pnl-neg'}`,
        text: `${Number(r.funding_apr_pct || 0).toFixed(2)}%`,
      }),
      h('td', {
        className: 'num-cell',
        text: `${(Number(r.annualized_vol || 0) * 100).toFixed(1)}%`,
      }),
      h('td', { className: 'num-cell', text: Number(r.market_beta ?? 1).toFixed(2) }),
      h('td', { className: 'num-cell', text: Number(r.s1_mom_score || 0).toFixed(3) }),
      h('td', { className: 'num-cell', text: Number(r.s1_carry_score || 0).toFixed(3) }),
      h('td', { className: `num-cell ${wCls}`, text: fmtPct(r.w_s1_carry_mom) }),
      h('td', { className: 'num-cell text-strong-white', text: fmtPct(r.target_weight) })
    );
  });

  tbS1.replaceChildren(...rows);
  attachRowSortData(tbS1, sliced, S1_SORT_EXTRACTORS);
}

function renderSleeveS2Table(s2, force) {
  const tbS2 = getEl('tbody-sleeve-s2');
  if (!tbS2) return;
  const sortSpec = state.tableSort['table-sleeve-s2'];
  const fp = computeFingerprint([s2, sortSpec]);
  if (!force && !isSectionDirty('tab-s2', fp)) return;

  const sorted = sortItemsForTable(s2, 'table-sleeve-s2', S2_SORT_EXTRACTORS);
  const sliced = sorted.slice(0, 60);
  tbS2.dataset.truncated = sorted.length > sliced.length ? 'true' : 'false';

  if (sliced.length === 0) {
    tbS2.replaceChildren(createEmptyRow(10, 'No Sleeve 2 assets matching filter.'));
    return;
  }

  const rows = sliced.map((r) => {
    const align = Number(r.s2_trend_alignment || 0);
    const alignEl =
      align > 0
        ? h('span', { className: 'side-pill long', text: 'BULL (+1)' })
        : align < 0
        ? h('span', { className: 'side-pill short', text: 'BEAR (-1)' })
        : h('span', { className: 'pnl-flat', text: 'NEUTRAL (0)' });
    const wCls =
      r.w_s2_trend_breakout > 0.001
        ? 'pnl-pos'
        : r.w_s2_trend_breakout < -0.001
        ? 'pnl-neg'
        : 'pnl-flat';
    return h(
      'tr',
      null,
      h('td', null, h('span', { className: 'sym-badge', text: String(r.symbol ?? '') })),
      h('td', { className: 'num-cell', text: fmtPrice(r.mark_price) }),
      h('td', { className: 'num-cell', text: fmtUsd(r.day_volume_usdc, 0) }),
      h('td', {
        className: 'num-cell',
        text: `${(Number(r.annualized_vol || 0) * 100).toFixed(1)}%`,
      }),
      h('td', null, alignEl),
      h('td', {
        className: 'num-cell text-cyan-bold',
        text: `${Number(r.s2_vol_surge_ratio ?? 1).toFixed(2)}x`,
      }),
      h(
        'td',
        null,
        r.event_trigger
          ? h('span', { className: 'status-pill watch', text: String(r.event_trigger) })
          : '—'
      ),
      h('td', { className: `num-cell ${wCls}`, text: fmtPct(r.w_s2_trend_breakout) }),
      h('td', { className: 'num-cell text-strong-white', text: fmtPct(r.target_weight) }),
      h('td', { className: 'num-cell', text: fmtPct(r.current_weight) })
    );
  });

  tbS2.replaceChildren(...rows);
  attachRowSortData(tbS2, sliced, S2_SORT_EXTRACTORS);
}

function renderSleeveS3Table(s3, force) {
  const tbS3 = getEl('tbody-sleeve-s3');
  if (!tbS3) return;
  const sortSpec = state.tableSort['table-sleeve-s3'];
  const fp = computeFingerprint([s3, sortSpec]);
  if (!force && !isSectionDirty('tab-s3', fp)) return;

  const sorted = sortItemsForTable(s3, 'table-sleeve-s3', S3_SORT_EXTRACTORS);
  const sliced = sorted.slice(0, 60);
  tbS3.dataset.truncated = sorted.length > sliced.length ? 'true' : 'false';

  if (sliced.length === 0) {
    tbS3.replaceChildren(createEmptyRow(10, 'No Sleeve 3 assets matching filter.'));
    return;
  }

  const rows = sliced.map((r) => {
    const wCls =
      r.w_s3_event_reversion > 0.001
        ? 'pnl-pos'
        : r.w_s3_event_reversion < -0.001
        ? 'pnl-neg'
        : 'pnl-flat';
    return h(
      'tr',
      null,
      h('td', null, h('span', { className: 'sym-badge', text: String(r.symbol ?? '') })),
      h('td', { className: 'num-cell', text: fmtPrice(r.mark_price) }),
      h('td', { className: 'num-cell', text: fmtUsd(r.day_volume_usdc, 0) }),
      h('td', {
        className: 'num-cell',
        text: `${Number(r.funding_apr_pct || 0).toFixed(2)}%`,
      }),
      h('td', {
        className: 'num-cell text-amber-bold',
        text: `${Number(r.s3_funding_zscore || 0).toFixed(2)}σ`,
      }),
      h('td', {
        className: 'num-cell',
        text: `${Number(r.s3_flush_atr_mult || 0).toFixed(2)} ATR`,
      }),
      h(
        'td',
        null,
        r.event_trigger
          ? h('span', { className: 'status-pill working', text: String(r.event_trigger) })
          : '—'
      ),
      h('td', { className: `num-cell ${wCls}`, text: fmtPct(r.w_s3_event_reversion) }),
      h('td', { className: 'num-cell text-strong-white', text: fmtPct(r.target_weight) }),
      h('td', { className: 'num-cell', text: fmtPct(r.current_weight) })
    );
  });

  tbS3.replaceChildren(...rows);
  attachRowSortData(tbS3, sliced, S3_SORT_EXTRACTORS);
}

function renderPerpScannerTable(scanner, force) {
  const tbScan = getEl('tbody-perp-scanner');
  if (!tbScan) return;
  const sortSpec = state.tableSort['table-perp-scanner'];
  const fp = computeFingerprint([scanner, sortSpec]);
  if (!force && !isSectionDirty('tab-scanner', fp)) return;

  const sorted = sortItemsForTable(scanner, 'table-perp-scanner', SCANNER_SORT_EXTRACTORS);
  const sliced = sorted.slice(0, 200);
  tbScan.dataset.truncated = sorted.length > sliced.length ? 'true' : 'false';

  if (sliced.length === 0) {
    tbScan.replaceChildren(
      createEmptyRow(10, 'No Hyperliquid perp assets matching search filter.')
    );
    return;
  }

  const rows = sliced.map((a) => {
    const mixDiv = h('div', { className: 'sleeve-mix-pills' });
    if (a.is_priority_watched) {
      mixDiv.appendChild(h('span', { className: 'sp-tag s1', text: '1s PRIORITY' }));
    }
    if (a.is_core_eligible) {
      mixDiv.appendChild(h('span', { className: 'sp-tag s2', text: 'CORE S1/S2' }));
    }
    if (a.is_event_eligible) {
      mixDiv.appendChild(h('span', { className: 'sp-tag s3', text: 'EVENT S3' }));
    }
    if (!mixDiv.hasChildNodes()) {
      mixDiv.appendChild(h('span', { className: 'pnl-flat', text: 'SCAN' }));
    }

    return h(
      'tr',
      null,
      h('td', null, h('span', { className: 'sym-badge', text: String(a.symbol ?? '') })),
      h('td', { text: String(a.coin ?? '') }),
      h('td', { className: 'num-cell', text: fmtPrice(a.mark_price) }),
      h('td', { className: 'num-cell', text: fmtPrice(a.mid_price) }),
      h('td', { className: 'num-cell', text: fmtPrice(a.oracle_price) }),
      h('td', { className: 'num-cell', text: fmtUsd(a.day_volume_usdc, 0) }),
      h('td', { className: 'num-cell', text: fmtUsd(a.open_interest_usdc, 0) }),
      h('td', {
        className: 'num-cell',
        text: `${(Number(a.funding_rate_hourly || 0) * 100).toFixed(4)}%`,
      }),
      h('td', {
        className: 'num-cell',
        text: `${Number(a.max_leverage ?? 20).toFixed(0)}x`,
      }),
      h('td', null, mixDiv)
    );
  });

  tbScan.replaceChildren(...rows);
  attachRowSortData(tbScan, sliced, SCANNER_SORT_EXTRACTORS);
}

function renderConsoleTab(audits, logs, force) {
  const tbAudit = getEl('tbody-order-audit');
  const logStream = getEl('telemetry-log-container');
  const sortSpec = state.tableSort['table-order-audit'];
  const fpAudit = computeFingerprint([audits, sortSpec]);
  if (tbAudit && (force || isSectionDirty('tab-console-audit', fpAudit))) {
    const sorted = sortItemsForTable(audits, 'table-order-audit', AUDIT_SORT_EXTRACTORS);
    const sliced = sorted.slice(0, 40);
    tbAudit.dataset.truncated = sorted.length > sliced.length ? 'true' : 'false';
    if (sliced.length === 0) {
      tbAudit.replaceChildren(
        createEmptyRow(7, 'No order audit state transitions recorded yet.')
      );
    } else {
      const rows = sliced.map((a) => {
        const sideCls = String(a.side).toUpperCase() === 'SHORT' ? 'short' : 'long';
        return h(
          'tr',
          null,
          h('td', { className: 'num-cell', text: fmtLocalTime(a.created_at_ms) }),
          h('td', null, h('code', { text: String(a.client_order_id ?? '') })),
          h(
            'td',
            null,
            h('span', { className: `side-pill ${sideCls}`, text: String(a.side ?? '') })
          ),
          h(
            'td',
            null,
            h('span', {
              className: 'status-pill watch',
              text: `${a.from_state ?? ''} → ${a.to_state ?? ''}`,
            })
          ),
          h('td', {
            className: 'num-cell',
            text: `${fmtPrice(a.limit_price)} / ${fmtPrice(a.avg_fill_price)}`,
          }),
          h(
            'td',
            { className: 'num-cell' },
            `${fmtUsd(a.reserved_usdc)} / `,
            h('strong', { className: 'text-emerald', text: fmtUsd(a.committed_usdc) })
          ),
          h('td', {
            className: 'reason-cell',
            title: String(a.reason ?? ''),
            text: String(a.reason ?? ''),
          })
        );
      });
      tbAudit.replaceChildren(...rows);
      attachRowSortData(tbAudit, sliced, AUDIT_SORT_EXTRACTORS);
    }
  }

  const fpLogs = computeFingerprint([logs]);
  if (logStream && (force || isSectionDirty('tab-console-logs', fpLogs))) {
    const slicedLogs = logs.slice(0, 40);
    if (slicedLogs.length === 0) {
      logStream.replaceChildren(
        h('div', { className: 't-log-empty', text: 'No telemetry events recorded yet.' })
      );
    } else {
      const items = slicedLogs.map((l) => {
        const lvl = String(l.level || 'INFO').toUpperCase();
        return h(
          'div',
          { className: 't-log-item' },
          h(
            'div',
            { className: 't-log-meta' },
            h(
              'span',
              null,
              h('span', { className: `t-log-lvl ${lvl}`, text: lvl }),
              ' · ',
              h('strong', { text: String(l.component ?? '') })
            ),
            h('span', { text: fmtLocalTime(l.created_at_ms) })
          ),
          h('div', { className: 't-log-msg', text: String(l.message ?? '') })
        );
      });
      logStream.replaceChildren(...items);
    }
  }
}

/**
 * R14-M24: Updates tab count badges cheaply and renders ONLY the currently active
 * explorer tab so inactive tabs don't sort & rebuild hundreds of DOM nodes every 1.5s.
 */
export function renderExplorerTabs(dash, forceRender = false) {
  if (!dash) return;
  const query = (getEl('scanner-search-input')?.value || '').trim().toUpperCase();
  const matchSym = (item) =>
    !query ||
    String(item.symbol || '').toUpperCase().includes(query) ||
    String(item.coin || '').toUpperCase().includes(query);

  const s1 = (dash.v_assets_sleeve_s1 || []).filter(matchSym);
  const s2 = (dash.v_assets_sleeve_s2 || []).filter(matchSym);
  const s3 = (dash.v_assets_sleeve_s3 || []).filter(matchSym);
  const scanner = (dash.exchange_assets || []).filter(matchSym);
  const audits = dash.order_audit || [];
  const logs = dash.telemetry_logs || [];

  const setCnt = (id, n) => {
    const el = getEl(id);
    if (el) el.textContent = String(n);
  };
  setCnt('tab-count-s1', s1.length);
  setCnt('tab-count-s2', s2.length);
  setCnt('tab-count-s3', s3.length);
  setCnt('tab-count-scanner', scanner.length);
  setCnt('tab-count-console', audits.length + logs.length);

  switch (state.activeTab) {
    case 's1':
      renderSleeveS1Table(s1, forceRender);
      break;
    case 's2':
      renderSleeveS2Table(s2, forceRender);
      break;
    case 's3':
      renderSleeveS3Table(s3, forceRender);
      break;
    case 'scanner':
      renderPerpScannerTable(scanner, forceRender);
      break;
    case 'console':
      renderConsoleTab(audits, logs, forceRender);
      break;
    default:
      break;
  }
}
