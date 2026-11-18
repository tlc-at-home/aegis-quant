/* ============================================================================
   AEGIS QUANT v2.0 — REAL-TIME MULTI-SLEEVE TERMINAL APPLICATION (app.js)
   Backed by MariaDB 13.0 InnoDB & 1s/2s SSE Stream
   ============================================================================ */

const state = {
  health: null,
  dashboard: null,
  activeTab: 's1',
  configCategory: 'all',
  showAllPriorityWatch: false,
  closingPositionIds: new Set(),
  closingSymbols: new Set(),
  closeInFlightCount: 0,
  sse: null,
};

const IMMUTABLE_SECURITY_KEYS = new Set([
  'enforce_master_key',
  'web_auth_enabled',
  'web_allow_remote_bind',
  'web_bind_host',
  'web_csp_policy',
  'web_cors_allowed_origins',
  'web_allowed_hosts',
  'web_api_token',
  'telegram_api_base_url',
  'hyperliquid_mainnet_url',
  'hyperliquid_testnet_url',
  'hyperliquid_info_url',
  'hyperliquid_testnet_info_url',
  'hyperliquid_exchange_url',
  'hyperliquid_testnet_exchange_url',
  'hyperliquid_allowed_hosts',
]);

function fmtUsd(val, decimals = 2) {
  const num = Number(val || 0);
  const sign = num < 0 ? '-' : '';
  return (
    sign +
    '$' +
    Math.abs(num).toLocaleString('en-US', {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    })
  );
}

function fmtSignedUsd(val, decimals = 2) {
  const num = Number(val || 0);
  const sign = num > 0.0001 ? '+' : num < -0.0001 ? '-' : '';
  return (
    sign +
    '$' +
    Math.abs(num).toLocaleString('en-US', {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    })
  );
}

function fmtPrice(px) {
  const n = Number(px || 0);
  if (n === 0) return '$0.00';
  if (n >= 1000) return fmtUsd(n, 2);
  if (n >= 1) return fmtUsd(n, 4);
  return '$' + n.toFixed(6);
}

function fmtPct(fracOrPct, isFraction = true, decimals = 1) {
  const v = Number(fracOrPct || 0) * (isFraction ? 100.0 : 1.0);
  const sign = v > 0.0001 ? '+' : '';
  return `${sign}${v.toFixed(decimals)}%`;
}

function fmtUtcTime(tsMs) {
  if (!tsMs || tsMs <= 0) return '—';
  const d = new Date(Number(tsMs));
  if (Number.isNaN(d.getTime())) return '—';
  return d.toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
}

function escapeHtml(raw) {
  if (raw === null || raw === undefined) return '';
  return String(raw)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function showToast(msg, isError = false) {
  const el = document.getElementById('toast-banner');
  if (!el) return;
  el.textContent = msg;
  el.style.borderColor = isError
    ? 'rgba(244, 63, 94, 0.7)'
    : 'rgba(0, 242, 254, 0.6)';
  el.classList.add('visible');
  clearTimeout(window.__aegisToastTimer);
  window.__aegisToastTimer = setTimeout(() => {
    el.classList.remove('visible');
  }, 4200);
}

async function apiFetch(url, options = {}) {
  const headers = new Headers(options.headers || {});
  if (!headers.has('Content-Type') && options.body) {
    headers.set('Content-Type', 'application/json');
  }
  headers.set('X-Requested-With', 'XMLHttpRequest');
  return fetch(url, {
    cache: 'no-store',
    credentials: 'same-origin',
    ...options,
    headers,
  });
}

/* ============================================================================
   INTERACTIVE ACTIONS (RISK TIER, TELEGRAM TEST, CLOSE POSITION, CONFIG MODAL)
   ============================================================================ */

async function switchRiskTier(tier) {
  try {
    const res = await apiFetch('/api/v1/risk-tier', {
      method: 'POST',
      body: JSON.stringify({ risk_tier: tier }),
    });
    const data = await res.json();
    if (!res.ok) {
      showToast(`Risk tier update failed: ${data.detail || data.error || res.status}`, true);
      return;
    }
    updateRiskTierButtons(data.risk_tier || tier);
    showToast(
      `Switched active Risk Tier -> ${(data.risk_tier || tier).toUpperCase()} (${Number(
        data.target_leverage_multiplier || 1.85
      ).toFixed(2)}x)`
    );
    await refreshAll(true);
  } catch (err) {
    showToast(`Failed to switch Risk Tier: ${err.message}`, true);
  }
}

async function triggerTelegramTestAlert() {
  const btn = document.getElementById('btn-telegram-test');
  if (btn) btn.disabled = true;
  try {
    showToast('Dispatching formatted HTML Telegram test alert...');
    const res = await apiFetch('/api/v1/alerts/test', {
      method: 'POST',
      body: JSON.stringify({}),
    });
    const data = await res.json();
    if (!res.ok || data.delivered === false) {
      showToast(
        `Telegram alert status: ${data.detail || data.error || 'Check bot credentials'}`,
        true
      );
    } else {
      showToast(
        `Telegram HTML alert delivered (${data.provider || 'telegram'} · HTTP ${
          data.http_status || 200
        })`
      );
    }
    await refreshAll();
  } catch (err) {
    showToast(`Telegram test request error: ${err.message}`, true);
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function triggerHyperliquidReconcile() {
  const btn = document.getElementById('btn-sync-hyperliquid');
  if (btn) btn.disabled = true;
  try {
    showToast('Synchronizing MariaDB 1:1 with Hyperliquid clearinghouseState & userFills...');
    const res = await apiFetch('/api/v1/reconcile', {
      method: 'POST',
      body: JSON.stringify({ close_legacy_positions: false }),
    });
    const data = await res.json();
    if (!res.ok) {
      showToast(`Reconcile error: ${data.detail || data.error || res.status}`, true);
    } else {
      const r = data.reconcile || {};
      const eq = Number(
        (data.snapshot && data.snapshot.total_equity_usdc) ||
          r.exchange_equity_usdc ||
          0
      );
      await refreshAll(true);
      const closedYtd = Number(
        (state.dashboard &&
          state.dashboard.pnl_summary &&
          state.dashboard.pnl_summary.ytd &&
          state.dashboard.pnl_summary.ytd.trade_count) ||
          r.closed_fills_reconciled ||
          0
      );
      showToast(
        `Synced 1:1 with Hyperliquid (Equity: ${fmtUsd(eq)} · Open: ${Number(
          r.legacy_positions_found || 0
        )} · Closed Trades: ${closedYtd})`
      );
      return;
    }
    await refreshAll(true);
  } catch (err) {
    showToast(`Hyperliquid sync error: ${err.message}`, true);
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function closePositionOrOrder(positionId, symbol, applyCooldown = true) {
  const pid = Number(positionId || 0);
  const sym = String(symbol || '').trim();
  if ((pid > 0 && state.closingPositionIds.has(pid)) || (sym && state.closingSymbols.has(sym))) {
    return;
  }
  if (pid > 0) state.closingPositionIds.add(pid);
  if (sym) state.closingSymbols.add(sym);
  state.closeInFlightCount += 1;

  // Immediate visual feedback on button + optimistic removal
  document
    .querySelectorAll(`.close-btn[data-close-pos-id="${pid}"]`)
    .forEach((btn) => {
      btn.disabled = true;
      btn.textContent = 'Closing...';
    });

  if (state.dashboard) {
    state.dashboard.v_assets_held = (state.dashboard.v_assets_held || []).filter(
      (h) => Number(h.position_id) !== pid && h.symbol !== sym
    );
    state.dashboard.v_assets_ordered = (state.dashboard.v_assets_ordered || []).filter(
      (o) => Number(o.position_id) !== pid && o.symbol !== sym
    );
    renderOpenHoldingsTable(state.dashboard, true);
    renderPriorityAndWorkingTable(state.dashboard, true);
  }

  try {
    const res = await apiFetch('/api/v1/positions/close', {
      method: 'POST',
      body: JSON.stringify({
        position_id: pid > 0 ? pid : null,
        symbol: sym,
        cooldown_sec: applyCooldown ? 3600 : 600,
        reason: 'WEB_UI_OPERATOR_CLOSE',
      }),
    });
    const data = await res.json();
    if (!res.ok) {
      showToast(`Close failed: ${data.detail || data.error || res.status}`, true);
      return;
    }
    showToast(
      `Closed ${data.symbol || sym} (#${data.position_id || pid}) -> ${
        data.new_status || 'CLOSED'
      }`
    );
  } catch (err) {
    showToast(`Error closing position: ${err.message}`, true);
  } finally {
    state.closeInFlightCount = Math.max(0, state.closeInFlightCount - 1);
    setTimeout(() => {
      if (pid > 0) state.closingPositionIds.delete(pid);
      if (sym) state.closingSymbols.delete(sym);
    }, 2500);
    await refreshAll(true);
  }
}

async function closeAllOpenPositions() {
  const btn = document.getElementById('btn-close-all-open');
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Closing All...';
  }
  state.closeInFlightCount += 1;
  try {
    showToast('Closing all open positions & cancelling working orders...');
    const res = await apiFetch('/api/v1/positions/close', {
      method: 'POST',
      body: JSON.stringify({
        close_all: true,
        cooldown_sec: 3600,
        reason: 'WEB_UI_CLOSE_ALL',
      }),
    });
    const data = await res.json();
    if (!res.ok) {
      showToast(`Close All failed: ${data.detail || data.error || res.status}`, true);
      return;
    }
    showToast(
      `Closed ${Number(data.closed_count || 0)} position(s)/order(s) (Realized PnL: ${fmtSignedUsd(
        data.realized_pnl_usdc || 0
      )})`
    );
  } catch (err) {
    showToast(`Error closing all positions: ${err.message}`, true);
  } finally {
    state.closeInFlightCount = Math.max(0, state.closeInFlightCount - 1);
    if (btn) {
      btn.disabled = false;
      btn.textContent = 'Close All';
    }
    await refreshAll(true);
  }
}

async function submitConfigUpdate() {
  const keyEl = document.getElementById('cfg-key-input');
  const valEl = document.getElementById('cfg-val-input');
  const key = keyEl ? keyEl.value.trim() : '';
  const val = valEl ? valEl.value.trim() : '';
  if (!key) {
    showToast('Please enter a config_key to update', true);
    return;
  }
  await saveConfigKeyValue(key, val);
  if (valEl) valEl.value = '';
}

async function saveSingleConfigRow(key, inputId) {
  const el = document.getElementById(inputId);
  if (!el) return;
  const val = el.value.trim();
  if (el.getAttribute('data-is-secret') === 'true' && !val) {
    showToast(`Enter a new secret value before saving '${key}'`, true);
    return;
  }
  await saveConfigKeyValue(key, val);
}

async function saveConfigKeyValue(key, val) {
  try {
    if (key === 'risk_tier') {
      await switchRiskTier(val);
      return;
    }
    const res = await apiFetch('/api/v1/config', {
      method: 'POST',
      body: JSON.stringify({ config_key: key, config_value: val }),
    });
    const data = await res.json();
    if (!res.ok) {
      showToast(`Config update failed: ${data.detail || data.error || res.status}`, true);
      return;
    }
    showToast(`Saved config '${key}'`);
    await refreshAll(true);
  } catch (err) {
    showToast(`Config update error: ${err.message}`, true);
  }
}

function openConfigModal() {
  const modal = document.getElementById('config-settings-modal');
  if (modal) {
    modal.classList.add('open');
    renderSystemConfigTables(true);
  }
}

function closeConfigModal() {
  const modal = document.getElementById('config-settings-modal');
  if (modal) {
    modal.classList.remove('open');
  }
}

function handleConfigModalBackdropClick(evt) {
  if (evt.target && evt.target.id === 'config-settings-modal') {
    closeConfigModal();
  }
}

function setConfigCategory(cat) {
  state.configCategory = cat;
  document.querySelectorAll('.cfg-cat-btn[data-cat]').forEach((btn) => {
    btn.classList.toggle('active', btn.getAttribute('data-cat') === cat);
  });
  renderSystemConfigTables(true);
}

function classifyConfigKeyCategory(key) {
  const k = String(key || '').toLowerCase();
  if (
    k.includes('risk_tier') ||
    k.includes('leverage') ||
    k.includes('weight') ||
    k.includes('stop') ||
    k.includes('cooldown') ||
    k.includes('equity')
  ) {
    return 'risk';
  }
  if (
    k.includes('execution') ||
    k.includes('hyperliquid') ||
    k.includes('scanner') ||
    k.includes('priority') ||
    k.includes('chase') ||
    k.includes('fee') ||
    k.includes('notional') ||
    k.includes('volume')
  ) {
    return 'execution';
  }
  if (k.startsWith('s1_') || k.startsWith('s2_') || k.startsWith('s3_')) {
    return 'sleeves';
  }
  if (k.includes('alert') || k.includes('telegram') || k.includes('discord')) {
    return 'alerts';
  }
  if (
    k.includes('wallet') ||
    k.includes('private_key') ||
    k.includes('agent') ||
    k.includes('vault') ||
    k.includes('master_key') ||
    k.includes('token')
  ) {
    return 'wallet';
  }
  return 'all';
}

function toggleShowAllPriorityWatch() {
  state.showAllPriorityWatch = !state.showAllPriorityWatch;
  if (state.dashboard) {
    renderPriorityAndWorkingTable(state.dashboard);
  }
}

function switchExplorerTab(tabName) {
  state.activeTab = tabName;
  document.querySelectorAll('.tab-btn').forEach((btn) => {
    btn.classList.toggle('active', btn.getAttribute('data-tab') === tabName);
  });
  document.querySelectorAll('.tab-pane').forEach((pane) => {
    pane.classList.toggle('active', pane.id === `tab-pane-${tabName}`);
  });
  renderActiveExplorerTab(true);
}

function updateRiskTierButtons(activeTier) {
  const norm = String(activeTier || 'extreme').toLowerCase();
  document.querySelectorAll('.tier-btn').forEach((btn) => {
    btn.classList.toggle('active', btn.getAttribute('data-tier') === norm);
  });
}

/* ============================================================================
   TOP TELEMETRY, PNL HORIZON RIBBON, KPI CARDS & TABLES RENDERERS
   ============================================================================ */

function renderHealthAndTopBar(health, dash) {
  const dbVer =
    (health && health.mariadb_version) ||
    (dash && dash.mariadb_version) ||
    '13.0.2-MariaDB';
  const dbText = document.getElementById('mariadb-version-text');
  if (dbText) {
    dbText.textContent = `${dbVer} · Online`;
  }

  const prioWatch = (dash && dash.v_priority_watchlist) || [];
  const prioEl = document.getElementById('prio-watch-count');
  if (prioEl) prioEl.textContent = String(prioWatch.length);

  const exAssets = (dash && dash.exchange_assets) || [];
  const scanEl = document.getElementById('scanner-asset-count');
  if (scanEl) scanEl.textContent = `${exAssets.length} PERPS`;

  const mode =
    (dash && dash.execution_mode) ||
    (health && health.execution_mode) ||
    'TESTNET';
  const modeBadge = document.getElementById('badge-exec-mode');
  const modeText = document.getElementById('exec-mode-text');
  if (modeText) modeText.textContent = mode.toUpperCase();
  if (modeBadge) {
    modeBadge.classList.toggle('mode-live', mode.toUpperCase() === 'LIVE');
    modeBadge.classList.toggle('mode-testnet', mode.toUpperCase() !== 'LIVE');
  }

  const tier =
    (dash && dash.risk_tier) ||
    (dash && dash.snapshot && dash.snapshot.risk_tier) ||
    (health && health.risk_tier) ||
    'extreme';
  updateRiskTierButtons(tier);
}

function renderPnlHorizonRibbon(dash) {
  const summary = (dash && dash.pnl_summary) || {};
  const fallbackUpnl = ((dash && dash.v_assets_held) || []).reduce(
    (acc, h) => acc + Number(h.unrealized_pnl_usdc || 0),
    0
  );
  const totalEq = Number((dash && dash.snapshot && dash.snapshot.total_equity_usdc) || 10000);

  const paintHorizon = (key, defaultObj) => {
    const h = summary[key] || defaultObj;
    const netPnl = Number(h.net_pnl_usdc ?? fallbackUpnl);
    const retPct = Number(
      h.return_pct ?? (totalEq > 0 ? (netPnl / totalEq) * 100.0 : 0.0)
    );
    const rpnl = Number(h.realized_pnl_usdc || 0);
    const upnl = Number(h.unrealized_pnl_usdc ?? fallbackUpnl);
    const trades = Number(h.trade_count || 0);
    const wins = Number(h.win_count || 0);

    const tone = netPnl > 0.005 ? 'pos' : netPnl < -0.005 ? 'neg' : 'flat';

    const cardEl = document.getElementById(`pnl-card-${key}`);
    const pctEl = document.getElementById(`pnl-${key}-pct`);
    const usdcEl = document.getElementById(`pnl-${key}-usdc`);
    const subEl = document.getElementById(`pnl-${key}-sub`);

    if (cardEl) {
      cardEl.classList.remove('pos', 'neg', 'flat');
      cardEl.classList.add(tone);
    }
    if (pctEl) {
      pctEl.classList.remove('pos', 'neg', 'flat');
      pctEl.classList.add(tone);
      pctEl.textContent = `${retPct > 0.001 ? '+' : ''}${retPct.toFixed(2)}%`;
    }
    if (usdcEl) {
      usdcEl.classList.remove('pos', 'neg', 'flat');
      usdcEl.classList.add(tone);
      usdcEl.textContent = fmtSignedUsd(netPnl);
    }
    if (subEl) {
      const tradeStr =
        trades > 0 ? `${trades} Trades (${wins}W)` : '0 Closed · Live Mark';
      subEl.innerHTML = `
        <span>Realized: ${fmtSignedUsd(rpnl)} · Unrealized: ${fmtSignedUsd(upnl)}</span>
        <span>${escapeHtml(tradeStr)}</span>
      `;
    }
  };

  const fallback = {
    net_pnl_usdc: fallbackUpnl,
    realized_pnl_usdc: 0,
    unrealized_pnl_usdc: fallbackUpnl,
    trade_count: 0,
    win_count: 0,
  };
  paintHorizon('daily', fallback);
  paintHorizon('weekly', fallback);
  paintHorizon('monthly', fallback);
  paintHorizon('ytd', fallback);
}

function renderKpiCards(dash) {
  const snap = (dash && dash.snapshot) || {};
  const totalEq = Number(snap.total_equity_usdc || 10000.0);
  const availCash = Number(snap.available_cash_usdc ?? totalEq);
  const reservedMargin = Number(snap.reserved_order_cash_usdc || 0.0);
  const committedNotional = Number(snap.committed_notional_usdc || 0.0);
  const netNotional = Number(snap.net_notional_usdc || 0.0);
  const grossLev = Number(snap.gross_leverage_x || 0.0);
  const netLev = Number(snap.net_leverage_x || 0.0);
  const tier = String(snap.risk_tier || dash.risk_tier || 'extreme').toUpperCase();
  const mult = Number(
    snap.leverage_multiplier || dash.target_leverage_multiplier || 1.85
  );
  const breadth = Number(snap.market_breadth_bull ?? 0.5);
  const beta = Number(snap.market_beta_exposure || 0.0);
  const regime = String(snap.market_regime_label || 'NEUTRAL');

  const setTxt = (id, text) => {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  };
  const setWidth = (id, pct) => {
    const el = document.getElementById(id);
    if (el) el.style.width = `${Math.max(0, Math.min(100, pct)).toFixed(1)}%`;
  };

  setTxt('kpi-total-equity', fmtUsd(totalEq));
  const tierIcon =
    tier === 'EXTREME'
      ? '🔥'
      : tier === 'AGGRESSIVE'
      ? '⚡'
      : tier === 'STANDARD'
      ? '⚖️'
      : '🛡️';
  setTxt('kpi-tier-badge', `${tierIcon} ${tier} (${mult.toFixed(2)}x)`);
  setTxt(
    'kpi-eligible-coins',
    `${Number(snap.eligible_coin_count || (dash.exchange_assets || []).length)} Eligible Perps`
  );

  const cashPct = totalEq > 0 ? (availCash / totalEq) * 100.0 : 100.0;
  setTxt('kpi-avail-cash', fmtUsd(availCash));
  setTxt('kpi-cash-pct', `${cashPct.toFixed(1)}% of Equity`);
  setWidth('kpi-cash-meter', cashPct);

  const resPct = totalEq > 0 ? (reservedMargin / totalEq) * 100.0 : 0.0;
  setTxt('kpi-reserved-margin', fmtUsd(reservedMargin));
  setTxt(
    'kpi-working-count',
    `${Number(
      snap.working_order_count ?? (dash.v_assets_ordered || []).length
    )} Working ALO Orders`
  );
  setWidth('kpi-reserved-meter', Math.min(100, resPct * 2));

  const commPct = totalEq > 0 ? (committedNotional / totalEq) * 100.0 : 0.0;
  setTxt('kpi-committed-notional', fmtUsd(committedNotional));
  setTxt(
    'kpi-open-counts',
    `${Number(snap.open_long_count || 0)} Long · ${Number(
      snap.open_short_count || 0
    )} Short`
  );
  setTxt('kpi-net-notional', `Net: ${fmtUsd(netNotional)}`);
  setWidth('kpi-committed-meter', Math.min(100, commPct));

  setTxt('kpi-gross-leverage', `${grossLev.toFixed(2)}x`);
  setTxt('kpi-net-leverage', `${netLev >= 0 ? '+' : ''}${netLev.toFixed(2)}x`);
  setTxt('kpi-leverage-target', `Target: ${mult.toFixed(2)}x ${tier}`);
  setWidth('kpi-leverage-meter', (grossLev / 2.5) * 100.0);

  setTxt('kpi-regime-label', regime);
  setTxt('kpi-breadth-pct', `Bull Breadth: ${(breadth * 100.0).toFixed(1)}%`);
  setTxt('kpi-beta-exp', `Beta: ${beta >= 0 ? '+' : ''}${beta.toFixed(2)}`);
  setWidth('kpi-breadth-meter', breadth * 100.0);

  // Sleeve attribution stat boxes
  setTxt('sleeve-s1-weight', fmtPct(snap.s1_gross_weight || 0, true, 1));
  setTxt('sleeve-s2-weight', fmtPct(snap.s2_gross_weight || 0, true, 1));
  setTxt('sleeve-s3-weight', fmtPct(snap.s3_gross_weight || 0, true, 1));
}

function renderPriorityAndWorkingTable(dash, forceRender = false) {
  const tbody = document.getElementById('tbody-priority-ordered');
  const toggleBtn = document.getElementById('btn-toggle-watch-all');
  if (!tbody) return;
  if (!forceRender && state.closeInFlightCount > 0) return;

  const held = (dash && dash.v_assets_held) || [];
  const heldAssetIds = new Set(held.map((h) => Number(h.asset_id)));

  const ordered = ((dash && dash.v_assets_ordered) || []).filter(
    (o) =>
      !state.closingPositionIds.has(Number(o.position_id)) &&
      !state.closingSymbols.has(String(o.symbol))
  );
  const prio = (dash && dash.v_priority_watchlist) || [];

  const orderedByAsset = new Map();
  ordered.forEach((o) => orderedByAsset.set(Number(o.asset_id), o));

  // Filter out OPEN holdings and WORKING orders from passive priority watch list so OPEN positions ONLY render in Right Table
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
  const maxPassiveDefault = 6;

  if (toggleBtn) {
    if (passiveWatched.length > maxPassiveDefault) {
      toggleBtn.style.display = 'inline-flex';
      toggleBtn.textContent = state.showAllPriorityWatch
        ? `Show Top ${maxPassiveDefault}`
        : `Show All (${ordered.length + passiveWatched.length})`;
    } else {
      toggleBtn.style.display = 'none';
    }
  }

  const rowsHtml = [];

  // 1. Render active WORKING orders first
  ordered.forEach((o) => {
    const sideCls = String(o.side).toUpperCase() === 'SHORT' ? 'short' : 'long';
    rowsHtml.push(`
      <tr>
        <td><span class="sym-badge">${escapeHtml(o.symbol)}</span></td>
        <td><span class="status-pill working">${escapeHtml(o.fsm_state || 'WORKING_ALO')}</span></td>
        <td><span class="side-pill ${sideCls}">${escapeHtml(o.side)}</span></td>
        <td>${fmtPrice(o.limit_price)} <small style="color:var(--text-muted)">/ ${fmtPrice(o.mark_price)}</small></td>
        <td style="color: var(--neon-cyan); font-weight: 700;">${Number(o.distance_from_mid_bps || 0).toFixed(2)} bps</td>
        <td style="color: #fbbf24; font-weight: 700;">${fmtUsd(o.reserved_margin_usdc)}</td>
        <td><span class="tab-count">${Number(o.chase_count || 0)}</span></td>
        <td>
          <button
            type="button"
            class="close-btn"
            data-close-pos-id="${o.position_id}"
            data-close-symbol="${escapeHtml(o.symbol)}"
            data-close-cooldown="false"
            onclick="closePositionOrOrder(${o.position_id}, '${escapeHtml(o.symbol)}', false)"
          >Cancel</button>
        </td>
      </tr>
    `);
  });

  // 2. Render pre-entry priority watched items (capped unless toggled)
  const visibleWatched = state.showAllPriorityWatch
    ? passiveWatched
    : passiveWatched.slice(0, maxPassiveDefault);

  visibleWatched.forEach((p) => {
    const statusLabel = p.event_trigger || 'PRIORITY_WATCH';
    rowsHtml.push(`
      <tr>
        <td><span class="sym-badge">${escapeHtml(p.symbol)}</span></td>
        <td><span class="status-pill watch">${escapeHtml(statusLabel)}</span></td>
        <td><span class="pnl-flat">—</span></td>
        <td>${fmtPrice(p.mark_price)} <small style="color:var(--text-muted)">/ ${fmtPrice(p.mid_price || p.mark_price)}</small></td>
        <td>0.00 bps</td>
        <td>$0.00</td>
        <td><span class="tab-count">1s</span></td>
        <td><span class="pnl-flat">1s Watch</span></td>
      </tr>
    `);
  });

  if (rowsHtml.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="8" style="text-align: center; color: var(--text-muted); padding: 18px;">
          No active WORKING orders or priority triggers — monitoring 1s watchlist stream...
        </td>
      </tr>
    `;
  } else {
    tbody.innerHTML = rowsHtml.join('');
  }
}

function renderOpenHoldingsTable(dash, forceRender = false) {
  const tbody = document.getElementById('tbody-assets-held');
  const pill = document.getElementById('held-count-pill');
  const closeAllBtn = document.getElementById('btn-close-all-open');
  if (!tbody) return;
  if (!forceRender && state.closeInFlightCount > 0) return;

  const held = ((dash && dash.v_assets_held) || []).filter(
    (h) =>
      !state.closingPositionIds.has(Number(h.position_id)) &&
      !state.closingSymbols.has(String(h.symbol))
  );
  if (pill) pill.textContent = `${held.length} OPEN`;
  if (closeAllBtn) {
    closeAllBtn.style.display = held.length > 0 ? 'inline-flex' : 'none';
  }

  if (held.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="8" style="text-align: center; color: var(--text-muted); padding: 18px;">
          No active OPEN holdings — capital remains in Available Cash until exchange fill confirmation.
        </td>
      </tr>
    `;
    return;
  }

  tbody.innerHTML = held
    .map((h) => {
      const sideCls = String(h.side).toUpperCase() === 'SHORT' ? 'short' : 'long';
      const upnl = Number(h.unrealized_pnl_usdc || 0);
      const pnlCls = upnl > 0.001 ? 'pnl-pos' : upnl < -0.001 ? 'pnl-neg' : 'pnl-flat';
      const stopDist =
        h.stop_distance_pct !== null && h.stop_distance_pct !== undefined
          ? `${Number(h.stop_distance_pct).toFixed(2)}%`
          : '—';
      return `
        <tr>
          <td><span class="sym-badge">${escapeHtml(h.symbol)}</span></td>
          <td><span class="side-pill ${sideCls}">${escapeHtml(h.side)}</span></td>
          <td style="font-weight: 700; color: #fff;">${fmtUsd(h.committed_notional_usdc)} <small style="color:var(--text-muted)">(${fmtPct(h.portfolio_weight || 0)})</small></td>
          <td>${fmtPrice(h.entry_price)} <small style="color:var(--neon-cyan)">→ ${fmtPrice(h.mark_price)}</small></td>
          <td>${fmtPrice(h.protective_stop_px)} <small style="color:#fbbf24">(${stopDist})</small></td>
          <td class="${pnlCls}">${fmtSignedUsd(upnl)}</td>
          <td>
            <div class="sleeve-mix-pills">
              <span class="sp-tag s1" title="Sleeve 1 Carry-Momentum">S1 ${fmtPct(h.s1_weight_contrib || 0)}</span>
              <span class="sp-tag s2" title="Sleeve 2 Trend-Breakout">S2 ${fmtPct(h.s2_weight_contrib || 0)}</span>
              <span class="sp-tag s3" title="Sleeve 3 Event-Reversion">S3 ${fmtPct(h.s3_weight_contrib || 0)}</span>
            </div>
          </td>
          <td>
            <button
              type="button"
              class="close-btn"
              data-close-pos-id="${h.position_id}"
              data-close-symbol="${escapeHtml(h.symbol)}"
              data-close-cooldown="true"
              onclick="closePositionOrOrder(${h.position_id}, '${escapeHtml(h.symbol)}', true)"
            >Close</button>
          </td>
        </tr>
      `;
    })
    .join('');
}

function buildInlineConfigControlHtml(row, idPrefix) {
  const key = String(row.config_key || '');
  const val = String(row.config_value ?? '');
  const vtype = String(row.value_type || 'string').toLowerCase();
  const inputId = `${idPrefix}-cfg-input-${key.replace(/[^a-zA-Z0-9_-]/g, '_')}`;

  if (IMMUTABLE_SECURITY_KEYS.has(key.toLowerCase())) {
    return `<span class="pnl-flat" title="Immutable security policy key">Locked Policy</span>`;
  }

  // Dropdown for well-known enum or boolean config keys
  let enumOptions = null;
  if (key === 'execution_mode') {
    enumOptions = ['TESTNET', 'LIVE', 'PAPER'];
  } else if (key === 'risk_tier') {
    enumOptions = ['extreme', 'aggressive', 'standard', 'conservative', 'custom'];
  } else if (key === 'alert_provider') {
    enumOptions = ['telegram', 'discord', 'both'];
  } else if (vtype === 'bool' || val === 'true' || val === 'false') {
    enumOptions = ['true', 'false'];
  }

  if (enumOptions) {
    const optsHtml = enumOptions
      .map(
        (opt) =>
          `<option value="${escapeHtml(opt)}" ${
            val.toLowerCase() === opt.toLowerCase() ? 'selected' : ''
          }>${escapeHtml(opt)}</option>`
      )
      .join('');
    return `
      <div class="cfg-inline-controls">
        <select id="${inputId}" class="cfg-inline-select">${optsHtml}</select>
        <button type="button" class="cfg-save-btn" onclick="saveSingleConfigRow('${escapeHtml(key)}', '${inputId}')">Save</button>
      </div>
    `;
  }

  const isSecret =
    vtype === 'secret' ||
    val === '[FERNET_ENCRYPTED_SECRET]' ||
    val === '[UNSET]' ||
    key.includes('private_key') ||
    key.includes('token') ||
    key.includes('webhook');

  if (isSecret && key !== 'telegram_chat_id') {
    return `
      <div class="cfg-inline-controls">
        <input
          type="password"
          id="${inputId}"
          class="cfg-inline-input"
          data-is-secret="true"
          placeholder="Enter new secret to overwrite..."
          onkeydown="if(event.key==='Enter') saveSingleConfigRow('${escapeHtml(key)}', '${inputId}')"
        />
        <button type="button" class="cfg-save-btn" onclick="saveSingleConfigRow('${escapeHtml(key)}', '${inputId}')">Save</button>
      </div>
    `;
  }

  const prefill = val === '[UNSET]' || val.includes('...') ? '' : val;
  const placeholder = val.includes('...') ? `${val} (enter full value)` : 'Enter value...';
  return `
    <div class="cfg-inline-controls">
      <input
        type="text"
        id="${inputId}"
        class="cfg-inline-input"
        value="${escapeHtml(prefill)}"
        placeholder="${escapeHtml(placeholder)}"
        onkeydown="if(event.key==='Enter') saveSingleConfigRow('${escapeHtml(key)}', '${inputId}')"
      />
      <button type="button" class="cfg-save-btn" onclick="saveSingleConfigRow('${escapeHtml(key)}', '${inputId}')">Save</button>
    </div>
  `;
}

function renderSystemConfigTables(force = false) {
  const dash = state.dashboard;
  if (!dash) return;

  // Do not overwrite inline config inputs while the operator is actively typing inside one
  if (!force && document.activeElement) {
    const activeCls = document.activeElement.className || '';
    if (
      activeCls.includes('cfg-inline-input') ||
      activeCls.includes('cfg-inline-select')
    ) {
      return;
    }
  }

  const configs = dash.system_config || [];
  const cat = state.configCategory || 'all';
  const tabQuery = (document.getElementById('scanner-search-input')?.value || '')
    .trim()
    .toUpperCase();
  const modalQuery = (document.getElementById('modal-cfg-search')?.value || '')
    .trim()
    .toUpperCase();

  const filterRows = (q) =>
    configs.filter((c) => {
      if (cat !== 'all' && classifyConfigKeyCategory(c.config_key) !== cat) {
        return false;
      }
      if (!q) return true;
      return (
        String(c.config_key || '')
          .toUpperCase()
          .includes(q) ||
        String(c.description || '')
          .toUpperCase()
          .includes(q)
      );
    });

  const renderRows = (rows, prefix) =>
    rows
      .map(
        (c) => `
        <tr>
          <td><strong style="color:var(--neon-cyan)">${escapeHtml(c.config_key)}</strong></td>
          <td><code>${escapeHtml(c.config_value)}</code></td>
          <td>${buildInlineConfigControlHtml(c, prefix)}</td>
          <td><span class="tab-count">${escapeHtml(c.value_type)}</span></td>
          <td style="color:var(--text-muted); white-space: normal; max-width: 340px;">${escapeHtml(c.description)}</td>
          <td>${fmtUtcTime(c.updated_at_ms)}</td>
        </tr>
      `
      )
      .join('') ||
    '<tr><td colspan="6" style="text-align:center;padding:16px;color:var(--text-muted)">No configuration keys matching filter.</td></tr>';

  const tbTab = document.getElementById('tbody-system-config');
  if (tbTab) {
    tbTab.innerHTML = renderRows(filterRows(tabQuery), 'tab');
  }

  const tbModal = document.getElementById('tbody-modal-system-config');
  if (tbModal) {
    tbModal.innerHTML = renderRows(filterRows(modalQuery), 'modal');
  }
}

function renderActiveExplorerTab(forceConfig = false) {
  const dash = state.dashboard;
  if (!dash) return;

  const query = (document.getElementById('scanner-search-input')?.value || '')
    .trim()
    .toUpperCase();
  const matchSym = (item) =>
    !query ||
    String(item.symbol || '')
      .toUpperCase()
      .includes(query) ||
    String(item.coin || '')
      .toUpperCase()
      .includes(query);

  const s1 = (dash.v_assets_sleeve_s1 || []).filter(matchSym);
  const s2 = (dash.v_assets_sleeve_s2 || []).filter(matchSym);
  const s3 = (dash.v_assets_sleeve_s3 || []).filter(matchSym);
  const scanner = (dash.exchange_assets || []).filter(matchSym);
  const audits = dash.order_audit || [];
  const logs = dash.telemetry_logs || [];

  const setCnt = (id, n) => {
    const el = document.getElementById(id);
    if (el) el.textContent = String(n);
  };
  setCnt('tab-count-s1', s1.length);
  setCnt('tab-count-s2', s2.length);
  setCnt('tab-count-s3', s3.length);
  setCnt('tab-count-scanner', scanner.length);
  setCnt('tab-count-console', audits.length + logs.length);

  // Render S1
  const tbS1 = document.getElementById('tbody-sleeve-s1');
  if (tbS1) {
    tbS1.innerHTML =
      s1
        .slice(0, 60)
        .map((r) => {
          const wCls =
            r.w_s1_carry_mom > 0.001
              ? 'pnl-pos'
              : r.w_s1_carry_mom < -0.001
              ? 'pnl-neg'
              : 'pnl-flat';
          return `
            <tr>
              <td><span class="sym-badge">${escapeHtml(r.symbol)}</span></td>
              <td>${fmtPrice(r.mark_price)}</td>
              <td>${fmtUsd(r.day_volume_usdc, 0)}</td>
              <td>${(Number(r.funding_rate_hourly || 0) * 100).toFixed(4)}%</td>
              <td class="${r.funding_apr_pct >= 0 ? 'pnl-pos' : 'pnl-neg'}">${Number(r.funding_apr_pct || 0).toFixed(2)}%</td>
              <td>${(Number(r.annualized_vol || 0) * 100).toFixed(1)}%</td>
              <td>${Number(r.market_beta || 1).toFixed(2)}</td>
              <td>${Number(r.s1_mom_score || 0).toFixed(3)}</td>
              <td>${Number(r.s1_carry_score || 0).toFixed(3)}</td>
              <td class="${wCls}">${fmtPct(r.w_s1_carry_mom)}</td>
              <td style="font-weight:700;color:#fff;">${fmtPct(r.target_weight)}</td>
            </tr>
          `;
        })
        .join('') ||
      '<tr><td colspan="11" style="text-align:center;padding:16px;color:var(--text-muted)">No Sleeve 1 assets matching filter.</td></tr>';
  }

  // Render S2
  const tbS2 = document.getElementById('tbody-sleeve-s2');
  if (tbS2) {
    tbS2.innerHTML =
      s2
        .slice(0, 60)
        .map((r) => {
          const align = Number(r.s2_trend_alignment || 0);
          const alignBadge =
            align > 0
              ? '<span class="side-pill long">BULL (+1)</span>'
              : align < 0
              ? '<span class="side-pill short">BEAR (-1)</span>'
              : '<span class="pnl-flat">NEUTRAL (0)</span>';
          const wCls =
            r.w_s2_trend_breakout > 0.001
              ? 'pnl-pos'
              : r.w_s2_trend_breakout < -0.001
              ? 'pnl-neg'
              : 'pnl-flat';
          return `
            <tr>
              <td><span class="sym-badge">${escapeHtml(r.symbol)}</span></td>
              <td>${fmtPrice(r.mark_price)}</td>
              <td>${fmtUsd(r.day_volume_usdc, 0)}</td>
              <td>${(Number(r.annualized_vol || 0) * 100).toFixed(1)}%</td>
              <td>${alignBadge}</td>
              <td style="color:var(--neon-cyan);font-weight:700;">${Number(r.s2_vol_surge_ratio || 1).toFixed(2)}x</td>
              <td>${r.event_trigger ? `<span class="status-pill watch">${escapeHtml(r.event_trigger)}</span>` : '—'}</td>
              <td class="${wCls}">${fmtPct(r.w_s2_trend_breakout)}</td>
              <td style="font-weight:700;color:#fff;">${fmtPct(r.target_weight)}</td>
              <td>${fmtPct(r.current_weight)}</td>
            </tr>
          `;
        })
        .join('') ||
      '<tr><td colspan="10" style="text-align:center;padding:16px;color:var(--text-muted)">No Sleeve 2 assets matching filter.</td></tr>';
  }

  // Render S3
  const tbS3 = document.getElementById('tbody-sleeve-s3');
  if (tbS3) {
    tbS3.innerHTML =
      s3
        .slice(0, 60)
        .map((r) => {
          const wCls =
            r.w_s3_event_reversion > 0.001
              ? 'pnl-pos'
              : r.w_s3_event_reversion < -0.001
              ? 'pnl-neg'
              : 'pnl-flat';
          return `
            <tr>
              <td><span class="sym-badge">${escapeHtml(r.symbol)}</span></td>
              <td>${fmtPrice(r.mark_price)}</td>
              <td>${fmtUsd(r.day_volume_usdc, 0)}</td>
              <td>${Number(r.funding_apr_pct || 0).toFixed(2)}%</td>
              <td style="color:#fbbf24;font-weight:700;">${Number(r.s3_funding_zscore || 0).toFixed(2)}σ</td>
              <td>${Number(r.s3_flush_atr_mult || 0).toFixed(2)} ATR</td>
              <td>${r.event_trigger ? `<span class="status-pill working">${escapeHtml(r.event_trigger)}</span>` : '—'}</td>
              <td class="${wCls}">${fmtPct(r.w_s3_event_reversion)}</td>
              <td style="font-weight:700;color:#fff;">${fmtPct(r.target_weight)}</td>
              <td>${fmtPct(r.current_weight)}</td>
            </tr>
          `;
        })
        .join('') ||
      '<tr><td colspan="10" style="text-align:center;padding:16px;color:var(--text-muted)">No Sleeve 3 assets matching filter.</td></tr>';
  }

  // Render 150+ Perp Scanner
  const tbScan = document.getElementById('tbody-perp-scanner');
  if (tbScan) {
    tbScan.innerHTML =
      scanner
        .slice(0, 200)
        .map((a) => {
          const tags = [];
          if (a.is_priority_watched)
            tags.push('<span class="sp-tag s1">1s PRIORITY</span>');
          if (a.is_core_eligible)
            tags.push('<span class="sp-tag s2">CORE S1/S2</span>');
          if (a.is_event_eligible)
            tags.push('<span class="sp-tag s3">EVENT S3</span>');
          return `
            <tr>
              <td><span class="sym-badge">${escapeHtml(a.symbol)}</span></td>
              <td>${escapeHtml(a.coin)}</td>
              <td>${fmtPrice(a.mark_price)}</td>
              <td>${fmtPrice(a.mid_price)}</td>
              <td>${fmtPrice(a.oracle_price)}</td>
              <td>${fmtUsd(a.day_volume_usdc, 0)}</td>
              <td>${fmtUsd(a.open_interest_usdc, 0)}</td>
              <td>${(Number(a.funding_rate_hourly || 0) * 100).toFixed(4)}%</td>
              <td>${Number(a.max_leverage || 20).toFixed(0)}x</td>
              <td><div class="sleeve-mix-pills">${tags.join('') || '<span class="pnl-flat">SCAN</span>'}</div></td>
            </tr>
          `;
        })
        .join('') ||
      '<tr><td colspan="10" style="text-align:center;padding:16px;color:var(--text-muted)">No Hyperliquid perp assets matching search filter.</td></tr>';
  }

  // Render Order Audit & Telemetry Console
  const tbAudit = document.getElementById('tbody-order-audit');
  if (tbAudit) {
    tbAudit.innerHTML =
      audits
        .slice(0, 40)
        .map((a) => {
          const sideCls =
            String(a.side).toUpperCase() === 'SHORT' ? 'short' : 'long';
          return `
            <tr>
              <td>${fmtUtcTime(a.created_at_ms)}</td>
              <td><code>${escapeHtml(a.client_order_id)}</code></td>
              <td><span class="side-pill ${sideCls}">${escapeHtml(a.side)}</span></td>
              <td><span class="status-pill watch">${escapeHtml(a.from_state)} → ${escapeHtml(a.to_state)}</span></td>
              <td>${fmtPrice(a.limit_price)} / ${fmtPrice(a.avg_fill_price)}</td>
              <td>${fmtUsd(a.reserved_usdc)} / <strong style="color:#34d399">${fmtUsd(a.committed_usdc)}</strong></td>
              <td>${escapeHtml(a.reason)}</td>
            </tr>
          `;
        })
        .join('') ||
      '<tr><td colspan="7" style="text-align:center;padding:16px;color:var(--text-muted)">No order audit state transitions recorded yet.</td></tr>';
  }

  const logStream = document.getElementById('telemetry-log-container');
  if (logStream) {
    logStream.innerHTML =
      logs
        .slice(0, 40)
        .map((l) => {
          const lvl = String(l.level || 'INFO').toUpperCase();
          return `
            <div class="t-log-item">
              <div class="t-log-meta">
                <span><span class="t-log-lvl ${escapeHtml(lvl)}">${escapeHtml(lvl)}</span> · <strong>${escapeHtml(l.component)}</strong></span>
                <span>${fmtUtcTime(l.created_at_ms)}</span>
              </div>
              <div style="color: var(--text-primary);">${escapeHtml(l.message)}</div>
            </div>
          `;
        })
        .join('') ||
      '<div style="color:var(--text-muted);padding:12px;">No telemetry events recorded yet.</div>';
  }

  // Render System Config Vault (both bottom tab and modal)
  renderSystemConfigTables(forceConfig);
}

/* ============================================================================
   DUAL HTML5 CANVAS CHARTS (RETINA HIGH-DPI)
   ============================================================================ */

function prepareCanvas(canvasId) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return null;
  const rect = canvas.parentElement.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const w = Math.max(280, rect.width);
  const h = Math.max(150, rect.height);
  if (canvas.width !== Math.floor(w * dpr) || canvas.height !== Math.floor(h * dpr)) {
    canvas.width = Math.floor(w * dpr);
    canvas.height = Math.floor(h * dpr);
  }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w, h };
}

function drawEquityCurveCanvas(dash) {
  const prep = prepareCanvas('equity-curve-canvas');
  if (!prep) return;
  const { ctx, w, h } = prep;
  ctx.clearRect(0, 0, w, h);

  let history = (dash && dash.snapshots_history) || [];
  if (history.length === 0 && dash && dash.snapshot) {
    history = [dash.snapshot];
  }
  if (history.length === 1) {
    history = [
      { ...history[0], created_at_ms: history[0].created_at_ms - 60000 },
      history[0],
    ];
  }

  const padL = 64;
  const padR = 18;
  const padT = 16;
  const padB = 26;
  const plotW = Math.max(10, w - padL - padR);
  const plotH = Math.max(10, h - padT - padB);

  const vals = [];
  history.forEach((pt) => {
    vals.push(Number(pt.total_equity_usdc || 10000));
    vals.push(Number(pt.committed_notional_usdc || 0));
    vals.push(Number(pt.reserved_order_cash_usdc || 0));
  });
  const minVal = 0;
  const maxVal = Math.max(10500, ...vals) * 1.06;

  // Grid lines
  ctx.strokeStyle = 'rgba(148, 163, 184, 0.12)';
  ctx.lineWidth = 1;
  ctx.font = '10px "JetBrains Mono", monospace';
  ctx.fillStyle = '#64748b';
  for (let i = 0; i <= 4; i++) {
    const frac = i / 4;
    const y = padT + plotH - frac * plotH;
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();
    const labelVal = minVal + frac * (maxVal - minVal);
    ctx.fillText('$' + Math.round(labelVal).toLocaleString(), 6, y + 3);
  }

  const xFor = (idx) =>
    padL + (history.length <= 1 ? 0 : (idx / (history.length - 1)) * plotW);
  const yFor = (v) =>
    padT + plotH - ((v - minVal) / Math.max(1, maxVal - minVal)) * plotH;

  const drawSeries = (getter, strokeColor, fillTop) => {
    ctx.beginPath();
    history.forEach((pt, idx) => {
      const x = xFor(idx);
      const y = yFor(getter(pt));
      if (idx === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = strokeColor;
    ctx.lineWidth = 2.2;
    ctx.stroke();

    if (fillTop) {
      const grad = ctx.createLinearGradient(0, padT, 0, padT + plotH);
      grad.addColorStop(0, fillTop);
      grad.addColorStop(1, 'rgba(6, 9, 17, 0.0)');
      ctx.lineTo(xFor(history.length - 1), padT + plotH);
      ctx.lineTo(xFor(0), padT + plotH);
      ctx.closePath();
      ctx.fillStyle = grad;
      ctx.fill();
    }
  };

  // 1. Total Equity Curve (Neon Cyan)
  drawSeries(
    (p) => Number(p.total_equity_usdc || 10000),
    '#00f2fe',
    'rgba(0, 242, 254, 0.22)'
  );
  // 2. Committed Notional Curve (Neon Emerald)
  drawSeries(
    (p) => Number(p.committed_notional_usdc || 0),
    '#10b981',
    'rgba(16, 185, 129, 0.16)'
  );
  // 3. Reserved Working Order Margin (Neon Amber)
  drawSeries(
    (p) => Number(p.reserved_order_cash_usdc || 0),
    '#f59e0b',
    null
  );
}

function drawSleeveAttributionCanvas(dash) {
  const prep = prepareCanvas('sleeve-attr-canvas');
  if (!prep) return;
  const { ctx, w, h } = prep;
  ctx.clearRect(0, 0, w, h);

  const snap = (dash && dash.snapshot) || {};
  const s1 = Math.abs(Number(snap.s1_gross_weight || 0.45));
  const s2 = Math.abs(Number(snap.s2_gross_weight || 0.40));
  const s3 = Math.abs(Number(snap.s3_gross_weight || 0.25));

  const bars = [
    { label: 'S1 CARRY-MOM', val: s1, budget: 0.45, color: '#00f2fe' },
    { label: 'S2 TREND-BRK', val: s2, budget: 0.40, color: '#10b981' },
    { label: 'S3 EVENT-REV', val: s3, budget: 0.25, color: '#f59e0b' },
  ];

  const padL = 108;
  const padR = 54;
  const rowH = Math.floor((h - 20) / 3);
  const maxW = Math.max(1.0, s1 + s2 + s3, 0.85);

  ctx.font = '700 11px "JetBrains Mono", monospace';

  bars.forEach((b, i) => {
    const yCenter = 16 + i * rowH + rowH / 2;
    ctx.fillStyle = '#cbd5e1';
    ctx.fillText(b.label, 8, yCenter + 4);

    const trackW = Math.max(20, w - padL - padR);
    const barH = 14;
    const barY = yCenter - barH / 2;

    // Background track
    ctx.fillStyle = 'rgba(255, 255, 255, 0.06)';
    ctx.fillRect(padL, barY, trackW, barH);

    // Active weight fill
    const fillW = Math.min(trackW, (b.val / maxW) * trackW);
    ctx.fillStyle = b.color;
    ctx.fillRect(padL, barY, Math.max(4, fillW), barH);

    ctx.fillStyle = '#ffffff';
    ctx.fillText(`${(b.val * 100).toFixed(1)}%`, padL + trackW + 8, yCenter + 4);
  });
}

/* ============================================================================
   DATA FETCHING & 1s/2s SSE STREAM WIRING
   ============================================================================ */

function applyDashboardPayload(dash, forceConfig = false) {
  if (!dash) return;
  state.dashboard = dash;
  renderHealthAndTopBar(state.health, dash);
  renderPnlHorizonRibbon(dash);
  renderKpiCards(dash);
  renderPriorityAndWorkingTable(dash);
  renderOpenHoldingsTable(dash);
  renderActiveExplorerTab(forceConfig);
  drawEquityCurveCanvas(dash);
  drawSleeveAttributionCanvas(dash);
}

async function refreshAll(forceConfig = false) {
  try {
    const [hRes, dRes] = await Promise.all([
      apiFetch('/api/v1/health'),
      apiFetch('/api/v1/dashboard'),
    ]);
    if (hRes.ok) {
      state.health = await hRes.json();
    }
    if (dRes.ok) {
      const dash = await dRes.json();
      applyDashboardPayload(dash, forceConfig);
    }
  } catch (err) {
    console.warn('Dashboard refresh warning:', err);
  }
}

function connectSSE() {
  if (state.sse) {
    try {
      state.sse.close();
    } catch (_) {}
  }
  try {
    const es = new EventSource('/api/v1/stream');
    state.sse = es;
    es.onmessage = (evt) => {
      try {
        const data = JSON.parse(evt.data);
        if (data && (data.snapshot || data.v_assets_held)) {
          applyDashboardPayload(data, false);
        }
      } catch (_) {}
    };
    es.onerror = () => {
      // Fallback polling handles transient reconnects cleanly
    };
  } catch (_) {}
}

window.addEventListener('resize', () => {
  if (state.dashboard) {
    drawEquityCurveCanvas(state.dashboard);
    drawSleeveAttributionCanvas(state.dashboard);
  }
});

document.addEventListener('keydown', (evt) => {
  if (evt.key === 'Escape') {
    closeConfigModal();
  }
});

document.addEventListener('DOMContentLoaded', async () => {
  // Attach delegated pointerdown listeners on surveillance tables so button presses trigger immediately before any 1.5s SSE DOM swap
  ['tbody-assets-held', 'tbody-priority-ordered'].forEach((tbodyId) => {
    const el = document.getElementById(tbodyId);
    if (!el) return;
    el.addEventListener('pointerdown', (evt) => {
      const btn = evt.target && evt.target.closest ? evt.target.closest('.close-btn[data-close-pos-id]') : null;
      if (!btn || btn.disabled) return;
      evt.preventDefault();
      const pid = Number(btn.getAttribute('data-close-pos-id') || 0);
      const sym = btn.getAttribute('data-close-symbol') || '';
      const applyCd = btn.getAttribute('data-close-cooldown') !== 'false';
      closePositionOrOrder(pid, sym, applyCd);
    });
  });

  await refreshAll(true);
  connectSSE();
  // 2.5-second fallback sync
  setInterval(() => refreshAll(false), 2500);
});
