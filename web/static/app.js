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
  authRequired: false,
  equityChartPoints: [],
};

const IMMUTABLE_SECURITY_KEYS = new Set([
  'enforce_master_key',
  'web_auth_enabled',
  'web_require_auth',
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
  'hyperliquid_testnet',
  'risk_capital_cap_updated_at_ms',
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

function getLocalTzAbbrev(dateObj) {
  try {
    const parts = new Intl.DateTimeFormat(undefined, { timeZoneName: 'short' }).formatToParts(dateObj);
    const tzPart = parts.find((p) => p.type === 'timeZoneName');
    if (tzPart && tzPart.value) return tzPart.value;
  } catch (_) {}
  const offsetMin = -dateObj.getTimezoneOffset();
  const sign = offsetMin >= 0 ? '+' : '-';
  const absMin = Math.abs(offsetMin);
  const hh = String(Math.floor(absMin / 60)).padStart(2, '0');
  const mm = String(absMin % 60).padStart(2, '0');
  return mm === '00' ? `GMT${sign}${hh}` : `GMT${sign}${hh}:${mm}`;
}

function fmtLocalTime(tsMs) {
  if (!tsMs || tsMs <= 0) return '—';
  const d = new Date(Number(tsMs));
  if (Number.isNaN(d.getTime())) return '—';
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const hh = String(d.getHours()).padStart(2, '0');
  const mi = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  const tz = getLocalTzAbbrev(d);
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss} ${tz}`;
}

function fmtLocalShortTime(tsMs) {
  if (!tsMs || tsMs <= 0) return '';
  const d = new Date(Number(tsMs));
  if (Number.isNaN(d.getTime())) return '';
  const hh = String(d.getHours()).padStart(2, '0');
  const mi = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return `${hh}:${mi}:${ss}`;
}

const fmtUtcTime = fmtLocalTime;

function openAuthModal(errMsg = '') {
  const modal = document.getElementById('auth-login-modal');
  const errEl = document.getElementById('auth-login-error');
  const inp = document.getElementById('auth-token-input');
  if (errEl) {
    if (errMsg) {
      errEl.textContent = errMsg;
      errEl.style.display = 'block';
    } else {
      errEl.textContent = '';
      errEl.style.display = 'none';
    }
  }
  if (modal && !modal.classList.contains('open')) {
    modal.classList.add('open');
    if (inp) setTimeout(() => inp.focus(), 40);
  }
}

function closeAuthModal() {
  const modal = document.getElementById('auth-login-modal');
  const errEl = document.getElementById('auth-login-error');
  const inp = document.getElementById('auth-token-input');
  if (modal) modal.classList.remove('open');
  if (errEl) {
    errEl.textContent = '';
    errEl.style.display = 'none';
  }
  if (inp) inp.value = '';
}

async function submitAuthLogin(evt) {
  evt.preventDefault();
  const inp = document.getElementById('auth-token-input');
  const btn = document.getElementById('btn-auth-login-submit');
  const token = inp ? inp.value.trim() : '';
  if (!token) {
    openAuthModal('Please enter your Web API Bearer Token.');
    return;
  }
  if (btn) btn.disabled = true;
  try {
    const res = await apiFetch('/api/v1/auth/login', {
      method: 'POST',
      body: JSON.stringify({ token }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      openAuthModal(data.detail || data.error || 'Invalid API token');
      return;
    }
    state.authRequired = false;
    closeAuthModal();
    showToast('Authenticated — connected to live terminal');
    await refreshAll(true);
    connectSSE();
  } catch (err) {
    openAuthModal(`Authentication request error: ${err.message}`);
  } finally {
    if (btn) btn.disabled = false;
  }
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
  const res = await fetch(url, {
    cache: 'no-store',
    credentials: 'same-origin',
    ...options,
    headers,
  });
  if (res.status === 401 && !String(url).includes('/api/v1/auth/login')) {
    state.authRequired = true;
    if (state.sse) {
      try {
        state.sse.close();
      } catch (_) {}
      state.sse = null;
    }
    openAuthModal();
  }
  return res;
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

  const confirmMsg = applyCooldown
    ? `Close open position ${sym || '#' + pid} at market (with cooldown)?`
    : `Cancel working order ${sym || '#' + pid}?`;
  if (!window.confirm(confirmMsg)) {
    return;
  }

  if (pid > 0) state.closingPositionIds.add(pid);
  if (sym) state.closingSymbols.add(sym);
  state.closeInFlightCount += 1;

  // Immediate visual feedback on button while request is in flight
  document
    .querySelectorAll(`.close-btn[data-position-id="${pid}"]`)
    .forEach((btn) => {
      btn.disabled = true;
      btn.textContent = 'Closing...';
    });

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
    if (data.new_status === 'OPEN') {
      showToast(
        `Close order for ${data.symbol || sym} (#${data.position_id || pid}) did not fill; position remains OPEN`,
        true
      );
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
    if (pid > 0) state.closingPositionIds.delete(pid);
    if (sym) state.closingSymbols.delete(sym);
    await refreshAll(true);
  }
}

async function closeAllOpenPositions() {
  if (!window.confirm('Close ALL open positions and cancel ALL working orders?')) {
    return;
  }
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
      `Closed ${Number(data.closed_count ?? 0)} position(s)/order(s) (Realized PnL: ${fmtSignedUsd(
        data.realized_pnl_usdc ?? 0
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
    k === 'max_open_positions' ||
    k === 'min_entry_interval_sec' ||
    k === 'min_position_hold_sec' ||
    k.includes('risk_tier') ||
    k.includes('risk_capital') ||
    k.includes('leverage') ||
    k.includes('weight') ||
    k.includes('stop') ||
    k.includes('cooldown') ||
    k.includes('drawdown') ||
    k.includes('spread') ||
    k.includes('equity') ||
    k.includes('max_open')
  ) {
    return 'risk';
  }
  if (
    k.includes('entry_interval') ||
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
    (acc, h) => acc + Number(h.unrealized_pnl_usdc ?? 0),
    0
  );
  const totalEq = Number(dash?.snapshot?.total_equity_usdc ?? 0);

  const paintHorizon = (key, defaultObj) => {
    const h = summary[key] || defaultObj;
    const netPnl = Number(h.net_pnl_usdc ?? fallbackUpnl);
    const retPct = Number(
      h.return_pct ?? (totalEq > 0 ? (netPnl / totalEq) * 100.0 : 0.0)
    );
    const rpnl = Number(h.realized_pnl_usdc ?? 0);
    const upnl = Number(h.unrealized_pnl_usdc ?? fallbackUpnl);
    const trades = Number(h.trade_count ?? 0);
    const wins = Number(h.win_count ?? 0);

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

function getConfigFloatFromDash(dash, key, fallback) {
  const row = (dash?.system_config || []).find((r) => r.config_key === key);
  if (!row) return fallback;
  const n = Number(row.config_value);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function renderKpiCards(dash) {
  const snap = (dash && dash.snapshot) || {};
  const totalEq = Number(snap.total_equity_usdc ?? 0);
  const availCash = Number(snap.available_cash_usdc ?? totalEq);
  const reservedMargin = Number(snap.reserved_order_cash_usdc ?? 0.0);
  const committedNotional = Number(snap.committed_notional_usdc ?? 0.0);
  const netNotional = Number(snap.net_notional_usdc ?? 0.0);
  const grossLev = Number(snap.gross_leverage_x ?? 0.0);
  const netLev = Number(snap.net_leverage_x ?? 0.0);
  const tier = String(snap.risk_tier || dash.risk_tier || 'extreme').toUpperCase();
  const mult = Number(
    snap.leverage_multiplier ?? dash.target_leverage_multiplier ?? 1.85
  );
  const breadth = Number(snap.market_breadth_bull ?? 0.5);
  const beta = Number(snap.market_beta_exposure ?? 0.0);
  const regime = String(snap.market_regime_label || 'NEUTRAL');
  const maxGrossCap = getConfigFloatFromDash(dash, 'max_gross_leverage_x', 2.5);

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
    `${Number(snap.eligible_coin_count ?? (dash.exchange_assets || []).length)} Eligible Perps`
  );

  const cashPct = totalEq > 0 ? (availCash / totalEq) * 100.0 : 0.0;
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
    `${Number(snap.open_long_count ?? 0)} Long · ${Number(
      snap.open_short_count ?? 0
    )} Short`
  );
  setTxt('kpi-net-notional', `Net: ${fmtUsd(netNotional)}`);
  setWidth('kpi-committed-meter', Math.min(100, commPct));

  setTxt('kpi-gross-leverage', `${grossLev.toFixed(2)}x`);
  setTxt('kpi-net-leverage', `${netLev >= 0 ? '+' : ''}${netLev.toFixed(2)}x`);
  setTxt('kpi-leverage-target', `Target: ${mult.toFixed(2)}x ${tier}`);
  setTxt('kpi-leverage-cap', `Cap: ${maxGrossCap.toFixed(2)}x`);
  setWidth('kpi-leverage-meter', (grossLev / Math.max(0.2, maxGrossCap)) * 100.0);

  setTxt('kpi-regime-label', regime);
  setTxt('kpi-breadth-pct', `Bull Breadth: ${(breadth * 100.0).toFixed(1)}%`);
  setTxt('kpi-beta-exp', `Beta: ${beta >= 0 ? '+' : ''}${beta.toFixed(2)}`);
  setWidth('kpi-breadth-meter', breadth * 100.0);

  // Sleeve attribution stat boxes
  setTxt('sleeve-s1-weight', fmtPct(snap.s1_gross_weight ?? 0, true, 1));
  setTxt('sleeve-s2-weight', fmtPct(snap.s2_gross_weight ?? 0, true, 1));
  setTxt('sleeve-s3-weight', fmtPct(snap.s3_gross_weight ?? 0, true, 1));
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
    const hasTpsl = Number(o.protective_stop_px || 0) > 0 || Number(o.take_profit_px || 0) > 0;
    const tpslHint = hasTpsl
      ? `<br><small style="color:var(--text-muted)">SL ${fmtPrice(o.protective_stop_px)} · TP ${fmtPrice(o.take_profit_px)}</small>`
      : '';
    rowsHtml.push(`
      <tr>
        <td><span class="sym-badge">${escapeHtml(o.symbol)}</span></td>
        <td><span class="status-pill working">${escapeHtml(o.fsm_state || 'WORKING_ALO')}</span></td>
        <td><span class="side-pill ${sideCls}">${escapeHtml(o.side)}</span></td>
        <td>${fmtPrice(o.limit_price)} <small style="color:var(--text-muted)">/ ${fmtPrice(o.mark_price)}</small>${tpslHint}</td>
        <td style="color: var(--neon-cyan); font-weight: 700;">${Number(o.distance_from_mid_bps || 0).toFixed(2)} bps</td>
        <td style="color: #fbbf24; font-weight: 700;">${fmtUsd(o.reserved_margin_usdc)}</td>
        <td><span class="tab-count">${Number(o.chase_count || 0)}</span></td>
        <td>
          <button
            type="button"
            class="close-btn"
            data-action="close-position"
            data-position-id="${Number(o.position_id)}"
            data-symbol="${escapeHtml(o.symbol)}"
            data-cooldown="false"
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

function isPreCapHolding(h, capEpochMs) {
  if (!(capEpochMs > 0)) return false;
  const rawTs = h?.opened_at_ms ?? h?.updated_at_ms;
  if (rawTs === null || rawTs === undefined) return false;
  const tsMs = Number(rawTs);
  return Number.isFinite(tsMs) && tsMs < capEpochMs;
}

function createTableSmall(cls, text) {
  const el = document.createElement('small');
  el.className = cls;
  el.textContent = text;
  return el;
}

function createSleeveTag(cls, title, text) {
  const el = document.createElement('span');
  el.className = `sp-tag ${cls}`;
  el.title = title;
  el.textContent = text;
  return el;
}

function renderOpenHoldingsTable(dash, forceRender = false) {
  const tbody = document.getElementById('tbody-assets-held');
  const pill = document.getElementById('held-count-pill');
  const closeAllBtn = document.getElementById('btn-close-all-open');
  if (!tbody) return;
  if (!forceRender && state.closeInFlightCount > 0) return;

  const rawHeld = (dash && dash.v_assets_held) || [];
  const held = rawHeld.filter(
    (h) =>
      !state.closingPositionIds.has(Number(h.position_id)) &&
      !state.closingSymbols.has(String(h.symbol))
  );
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
    : held.filter((h) => isPreCapHolding(h, capEpochMs)).length;
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
    const emptyRow = document.createElement('tr');
    const emptyCell = document.createElement('td');
    emptyCell.colSpan = 9;
    emptyCell.className = 'table-empty-cell';
    emptyCell.textContent =
      'No active OPEN holdings — capital remains in Available Cash until exchange fill confirmation.';
    emptyRow.appendChild(emptyCell);
    tbody.replaceChildren(emptyRow);
    return;
  }

  const rows = held.map((h) => {
    const isPreCap = isPreCapHolding(h, capEpochMs);
    const sideStr = String(h.side ?? '');
    const sideCls = sideStr.toUpperCase() === 'SHORT' ? 'short' : 'long';
    const upnl = Number(h.unrealized_pnl_usdc || 0);
    const pnlCls = upnl > 0.001 ? 'pnl-pos' : upnl < -0.001 ? 'pnl-neg' : 'pnl-flat';
    const stopDist =
      h.stop_distance_pct !== null && h.stop_distance_pct !== undefined
        ? `${Number(h.stop_distance_pct).toFixed(2)}%`
        : '—';
    const tpDist =
      h.tp_distance_pct !== null && h.tp_distance_pct !== undefined
        ? `${Number(h.tp_distance_pct).toFixed(2)}%`
        : '—';
    const slOid = h.protective_stop_oid ? String(h.protective_stop_oid).trim() : '';
    const tpOid = h.take_profit_oid ? String(h.take_profit_oid).trim() : '';

    const tr = document.createElement('tr');
    if (isPreCap) {
      tr.className = 'holding-pre-cap';
    }

    const tdSym = document.createElement('td');
    const symWrap = document.createElement('div');
    symWrap.className = 'holding-symbol-cell';
    const symBadge = document.createElement('span');
    symBadge.className = 'sym-badge';
    symBadge.textContent = String(h.symbol ?? '');
    symWrap.appendChild(symBadge);
    if (isPreCap) {
      const preCapBadge = document.createElement('span');
      preCapBadge.className = 'pre-cap-badge';
      preCapBadge.textContent = 'PRE-CAP';
      symWrap.appendChild(preCapBadge);
    }
    tdSym.appendChild(symWrap);

    const tdSide = document.createElement('td');
    const sidePill = document.createElement('span');
    sidePill.className = `side-pill ${sideCls}`;
    sidePill.textContent = sideStr;
    tdSide.appendChild(sidePill);

    const tdCommitted = document.createElement('td');
    tdCommitted.className = 'cell-committed';
    tdCommitted.append(
      document.createTextNode(`${fmtUsd(h.committed_notional_usdc)} `),
      createTableSmall('cell-muted-sub', `(${fmtPct(h.portfolio_weight || 0)})`)
    );

    const tdEntryMark = document.createElement('td');
    tdEntryMark.append(
      document.createTextNode(`${fmtPrice(h.entry_price)} `),
      createTableSmall('cell-mark-price', `→ ${fmtPrice(h.mark_price)}`)
    );

    const tdSl = document.createElement('td');
    const slBadge = createSleeveTag(
      slOid ? 's1' : 's3',
      slOid ? `Exchange SL Order: ${slOid}` : 'SL Order Pending',
      slOid ? 'SL' : 'SL?'
    );
    tdSl.append(
      document.createTextNode(`${fmtPrice(h.protective_stop_px)} `),
      createTableSmall('cell-sl-dist', `(${stopDist})`),
      document.createTextNode(' '),
      slBadge
    );

    const tdTp = document.createElement('td');
    const tpPxText = Number(h.take_profit_px || 0) > 0 ? fmtPrice(h.take_profit_px) : '—';
    const tpBadge = createSleeveTag(
      tpOid ? 's2' : 's3',
      tpOid ? `Exchange TP Order: ${tpOid}` : 'TP Order Pending',
      tpOid ? 'TP' : 'TP?'
    );
    tdTp.append(
      document.createTextNode(`${tpPxText} `),
      createTableSmall('cell-tp-dist', `(${tpDist})`),
      document.createTextNode(' '),
      tpBadge
    );

    const tdPnl = document.createElement('td');
    tdPnl.className = pnlCls;
    tdPnl.textContent = fmtSignedUsd(upnl);

    const tdSleeves = document.createElement('td');
    const sleeveDiv = document.createElement('div');
    sleeveDiv.className = 'sleeve-mix-pills';
    sleeveDiv.append(
      createSleeveTag('s1', 'Sleeve 1 Carry-Momentum', `S1 ${fmtPct(h.s1_weight_contrib || 0)}`),
      createSleeveTag('s2', 'Sleeve 2 Trend-Breakout', `S2 ${fmtPct(h.s2_weight_contrib || 0)}`),
      createSleeveTag('s3', 'Sleeve 3 Event-Reversion', `S3 ${fmtPct(h.s3_weight_contrib || 0)}`)
    );
    tdSleeves.appendChild(sleeveDiv);

    const tdAction = document.createElement('td');
    const closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'close-btn';
    closeBtn.dataset.action = 'close-position';
    closeBtn.dataset.positionId = String(Number(h.position_id));
    closeBtn.dataset.symbol = String(h.symbol ?? '');
    closeBtn.dataset.cooldown = 'true';
    closeBtn.textContent = 'Close';
    tdAction.appendChild(closeBtn);

    tr.append(
      tdSym,
      tdSide,
      tdCommitted,
      tdEntryMark,
      tdSl,
      tdTp,
      tdPnl,
      tdSleeves,
      tdAction
    );
    return tr;
  });

  tbody.replaceChildren(...rows);
}

function formatCooldownRemaining(secRaw) {
  const sec = Math.max(0, Math.floor(Number(secRaw ?? 0)));
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function renderCooldownsPanel(dash) {
  const listEl = document.getElementById('cooldown-badges-list');
  const pillEl = document.getElementById('cooldown-count-pill');
  if (!listEl) return;

  const cooldowns = (dash && dash.v_assets_in_cooldown) || [];
  if (pillEl) {
    pillEl.textContent = `${cooldowns.length} ACTIVE`;
  }

  if (cooldowns.length === 0) {
    listEl.innerHTML =
      '<span class="pnl-flat" style="font-family: var(--font-mono); font-size: 11px;">No assets currently in cooldown.</span>';
    return;
  }

  listEl.innerHTML = cooldowns
    .map((c) => {
      const rem = formatCooldownRemaining(c.remaining_seconds);
      const expStr = fmtLocalTime(c.expires_at_ms);
      return `
        <div class="t-badge pulse-3s" title="${escapeHtml(c.reason || '')}" style="gap: 8px; padding: 6px 10px;">
          <span class="sym-badge">${escapeHtml(c.symbol)}</span>
          <span class="sp-tag s3">${escapeHtml(c.cooldown_type || 'COOLDOWN')}</span>
          <span style="color: #fbbf24;">${escapeHtml(rem)} left</span>
          <span style="color: var(--text-muted); font-size: 10px;">until ${escapeHtml(expStr)}</span>
        </div>
      `;
    })
    .join('');
}

function buildInlineConfigControlHtml(row, idPrefix, rowIdx = 0) {
  const key = String(row.config_key || '');
  const val = String(row.config_value ?? '');
  const vtype = String(row.value_type || 'string').toLowerCase();
  const safeSlug = key.replace(/[^a-zA-Z0-9_-]/g, '_');
  const inputId = `${idPrefix}-cfg-input-${rowIdx}-${safeSlug}`;

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
        <select id="${escapeHtml(inputId)}" class="cfg-inline-select">${optsHtml}</select>
        <button
          type="button"
          class="cfg-save-btn"
          data-action="save-config"
          data-key="${escapeHtml(key)}"
          data-input-id="${escapeHtml(inputId)}"
        >Save</button>
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
          id="${escapeHtml(inputId)}"
          class="cfg-inline-input"
          data-is-secret="true"
          data-action="save-config-enter"
          data-key="${escapeHtml(key)}"
          data-input-id="${escapeHtml(inputId)}"
          placeholder="Enter new secret to overwrite..."
        />
        <button
          type="button"
          class="cfg-save-btn"
          data-action="save-config"
          data-key="${escapeHtml(key)}"
          data-input-id="${escapeHtml(inputId)}"
        >Save</button>
      </div>
    `;
  }

  const prefill = val === '[UNSET]' || val.includes('...') ? '' : val;
  const placeholder = val.includes('...') ? `${val} (enter full value)` : 'Enter value...';
  return `
    <div class="cfg-inline-controls">
      <input
        type="text"
        id="${escapeHtml(inputId)}"
        class="cfg-inline-input"
        data-action="save-config-enter"
        data-key="${escapeHtml(key)}"
        data-input-id="${escapeHtml(inputId)}"
        value="${escapeHtml(prefill)}"
        placeholder="${escapeHtml(placeholder)}"
      />
      <button
        type="button"
        class="cfg-save-btn"
        data-action="save-config"
        data-key="${escapeHtml(key)}"
        data-input-id="${escapeHtml(inputId)}"
      >Save</button>
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

  const PRIORITY_CONFIG_KEYS = [
    'live_exchange_equity_usdc',
    'risk_tier',
    'target_leverage_multiplier',
    'max_open_positions',
    'min_entry_interval_sec',
    'max_gross_leverage_x',
    'max_single_asset_weight',
    'execution_mode',
  ];

  const filterRows = (q) =>
    configs
      .filter((c) => {
        const k = String(c.config_key || '').toLowerCase();
        const isDualRiskExec =
          k === 'max_open_positions' || k === 'min_entry_interval_sec';
        if (
          cat !== 'all' &&
          classifyConfigKeyCategory(c.config_key) !== cat &&
          !(isDualRiskExec && (cat === 'risk' || cat === 'execution'))
        ) {
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
      })
      .sort((a, b) => {
        const ia = PRIORITY_CONFIG_KEYS.indexOf(String(a.config_key || ''));
        const ib = PRIORITY_CONFIG_KEYS.indexOf(String(b.config_key || ''));
        if (ia !== -1 && ib !== -1) return ia - ib;
        if (ia !== -1) return -1;
        if (ib !== -1) return 1;
        return String(a.config_key || '').localeCompare(String(b.config_key || ''));
      });

  const renderRows = (rows, prefix) =>
    rows
      .map(
        (c, idx) => `
        <tr>
          <td><strong style="color:var(--neon-cyan)">${escapeHtml(c.config_key)}</strong></td>
          <td><code>${escapeHtml(c.config_value)}</code></td>
          <td>${buildInlineConfigControlHtml(c, prefix, idx)}</td>
          <td><span class="tab-count">${escapeHtml(c.value_type)}</span></td>
          <td style="color:var(--text-muted); white-space: normal; max-width: 340px;">${escapeHtml(c.description)}</td>
          <td>${fmtLocalTime(c.updated_at_ms)}</td>
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
              <td>${Number(r.market_beta ?? 1).toFixed(2)}</td>
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
              <td style="color:var(--neon-cyan);font-weight:700;">${Number(r.s2_vol_surge_ratio ?? 1).toFixed(2)}x</td>
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
              <td>${Number(a.max_leverage ?? 20).toFixed(0)}x</td>
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
              <td>${fmtLocalTime(a.created_at_ms)}</td>
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
                <span>${fmtLocalTime(l.created_at_ms)}</span>
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
      { ...history[0], created_at_ms: Number(history[0].created_at_ms ?? Date.now()) - 60000 },
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
    vals.push(Number(pt.total_equity_usdc ?? 0));
    vals.push(Number(pt.committed_notional_usdc ?? 0));
    vals.push(Number(pt.reserved_order_cash_usdc ?? 0));
  });
  const minVal = 0;
  const rawMax = Math.max(0, ...vals);
  const maxVal = rawMax > 0 ? rawMax * 1.08 : 100;

  // Grid lines & Y-axis labels
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
    const labelStr =
      maxVal < 500
        ? '$' + labelVal.toFixed(2)
        : '$' + Math.round(labelVal).toLocaleString();
    ctx.fillText(labelStr, 6, y + 3);
  }

  const xFor = (idx) =>
    padL + (history.length <= 1 ? 0 : (idx / (history.length - 1)) * plotW);
  const yFor = (v) =>
    padT + plotH - ((v - minVal) / Math.max(1e-6, maxVal - minVal)) * plotH;

  // Store point coordinates for local-time hover tooltip
  state.equityChartPoints = history.map((pt, idx) => ({
    x: xFor(idx),
    y: yFor(Number(pt.total_equity_usdc ?? 0)),
    pt,
  }));

  // Local-timezone X-axis timestamps (start, mid, end)
  if (history.length > 0) {
    const indices =
      history.length >= 3
        ? [0, Math.floor((history.length - 1) / 2), history.length - 1]
        : [0, history.length - 1];
    indices.forEach((idx, posIdx) => {
      const pt = history[idx];
      if (!pt || !pt.created_at_ms) return;
      const baseTime = fmtLocalShortTime(pt.created_at_ms);
      if (!baseTime) return;
      const tzSuffix =
        posIdx === indices.length - 1
          ? ` ${getLocalTzAbbrev(new Date(Number(pt.created_at_ms)))}`
          : '';
      const txt = baseTime + tzSuffix;
      const x = xFor(idx);
      ctx.textAlign =
        posIdx === 0 ? 'left' : posIdx === indices.length - 1 ? 'right' : 'center';
      ctx.fillText(txt, x, padT + plotH + 17);
    });
    ctx.textAlign = 'left';
  }

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
    (p) => Number(p.total_equity_usdc ?? 0),
    '#00f2fe',
    'rgba(0, 242, 254, 0.22)'
  );
  // 2. Committed Notional Curve (Neon Emerald)
  drawSeries(
    (p) => Number(p.committed_notional_usdc ?? 0),
    '#10b981',
    'rgba(16, 185, 129, 0.16)'
  );
  // 3. Reserved Working Order Margin (Neon Amber)
  drawSeries(
    (p) => Number(p.reserved_order_cash_usdc ?? 0),
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
  const rawS1 = Number(snap.s1_gross_weight ?? 0);
  const rawS2 = Number(snap.s2_gross_weight ?? 0);
  const rawS3 = Number(snap.s3_gross_weight ?? 0);
  const s1 = Number.isFinite(rawS1) ? Math.abs(rawS1) : 0;
  const s2 = Number.isFinite(rawS2) ? Math.abs(rawS2) : 0;
  const s3 = Number.isFinite(rawS3) ? Math.abs(rawS3) : 0;

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

    // Active weight fill (0.0 renders 0 width, never fake fallback bars)
    const fillW = b.val > 0 ? Math.min(trackW, (b.val / maxW) * trackW) : 0;
    if (fillW > 0) {
      ctx.fillStyle = b.color;
      ctx.fillRect(padL, barY, Math.max(3, fillW), barH);
    }

    ctx.fillStyle = '#ffffff';
    ctx.fillText(`${(b.val * 100).toFixed(1)}%`, padL + trackW + 8, yCenter + 4);
  });
}

/* ============================================================================
   DATA FETCHING & 1.5s SSE STREAM WIRING
   ============================================================================ */

function applyDashboardPayload(dash, forceConfig = false) {
  if (!dash) return;
  state.dashboard = dash;
  renderHealthAndTopBar(state.health, dash);
  renderPnlHorizonRibbon(dash);
  renderKpiCards(dash);
  renderPriorityAndWorkingTable(dash);
  renderOpenHoldingsTable(dash);
  renderCooldownsPanel(dash);
  renderActiveExplorerTab(forceConfig);
  drawEquityCurveCanvas(dash);
  drawSleeveAttributionCanvas(dash);
}

async function refreshAll(forceConfig = false) {
  if (state.authRequired) return;
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
  if (state.authRequired) return;
  if (state.sse) {
    try {
      state.sse.close();
    } catch (_) {}
    state.sse = null;
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
    es.onerror = async () => {
      if (state.sse === es && es.readyState === EventSource.CLOSED && !state.authRequired) {
        // Probe once to surface 401 prompt if auth became required
        try {
          await apiFetch('/api/v1/dashboard');
        } catch (_) {}
      }
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
    return;
  }
  if (evt.key === 'Enter') {
    const target = evt.target;
    if (
      target &&
      target.matches &&
      target.matches('.cfg-inline-input[data-action="save-config-enter"]')
    ) {
      evt.preventDefault();
      const key = target.getAttribute('data-key') || '';
      const inputId = target.getAttribute('data-input-id') || target.id;
      if (key && inputId) {
        saveSingleConfigRow(key, inputId);
      }
    }
  }
});

document.addEventListener('click', (evt) => {
  const target = evt.target;
  if (!target || !target.closest) return;

  // Backdrop close on config modal
  if (target.id === 'config-settings-modal') {
    closeConfigModal();
    return;
  }

  const tierBtn = target.closest('.tier-btn[data-tier]');
  if (tierBtn) {
    switchRiskTier(tierBtn.getAttribute('data-tier'));
    return;
  }

  if (target.closest('#btn-sync-hyperliquid')) {
    triggerHyperliquidReconcile();
    return;
  }
  if (target.closest('#btn-telegram-test')) {
    triggerTelegramTestAlert();
    return;
  }
  if (target.closest('#btn-open-config-modal')) {
    openConfigModal();
    return;
  }
  if (target.closest('#btn-close-config-modal')) {
    closeConfigModal();
    return;
  }
  if (target.closest('#btn-toggle-watch-all')) {
    toggleShowAllPriorityWatch();
    return;
  }
  if (target.closest('#btn-close-all-open')) {
    closeAllOpenPositions();
    return;
  }
  if (target.closest('#btn-submit-config-update')) {
    submitConfigUpdate();
    return;
  }

  const tabBtn = target.closest('.tab-btn[data-tab]');
  if (tabBtn) {
    switchExplorerTab(tabBtn.getAttribute('data-tab'));
    return;
  }

  const catBtn = target.closest('.cfg-cat-btn[data-cat]');
  if (catBtn) {
    setConfigCategory(catBtn.getAttribute('data-cat'));
    return;
  }

  const closeBtn = target.closest('.close-btn[data-action="close-position"]');
  if (closeBtn && !closeBtn.disabled) {
    const pid = Number(closeBtn.getAttribute('data-position-id') || 0);
    const sym = closeBtn.getAttribute('data-symbol') || '';
    const applyCd = closeBtn.getAttribute('data-cooldown') !== 'false';
    closePositionOrOrder(pid, sym, applyCd);
    return;
  }

  const saveCfgBtn = target.closest('.cfg-save-btn[data-action="save-config"]');
  if (saveCfgBtn) {
    const key = saveCfgBtn.getAttribute('data-key') || '';
    const inputId = saveCfgBtn.getAttribute('data-input-id') || '';
    if (key && inputId) {
      saveSingleConfigRow(key, inputId);
    }
  }
});

document.addEventListener('DOMContentLoaded', async () => {
  const scannerSearch = document.getElementById('scanner-search-input');
  if (scannerSearch) {
    scannerSearch.addEventListener('input', () => renderActiveExplorerTab());
  }
  const modalSearch = document.getElementById('modal-cfg-search');
  if (modalSearch) {
    modalSearch.addEventListener('input', () => renderSystemConfigTables());
  }
  const authForm = document.getElementById('auth-login-form');
  if (authForm) {
    authForm.addEventListener('submit', submitAuthLogin);
  }

  // Interactive local-timezone hover tooltip on Equity Curve canvas
  const eqCanvas = document.getElementById('equity-curve-canvas');
  const eqTooltip = document.getElementById('equity-chart-tooltip');
  if (eqCanvas && eqTooltip) {
    eqCanvas.addEventListener('mousemove', (evt) => {
      const pts = state.equityChartPoints || [];
      if (pts.length === 0) return;
      const rect = eqCanvas.getBoundingClientRect();
      const mouseX = evt.clientX - rect.left;
      let nearest = pts[0];
      let bestDist = Math.abs(mouseX - nearest.x);
      for (let i = 1; i < pts.length; i++) {
        const d = Math.abs(mouseX - pts[i].x);
        if (d < bestDist) {
          bestDist = d;
          nearest = pts[i];
        }
      }
      const pt = nearest.pt;
      eqTooltip.innerHTML = `
        <div style="color: var(--neon-cyan); font-weight: 700; margin-bottom: 3px;">${escapeHtml(fmtLocalTime(pt.created_at_ms))}</div>
        <div>Equity: <strong>${escapeHtml(fmtUsd(pt.total_equity_usdc ?? 0))}</strong></div>
        <div style="color: #34d399;">Committed: ${escapeHtml(fmtUsd(pt.committed_notional_usdc ?? 0))}</div>
        <div style="color: #fbbf24;">Reserved: ${escapeHtml(fmtUsd(pt.reserved_order_cash_usdc ?? 0))}</div>
      `;
      eqTooltip.style.display = 'block';
      const tipX = Math.min(rect.width - 170, Math.max(8, nearest.x + 12));
      const tipY = Math.max(8, Math.min(rect.height - 80, nearest.y - 24));
      eqTooltip.style.left = `${tipX}px`;
      eqTooltip.style.top = `${tipY}px`;
    });
    eqCanvas.addEventListener('mouseleave', () => {
      eqTooltip.style.display = 'none';
    });
  }

  await refreshAll(true);
  connectSSE();

  // 2.5-second fallback poll ONLY when EventSource SSE is not OPEN
  setInterval(() => {
    if (state.authRequired) return;
    if (state.sse && state.sse.readyState === EventSource.OPEN) return;
    refreshAll(false);
  }, 2500);
});
