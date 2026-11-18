/* ============================================================================
   AEGIS QUANT v2.0 — REAL-TIME MULTI-SLEEVE TERMINAL APPLICATION (app.js)
   Backed by MariaDB 13.0 InnoDB, 7 SQL Views & 1s/2s SSE Stream
   ============================================================================ */

const state = {
  health: null,
  dashboard: null,
  activeTab: 's1',
  cooldowns: [],
  lastTickMs: Date.now(),
  sse: null,
};

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

function fmtPrice(px) {
  const n = Number(px || 0);
  if (n === 0) return '$0.00';
  if (n >= 1000) return fmtUsd(n, 2);
  if (n >= 1) return fmtUsd(n, 4);
  return '$' + n.toFixed(6);
}

function fmtPct(fracOrPct, isFraction = true, decimals = 1) {
  const v = Number(fracOrPct || 0) * (isFraction ? 100.0 : 1.0);
  const sign = v > 0 ? '+' : '';
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
   INTERACTIVE ACTIONS (RISK TIER, TELEGRAM TEST, CLOSE POSITION, CONFIG)
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
    await refreshAll();
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

async function closePositionOrOrder(positionId, symbol, applyCooldown = true) {
  try {
    const res = await apiFetch('/api/v1/positions/close', {
      method: 'POST',
      body: JSON.stringify({
        position_id: Number(positionId),
        symbol: String(symbol || ''),
        cooldown_sec: applyCooldown ? 1800 : 0,
        reason: 'WEB_UI_OPERATOR_CLOSE',
      }),
    });
    const data = await res.json();
    if (!res.ok) {
      showToast(`Close failed: ${data.detail || data.error || res.status}`, true);
      return;
    }
    showToast(
      `Position/Order #${positionId} (${symbol}) closed (${data.status || 'CLOSED'})`
    );
    await refreshAll();
  } catch (err) {
    showToast(`Error closing position: ${err.message}`, true);
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
  try {
    const res = await apiFetch('/api/v1/config', {
      method: 'POST',
      body: JSON.stringify({ config_key: key, config_value: val }),
    });
    const data = await res.json();
    if (!res.ok) {
      showToast(`Config update failed: ${data.detail || data.error || res.status}`, true);
      return;
    }
    showToast(`Updated system_config['${key}']`);
    if (valEl) valEl.value = '';
    await refreshAll();
  } catch (err) {
    showToast(`Config update error: ${err.message}`, true);
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
  renderActiveExplorerTab();
}

function updateRiskTierButtons(activeTier) {
  const norm = String(activeTier || 'extreme').toLowerCase();
  document.querySelectorAll('.tier-btn').forEach((btn) => {
    btn.classList.toggle('active', btn.getAttribute('data-tier') === norm);
  });
}

/* ============================================================================
   TOP TELEMETRY, COOLDOWNS, KPI CARDS & TABLES RENDERERS
   ============================================================================ */

function renderHealthAndTopBar(health, dash) {
  const dbVer =
    (health && health.mariadb_version) ||
    (dash && dash.mariadb_version) ||
    '13.0.2-MariaDB';
  const viewsCount =
    (health && health.views_healthy_count) ||
    (dash && dash.views_healthy_count) ||
    7;
  const dbText = document.getElementById('mariadb-version-text');
  if (dbText) {
    dbText.textContent = `${dbVer} · ${viewsCount}/7 Views OK`;
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

function renderCooldownStrip(cooldowns) {
  state.cooldowns = (cooldowns || []).map((c) => ({
    ...c,
    local_fetched_ms: Date.now(),
  }));
  paintCooldownTimers();
}

function paintCooldownTimers() {
  const countEl = document.getElementById('cooldown-count');
  const container = document.getElementById('cooldown-pills-container');
  if (!container) return;

  const now = Date.now();
  const active = (state.cooldowns || []).filter((c) => {
    const exp = Number(c.expires_at_ms || 0);
    return exp > now || Number(c.remaining_seconds || 0) > 0;
  });

  if (countEl) countEl.textContent = String(active.length);

  if (active.length === 0) {
    container.innerHTML =
      '<span style="color: var(--text-muted); font-family: var(--font-mono); font-size: 11px;">No assets currently in post-stop / shock cooldown — all eligible perps unlocked for entry.</span>';
    return;
  }

  container.innerHTML = active
    .map((c) => {
      const remSec = c.expires_at_ms
        ? Math.max(0, Math.round((Number(c.expires_at_ms) - now) / 1000))
        : Math.max(0, Number(c.remaining_seconds || 0));
      const mins = Math.floor(remSec / 60);
      const secs = remSec % 60;
      const timerStr = `${mins}m ${String(secs).padStart(2, '0')}s`;
      return `
        <div class="cooldown-pill" title="${escapeHtml(c.reason || '')}">
          <span class="cd-sym">${escapeHtml(c.symbol)}</span>
          <span class="cd-type">${escapeHtml(c.cooldown_type)}</span>
          <span class="cd-timer">${timerStr}</span>
        </div>
      `;
    })
    .join('');
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

function renderPriorityAndWorkingTable(dash) {
  const tbody = document.getElementById('tbody-priority-ordered');
  if (!tbody) return;

  const ordered = (dash && dash.v_assets_ordered) || [];
  const prio = (dash && dash.v_priority_watchlist) || [];

  const orderedByAsset = new Map();
  ordered.forEach((o) => orderedByAsset.set(o.asset_id, o));

  const rowsHtml = [];

  // Render active WORKING orders first (from v_assets_ordered)
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
          <button type="button" class="close-btn" onclick="closePositionOrOrder(${o.position_id}, '${escapeHtml(o.symbol)}', false)">Cancel</button>
        </td>
      </tr>
    `);
  });

  // Render additional priority watched assets (from v_priority_watchlist) not already in WORKING rows
  prio.forEach((p) => {
    if (orderedByAsset.has(p.asset_id)) return;
    const statusLabel = p.position_status || p.event_trigger || 'PRIORITY_WATCH';
    const sideHtml = p.position_side
      ? `<span class="side-pill ${String(p.position_side).toUpperCase() === 'SHORT' ? 'short' : 'long'}">${escapeHtml(p.position_side)}</span>`
      : '<span class="pnl-flat">—</span>';
    rowsHtml.push(`
      <tr>
        <td><span class="sym-badge">${escapeHtml(p.symbol)}</span></td>
        <td><span class="status-pill watch">${escapeHtml(statusLabel)}</span></td>
        <td>${sideHtml}</td>
        <td>${fmtPrice(p.entry_price || p.mark_price)} <small style="color:var(--text-muted)">/ ${fmtPrice(p.mark_price)}</small></td>
        <td>0.00 bps</td>
        <td>${fmtUsd(p.reserved_margin_usdc || p.committed_notional_usdc || 0)}</td>
        <td><span class="tab-count">1s</span></td>
        <td>
          ${
            p.position_id
              ? `<button type="button" class="close-btn" onclick="closePositionOrOrder(${p.position_id}, '${escapeHtml(p.symbol)}', true)">Close</button>`
              : '<span class="pnl-flat">Watching</span>'
          }
        </td>
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

function renderOpenHoldingsTable(dash) {
  const tbody = document.getElementById('tbody-assets-held');
  const pill = document.getElementById('held-count-pill');
  if (!tbody) return;

  const held = (dash && dash.v_assets_held) || [];
  if (pill) pill.textContent = `${held.length} OPEN`;

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
          <td class="${pnlCls}">${upnl >= 0 ? '+' : ''}${fmtUsd(upnl)}</td>
          <td>
            <div class="sleeve-mix-pills">
              <span class="sp-tag s1" title="Sleeve 1 Carry-Momentum">S1 ${fmtPct(h.s1_weight_contrib || 0)}</span>
              <span class="sp-tag s2" title="Sleeve 2 Trend-Breakout">S2 ${fmtPct(h.s2_weight_contrib || 0)}</span>
              <span class="sp-tag s3" title="Sleeve 3 Event-Reversion">S3 ${fmtPct(h.s3_weight_contrib || 0)}</span>
            </div>
          </td>
          <td>
            <button type="button" class="close-btn" onclick="closePositionOrOrder(${h.position_id}, '${escapeHtml(h.symbol)}', true)">Close</button>
          </td>
        </tr>
      `;
    })
    .join('');
}

function renderActiveExplorerTab() {
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
  const configs = dash.system_config || [];

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

  // Render System Config Vault
  const tbCfg = document.getElementById('tbody-system-config');
  if (tbCfg) {
    tbCfg.innerHTML = configs
      .filter(
        (c) =>
          !query ||
          String(c.config_key || '')
            .toUpperCase()
            .includes(query)
      )
      .map(
        (c) => `
        <tr>
          <td><strong style="color:var(--neon-cyan)">${escapeHtml(c.config_key)}</strong></td>
          <td><code>${escapeHtml(c.config_value)}</code></td>
          <td><span class="tab-count">${escapeHtml(c.value_type)}</span></td>
          <td style="color:var(--text-muted)">${escapeHtml(c.description)}</td>
          <td>${fmtUtcTime(c.updated_at_ms)}</td>
        </tr>
      `
      )
      .join('');
  }
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

function applyDashboardPayload(dash) {
  if (!dash) return;
  state.dashboard = dash;
  renderHealthAndTopBar(state.health, dash);
  renderCooldownStrip(dash.v_assets_in_cooldown || []);
  renderKpiCards(dash);
  renderPriorityAndWorkingTable(dash);
  renderOpenHoldingsTable(dash);
  renderActiveExplorerTab();
  drawEquityCurveCanvas(dash);
  drawSleeveAttributionCanvas(dash);
}

async function refreshAll() {
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
      applyDashboardPayload(dash);
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
          applyDashboardPayload(data);
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

document.addEventListener('DOMContentLoaded', async () => {
  await refreshAll();
  connectSSE();
  // 1-second local countdown timer tick for active cooldowns
  setInterval(paintCooldownTimers, 1000);
  // 2-second fallback sync
  setInterval(refreshAll, 2500);
});
