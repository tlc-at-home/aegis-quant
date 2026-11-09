const fmtUsd = (n) => '$' + Number(n || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtNum = (n, d = 4) => Number(n || 0).toFixed(d);
const pad2 = (n) => String(n).padStart(2, '0');

function escapeHtml(raw) {
  if (raw === null || raw === undefined) return '';
  return String(raw)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function resolveApiAuthToken() {
  return '';
}

function apiFetch(url, options = {}) {
  const headers = new Headers(options.headers || {});
  const token = resolveApiAuthToken();
  if (token && !headers.has('X-Aegis-Token') && !headers.has('Authorization')) {
    headers.set('X-Aegis-Token', token);
    headers.set('Authorization', `Bearer ${token}`);
  }
  if (!headers.has('X-Requested-With')) {
    headers.set('X-Requested-With', 'XMLHttpRequest');
  }
  if (!headers.has('X-CSRF-Token')) {
    headers.set('X-CSRF-Token', token || 'aegis-same-origin');
  }
  return fetch(url, { cache: 'no-store', ...options, credentials: 'same-origin', headers }).then((res) => {
    if (res.status === 401) {
      const authPanel = document.getElementById('auth-login-panel');
      if (authPanel) authPanel.style.display = 'flex';
    }
    return res;
  });
}

async function submitAuthLogin() {
  const inputEl = document.getElementById('auth-token-input');
  const statusEl = document.getElementById('auth-login-status');
  const token = inputEl ? inputEl.value.trim() : '';
  if (!token) {
    if (statusEl) statusEl.textContent = 'Token required';
    return;
  }
  try {
    const res = await fetch('/api/v1/auth/login', {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-Requested-With': 'XMLHttpRequest'
      },
      body: JSON.stringify({ token })
    });
    if (!res.ok) {
      if (statusEl) statusEl.textContent = 'Invalid token';
      return;
    }
    if (inputEl) inputEl.value = '';
    if (statusEl) statusEl.textContent = '';
    const authPanel = document.getElementById('auth-login-panel');
    if (authPanel) authPanel.style.display = 'none';
    await fetchInitialDashboard();
    connectSSE();
  } catch (err) {
    if (statusEl) statusEl.textContent = 'Login request failed';
  }
}

async function submitAuthLogout() {
  try {
    await apiFetch('/api/v1/auth/logout', { method: 'POST' });
    const authPanel = document.getElementById('auth-login-panel');
    if (authPanel) authPanel.style.display = 'flex';
  } catch (_) {}
}

let activePage = 'overall';
let cachedConfigRows = [];
let activeConfigCategory = 'all';
let cachedPondUniverse = [];
let latestSettlingTimeUtc = '00:05';

// Browser Local Timezone Parsing & Formatting Utilities
function parseUtcToDate(raw) {
  if (raw === null || raw === undefined) return null;
  if (raw instanceof Date) {
    return Number.isNaN(raw.getTime()) ? null : raw;
  }
  if (typeof raw === 'number') {
    if (!Number.isFinite(raw) || raw < 1e9) return null;
    const ms = raw < 1e11 ? raw * 1000 : raw;
    const dt = new Date(ms);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }
  const str = String(raw).trim();
  if (!str) return null;
  if (/^\d{10,13}$/.test(str)) {
    const num = Number(str);
    if (!Number.isFinite(num) || num < 1e9) return null;
    const ms = str.length === 10 ? num * 1000 : num;
    const dt = new Date(ms);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }
  const cleaned = str.replace(/(\d{2}:\d{2}:\d{2})\.\d+/, '$1');
  const norm = cleaned.replace(/^(\d{4})\/(\d{2})\/(\d{2})(?=[ T]|$)/, '$1-$2-$3');
  if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$/.test(norm)) {
    const withSec = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}$/.test(norm) ? `${norm}:00` : norm;
    const isoUtc = withSec.replace(' ', 'T') + 'Z';
    const dt = new Date(isoUtc);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }
  if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})$/i.test(norm)) {
    const normalizedOffset = norm
      .replace(' ', 'T')
      .replace(/([+-]\d{2})(\d{2})$/, '$1:$2')
      .replace(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})(Z|[+-]\d{2}:\d{2})$/i, '$1:00$2');
    const dt = new Date(normalizedOffset);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(norm)) {
    const dt = new Date(`${norm}T00:00:00Z`);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }
  return null;
}

function formatLocalTimestamp(raw, fallback = '-') {
  if (raw === null || raw === undefined) return fallback;
  const str = String(raw).trim();
  if (!str) return fallback;
  const dt = parseUtcToDate(raw);
  if (!dt) return str.replace(/(\d{2}:\d{2}:\d{2})\.\d+/, '$1');
  const yyyy = dt.getFullYear();
  const mm = pad2(dt.getMonth() + 1);
  const dd = pad2(dt.getDate());
  const hh = pad2(dt.getHours());
  const mi = pad2(dt.getMinutes());
  const ss = pad2(dt.getSeconds());
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

function formatUtcTimeOfDayToLocal(utcHhMm) {
  const raw = String(utcHhMm || '00:05').trim();
  const m = raw.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return raw;
  const hhUtc = Number(m[1]);
  const mmUtc = Number(m[2]);
  const ssUtc = Number(m[3] || 0);
  if (hhUtc < 0 || hhUtc > 23 || mmUtc < 0 || mmUtc > 59 || ssUtc < 0 || ssUtc > 59) {
    return raw;
  }
  const now = new Date();
  const dt = new Date(Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
    hhUtc,
    mmUtc,
    ssUtc
  ));
  if (Number.isNaN(dt.getTime())) return raw;
  const hh = pad2(dt.getHours());
  const mi = pad2(dt.getMinutes());
  if (m[3] !== undefined && ssUtc > 0) {
    return `${hh}:${mi}:${pad2(dt.getSeconds())}`;
  }
  return `${hh}:${mi}`;
}

function replaceUtcTimestampsInText(text) {
  if (typeof text !== 'string') return String(text ?? '');
  return text
    .replace(
      /\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?/g,
      (match) => formatLocalTimestamp(match, match)
    )
    .replace(
      /\b(\d{1,2}):(\d{2})(?::(\d{2}))?\s*UTC\b/gi,
      (full, hStr, mStr, sStr) => {
        const hh = Number(hStr), mm = Number(mStr), ss = Number(sStr || 0);
        if (hh > 23 || mm > 59 || ss > 59) return full;
        const hhmm = sStr !== undefined ? `${hStr}:${mStr}:${sStr}` : `${hStr}:${mStr}`;
        return `${formatUtcTimeOfDayToLocal(hhmm)} Local`;
      }
    );
}

function formatConfigDescriptionWithLocalTime(c, overrideVal) {
  const baseDesc = c.description || '';
  const key = c.config_key || '';
  const rawVal = overrideVal !== undefined
    ? overrideVal
    : (Object.prototype.hasOwnProperty.call(dirtyConfigInputs, key) ? dirtyConfigInputs[key] : (c.config_value ?? ''));
  const val = String(rawVal).trim();
  if (!val) return baseDesc;
  if (['settling_time_utc', 'settling_window_open_utc', 'settling_window_close_utc'].includes(key)) {
    const m = val.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
    if (!m) return baseDesc;
    const hh = Number(m[1]), mm = Number(m[2]), ss = Number(m[3] || 0);
    if (hh > 23 || mm > 59 || ss > 59) return baseDesc;
    return `${baseDesc} (Local: ${formatUtcTimeOfDayToLocal(val)})`;
  }
  if (key === 'equity_baseline_reset_at' || key.endsWith('_at')) {
    const dt = parseUtcToDate(val);
    if (!dt) return baseDesc;
    return `${baseDesc} (Local: ${formatLocalTimestamp(dt)})`;
  }
  return baseDesc;
}

function updateTimezoneIndicators(utcHhMm = latestSettlingTimeUtc) {
  latestSettlingTimeUtc = utcHhMm || '00:05';
  const localSettleStr = formatUtcTimeOfDayToLocal(latestSettlingTimeUtc);

  const headerSettleEl = document.getElementById('header-settle-time');
  if (headerSettleEl) headerSettleEl.textContent = `${localSettleStr} Local`;

  const settleBadgeEl = document.getElementById('pp50-settle-time-badge');
  if (settleBadgeEl) settleBadgeEl.textContent = `${localSettleStr} Local`;

  const settleHeadingEl = document.getElementById('settle-table-heading');
  if (settleHeadingEl) {
    settleHeadingEl.textContent = `Daily ${localSettleStr} Local 50/50 Settling Ledger (\`book_transfers\` — Invariant F5 Idle-Only Transfer)`;
  }

  const dryBtn = document.getElementById('btn-settle-dry');
  if (dryBtn && !dryBtn.disabled) {
    dryBtn.textContent = `Dry-Run ${localSettleStr} Settle`;
  }
}

// Accessible 4-Page Tab Switcher with Keyboard Navigation & URL Hash Sync
function switchDashboardPage(pageId) {
  const validPages = ['overall', 'pit', 'pond', 'settings'];
  if (!validPages.includes(pageId)) pageId = 'overall';
  activePage = pageId;

  validPages.forEach((id) => {
    const btn = document.getElementById(`tab-btn-${id}`);
    const panel = document.getElementById(`page-${id}`);
    const isSelected = id === pageId;
    if (btn) {
      btn.setAttribute('aria-selected', isSelected ? 'true' : 'false');
      btn.setAttribute('tabindex', isSelected ? '0' : '-1');
    }
    if (panel) {
      if (isSelected) {
        panel.removeAttribute('hidden');
      } else {
        panel.setAttribute('hidden', '');
      }
    }
  });

  if (history.replaceState) {
    history.replaceState(null, '', `#${pageId}`);
  }
}

// Keyboard Left/Right/Home/End navigation for ARIA tablist + DOM event bindings
document.addEventListener('DOMContentLoaded', () => {
  updateTimezoneIndicators('00:05');
  const tabIds = ['overall', 'pit', 'pond', 'settings'];
  const tablist = document.querySelector('[role="tablist"]');
  if (tablist) {
    tablist.addEventListener('keydown', (e) => {
      const curIdx = tabIds.indexOf(activePage);
      let nextIdx = curIdx;
      if (e.key === 'ArrowRight') nextIdx = (curIdx + 1) % tabIds.length;
      else if (e.key === 'ArrowLeft') nextIdx = (curIdx - 1 + tabIds.length) % tabIds.length;
      else if (e.key === 'Home') nextIdx = 0;
      else if (e.key === 'End') nextIdx = tabIds.length - 1;
      else return;
      e.preventDefault();
      const nextId = tabIds[nextIdx];
      switchDashboardPage(nextId);
      const btn = document.getElementById(`tab-btn-${nextId}`);
      if (btn) btn.focus();
    });
  }

  document.querySelectorAll('.page-tab[data-page]').forEach((btn) => {
    btn.addEventListener('click', () => {
      switchDashboardPage(btn.getAttribute('data-page'));
    });
  });

  document.querySelectorAll('[data-switch-page]').forEach((btn) => {
    btn.addEventListener('click', () => {
      switchDashboardPage(btn.getAttribute('data-switch-page'));
    });
  });

  const bindClick = (id, fn) => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('click', fn);
  };
  bindClick('btn-cycle', triggerCycle);
  bindClick('btn-eval-signals', triggerCycle);
  bindClick('btn-pond-scan', triggerPondScan);
  bindClick('btn-pond-scan-inline', triggerPondScan);
  bindClick('btn-pond-scan-table', triggerPondScan);
  bindClick('btn-settle-dry', () => triggerSettleRun(true));
  bindClick('btn-settle-sim', () => triggerSettleRun(true));
  bindClick('btn-settle-live', () => triggerSettleRun(false));
  bindClick('btn-calibrate', triggerCalibration);
  bindClick('btn-backtest', triggerBacktest);
  bindClick('btn-auth-login', submitAuthLogin);
  bindClick('btn-close-report', () => {
    const rp = document.getElementById('report-panel');
    if (rp) rp.style.display = 'none';
  });
  bindClick('btn-apply-capital-baseline', applyCapitalBaseline);
  bindClick('btn-sync-wallet', syncWalletEquity);
  bindClick('btn-save-credentials', saveCredentials);
  bindClick('btn-save-tg-alerts', saveTelegramAlerts);
  bindClick('btn-send-test-alert', sendTestAlert);

  const authInput = document.getElementById('auth-token-input');
  if (authInput) {
    authInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') submitAuthLogin();
    });
  }

  const pondPolicySelect = document.getElementById('pond-policy-select');
  if (pondPolicySelect) {
    pondPolicySelect.addEventListener('change', (e) => updatePondDirectionPolicy(e.target.value));
  }

  const pondFilterInput = document.getElementById('pond-universe-filter');
  if (pondFilterInput) {
    pondFilterInput.addEventListener('input', () => renderPondUniverseTable());
  }

  document.querySelectorAll('[data-dirty-field]').forEach((el) => {
    el.addEventListener('input', () => markFormFieldDirty(el.getAttribute('data-dirty-field')));
  });

  const credModeSelect = document.getElementById('cred-mode');
  if (credModeSelect) {
    credModeSelect.addEventListener('change', onRoutingModeChange);
  }

  document.querySelectorAll('#config-filter-pill-bar [data-cat]').forEach((btn) => {
    btn.addEventListener('click', () => {
      setConfigCategoryFilter(btn.getAttribute('data-cat'), btn);
    });
  });

  const cfgSearchInput = document.getElementById('config-search-input');
  if (cfgSearchInput) {
    cfgSearchInput.addEventListener('input', () => renderConfigTable());
  }

  const cfgTbody = document.getElementById('config-tbody');
  if (cfgTbody) {
    cfgTbody.addEventListener('input', (e) => {
      const target = e.target;
      if (target && target.matches && target.matches('input[data-config-input]')) {
        const key = target.getAttribute('data-config-input');
        if (key) onConfigInputEdit(key, target.value);
      }
    });
    cfgTbody.addEventListener('click', (e) => {
      const btn = e.target && e.target.closest ? e.target.closest('button[data-save-config]') : null;
      if (btn) {
        const key = btn.getAttribute('data-save-config');
        if (key) saveConfig(key);
      }
    });
  }

  const openTradesTbody = document.getElementById('open-trades-tbody');
  if (openTradesTbody) {
    openTradesTbody.addEventListener('click', (e) => {
      const btn = e.target && e.target.closest ? e.target.closest('button[data-take-profit-trade]') : null;
      if (btn) {
        const tradeId = Number(btn.getAttribute('data-take-profit-trade'));
        const symbol = btn.getAttribute('data-trade-symbol') || '';
        if (Number.isFinite(tradeId) && tradeId > 0) {
          triggerTakeProfit(tradeId, symbol, btn);
        }
      }
    });
  }

  const pondCellsTbody = document.getElementById('pond-cells-tbody');
  if (pondCellsTbody) {
    pondCellsTbody.addEventListener('click', (e) => {
      const btn = e.target && e.target.closest ? e.target.closest('button[data-take-profit-cell]') : null;
      if (btn) {
        const cellId = Number(btn.getAttribute('data-take-profit-cell'));
        const symbol = btn.getAttribute('data-cell-symbol') || '';
        if (Number.isFinite(cellId) && cellId > 0) {
          triggerPondCellTakeProfit(cellId, symbol, btn);
        }
      }
    });
  }

  const hash = (window.location.hash || '').replace('#', '').toLowerCase();
  if (['overall', 'pit', 'pond', 'settings'].includes(hash)) {
    switchDashboardPage(hash);
  }

  fetchInitialDashboard();
  connectSSE();
});

// Colour-Coded PnL Helpers (Color + Directional Icon + Sign)
function getPnlClass(val) {
  const num = Number(val || 0);
  if (num > 0.0001) return 'pnl-pos';
  if (num < -0.0001) return 'pnl-neg';
  return 'pnl-flat';
}

function formatPnlText(val, pctOpt = null) {
  const num = Number(val || 0);
  const absStr = fmtUsd(Math.abs(num));
  let prefix = '• ';
  let sign = '';
  if (num > 0.0001) { prefix = '▲ '; sign = '+'; }
  else if (num < -0.0001) { prefix = '▼ '; sign = '-'; }
  const pctSuffix = pctOpt !== null && pctOpt !== undefined
    ? ` (${num > 0.0001 ? '+' : ''}${Number(pctOpt).toFixed(2)}%)`
    : '';
  return `${prefix}${sign}${absStr}${pctSuffix}`;
}

function formatPnlPillHtml(val, pctOpt = null) {
  const cls = getPnlClass(val);
  return `<span class="pnl-pill ${cls}">${formatPnlText(val, pctOpt)}</span>`;
}

function applyPnlElement(el, val, pctOpt = null, isPill = false) {
  if (!el) return;
  const cls = getPnlClass(val);
  el.classList.remove('pnl-pos', 'pnl-neg', 'pnl-flat');
  el.classList.add(cls);
  if (isPill) el.classList.add('pnl-pill');
  el.textContent = formatPnlText(val, pctOpt);
}

function applyKpiCardPnlBorder(cardEl, val) {
  if (!cardEl) return;
  cardEl.classList.remove('kpi-pnl-pos', 'kpi-pnl-neg');
  const num = Number(val || 0);
  if (num > 0.0001) cardEl.classList.add('kpi-pnl-pos');
  else if (num < -0.0001) cardEl.classList.add('kpi-pnl-neg');
}

function regimeBadgeClass(regime) {
  if (regime === 'BULL') return 'badge badge-bull';
  if (regime === 'BEAR') return 'badge badge-bear';
  return 'badge badge-chop';
}

function renderDashboard(data) {
  if (!data) return;
  const p = data.portfolio || {};
  const pond = data.pond || {};
  const pp50 = data.pp50 || {};

  // 1. OVERALL STATUS & NAV BAR PNL PILLS
  const totalEq = pp50.total_platform_equity_usdc ?? ((p.pit_equity_usdc || 5000) + (pond.pond_equity_usdc || 5000));
  const initEq = pp50.platform_initial_equity_usdc ?? 10000;
  const totalPnl = pp50.total_platform_pnl_usdc ?? ((p.total_pnl_usdc || 0) + (pond.total_pnl_usdc || 0));
  const totalPnlPct = pp50.total_platform_pnl_pct ?? ((totalPnl / Math.max(initEq, 1)) * 100);

  const pitInfo = pp50.pit || {};
  const pondInfo = pp50.pond || {};
  const pitEq = pitInfo.equity_usdc ?? p.pit_equity_usdc ?? 5000;
  const pitPnl = pitInfo.total_pnl_usdc ?? p.total_pnl_usdc ?? 0;
  const pitPnlPct = pitInfo.total_pnl_pct ?? p.total_pnl_pct ?? 0;
  const pitRealized = pitInfo.realized_pnl_usdc ?? p.realized_pnl_usdc ?? 0;
  const pitUnrealized = (pitInfo.unrealized_pnl_usdc ?? p.unrealized_pnl_usdc ?? 0) + (pitInfo.funding_accrued_usdc ?? p.funding_accrued_usdc ?? 0);

  const pondEq = pondInfo.equity_usdc ?? pond.pond_equity_usdc ?? 5000;
  const pondPnl = pondInfo.total_pnl_usdc ?? pond.total_pnl_usdc ?? 0;
  const pondPnlPct = pondInfo.total_pnl_pct ?? pond.total_pnl_pct ?? 0;
  const pondRealized = pondInfo.realized_pnl_usdc ?? pond.realized_pnl_usdc ?? 0;
  const pondUnrealized = (pondInfo.unrealized_pnl_usdc ?? pond.unrealized_pnl_usdc ?? 0) + (pondInfo.funding_accrued_usdc ?? pond.funding_accrued_usdc ?? 0);

  applyPnlElement(document.getElementById('nav-total-pnl-pill'), totalPnl, totalPnlPct, true);
  applyPnlElement(document.getElementById('nav-pit-pnl-pill'), pitPnl, null, true);
  applyPnlElement(document.getElementById('nav-pond-pnl-pill'), pondPnl, null, true);

  document.getElementById('pp50-total-equity').textContent = fmtUsd(totalEq);
  document.getElementById('pp50-initial-equity').textContent = fmtUsd(initEq);
  document.getElementById('pp50-target-each').textContent = fmtUsd(totalEq * 0.5);

  applyPnlElement(document.getElementById('pp50-total-pnl'), totalPnl);
  applyPnlElement(document.getElementById('pp50-total-pnl-badge'), totalPnl, totalPnlPct, true);
  applyKpiCardPnlBorder(document.getElementById('pp50-card-total-pnl'), totalPnl);

  applyPnlElement(document.getElementById('pp50-pit-pnl'), pitPnl);
  applyPnlElement(document.getElementById('pp50-pit-pnl-badge'), pitPnl, pitPnlPct, true);
  applyKpiCardPnlBorder(document.getElementById('pp50-card-pit-pnl'), pitPnl);
  document.getElementById('pp50-pit-equity').textContent = fmtUsd(pitEq);
  const pitShare = pp50.pit_share_pct ?? 50.0;
  const pondShare = pp50.pond_share_pct ?? 50.0;
  document.getElementById('pp50-pit-share').textContent = `${Number(pitShare).toFixed(1)}%`;

  applyPnlElement(document.getElementById('pp50-pond-pnl'), pondPnl);
  applyPnlElement(document.getElementById('pp50-pond-pnl-badge'), pondPnl, pondPnlPct, true);
  applyKpiCardPnlBorder(document.getElementById('pp50-card-pond-pnl'), pondPnl);
  document.getElementById('pp50-pond-equity').textContent = fmtUsd(pondEq);
  document.getElementById('pp50-pond-share').textContent = `${Number(pondShare).toFixed(1)}%`;

  const driftBps = Number(pp50.drift_bps || 0);
  const deadbandBps = Number(pp50.deadband_bps || 50);
  document.getElementById('pp50-drift-bps').textContent = `${driftBps.toFixed(1)} bps`;
  document.getElementById('pp50-deadband-bps').textContent = `${deadbandBps.toFixed(1)} bps`;
  updateTimezoneIndicators(pp50.settling_time_utc || '00:05');
  const settleStatusEl = document.getElementById('pp50-settle-status-label');
  if (pp50.platform_pp50_halt) {
    settleStatusEl.textContent = 'HALTED';
    settleStatusEl.className = 'text-red';
  } else if (pp50.settling_in_progress) {
    settleStatusEl.textContent = 'SETTLING';
    settleStatusEl.className = 'text-amber';
  } else if (driftBps > deadbandBps) {
    settleStatusEl.textContent = 'REBALANCE DUE';
    settleStatusEl.className = 'text-amber';
  } else {
    settleStatusEl.textContent = 'WITHIN DEAD-BAND';
    settleStatusEl.className = 'text-green';
  }
  document.getElementById('pp50-split-bar-pit').style.width = `${Math.max(5, Math.min(95, pitShare))}%`;
  document.getElementById('pp50-split-bar-pond').style.width = `${Math.max(5, Math.min(95, pondShare))}%`;

  const pitRegime = pitInfo.hmm_regime || p.hmm_regime || 'CHOP';
  const ovPitRegime = document.getElementById('overall-pit-regime-badge');
  ovPitRegime.textContent = pitRegime;
  ovPitRegime.className = regimeBadgeClass(pitRegime);
  document.getElementById('overall-pit-equity').innerHTML = `${fmtUsd(pitEq)} <span class="text-muted" style="font-size:11px;">/ ${fmtUsd(pitInfo.initial_capital_usdc || 5000)}</span>`;
  applyPnlElement(document.getElementById('overall-pit-total-pnl'), pitPnl, pitPnlPct);
  applyPnlElement(document.getElementById('overall-pit-realized-pnl'), pitRealized);
  applyPnlElement(document.getElementById('overall-pit-unrealized-pnl'), pitUnrealized);
  document.getElementById('overall-pit-positions').textContent = `${pitInfo.open_positions || 0} pos | ${fmtUsd(pitInfo.heat_usdc || 0)} heat`;
  document.getElementById('overall-pit-leverage').textContent = `${fmtUsd(pitInfo.deployed_notional_usdc || 0)} (${fmtNum(pitInfo.gross_leverage || 0, 2)}x)`;

  const pondPolicy = pondInfo.direction_policy || pond.direction_policy || 'both';
  document.getElementById('overall-pond-policy-badge').textContent = `POLICY: ${pondPolicy.toUpperCase()}`;
  document.getElementById('overall-pond-equity').innerHTML = `${fmtUsd(pondEq)} <span class="text-muted" style="font-size:11px;">/ ${fmtUsd(pondInfo.initial_capital_usdc || 5000)}</span>`;
  applyPnlElement(document.getElementById('overall-pond-total-pnl'), pondPnl, pondPnlPct);
  applyPnlElement(document.getElementById('overall-pond-realized-pnl'), pondRealized);
  applyPnlElement(document.getElementById('overall-pond-unrealized-pnl'), pondUnrealized);
  document.getElementById('overall-pond-cells').textContent = `${pond.open_cells_count || 0} / ${pond.max_concurrent_cells || 8} cells | ${fmtUsd(pond.open_heat_usdc || 0)} / ${fmtUsd(pond.max_open_heat_usdc || 200)} heat`;
  document.getElementById('overall-pond-leverage').textContent = `${fmtUsd(pond.deployed_notional_usdc || 0)} (${fmtNum(pond.gross_leverage || 0, 2)}x)`;

  if (pp50.latest_snapshot) {
    const snap = pp50.latest_snapshot;
    const snapTs = formatLocalTimestamp(snap.timestamp_ms || snap.created_at, '');
    if (snap.report_line) {
      const localizedLine = replaceUtcTimestampsInText(snap.report_line);
      document.getElementById('pp50-report-line').textContent =
        (snapTs && !localizedLine.includes(snapTs)) ? `[${snapTs}] ${localizedLine}` : localizedLine;
    } else {
      const prefix = snapTs ? `[${snapTs}] ` : '';
      document.getElementById('pp50-report-line').textContent =
        `${prefix}PP-50 D+${pad2(snap.snapshot_id || 1)} | platform ${fmtUsd(snap.platform_equity_usdc ?? totalEq)} (${totalPnlPct >= 0 ? '+' : ''}${Number(totalPnlPct).toFixed(2)}%) | PIT ${fmtUsd(snap.pit_equity_usdc ?? pitEq)} | POND ${fmtUsd(snap.pond_equity_usdc ?? pondEq)} | drift ${fmtNum(snap.drift_bps ?? driftBps, 1)} bps | settle: ${replaceUtcTimestampsInText(snap.settling_status || 'READY')} | cells: ${snap.pond_open_cells ?? (pond.open_cells_count || 0)}/${pond.max_concurrent_cells || 8} (heat ${fmtUsd(pond.open_heat_usdc || 0)}) | champion ${pond.active_champion_id || 'NPS-01'}`;
    }
  }

  const transfers = pp50.recent_transfers || [];
  const settleTbody = document.getElementById('settle-transfers-tbody');
  if (transfers.length === 0) {
    settleTbody.innerHTML = '<tr><td colspan="12" class="text-muted">No settling transfers recorded yet. Click "Simulate Dry-Run Settle" to test the 5-phase protocol.</td></tr>';
  } else {
    settleTbody.innerHTML = transfers.map((tr) => {
      const stClass = (tr.status === 'COMPLETED' || tr.status === 'NO_ACTION_WITHIN_DEADBAND') ? 'text-green' : (tr.status === 'PARTIAL_IDLE_CAPPED' ? 'text-amber' : 'text-blue');
      const prePit = tr.from_book === 'PIT' ? tr.pre_from_usdc : (tr.to_book === 'PIT' ? tr.pre_to_usdc : tr.pre_from_usdc);
      const prePond = tr.from_book === 'POND' ? tr.pre_from_usdc : (tr.to_book === 'POND' ? tr.pre_to_usdc : tr.pre_to_usdc);
      const preTotal = (Number(prePit || 0) + Number(prePond || 0));
      const execAmt = tr.amount_usdc ?? tr.executed_transfer_usdc ?? 0;
      const reqAmt = tr.required_transfer_usdc ?? execAmt;
      const driftVal = tr.drift_bps_before ?? tr.drift_bps ?? 0;
      const rawTs = (tr.created_at && (!tr.settle_date || String(tr.settle_date).length <= 10))
        ? tr.created_at
        : (tr.settle_date || tr.created_at);
      return `
        <tr>
          <td>#${escapeHtml(tr.transfer_id)}</td>
          <td>${escapeHtml(formatLocalTimestamp(rawTs))}</td>
          <td><strong>${escapeHtml(tr.from_book)} → ${escapeHtml(tr.to_book)}</strong></td>
          <td>${fmtUsd(prePit)}</td>
          <td>${fmtUsd(prePond)}</td>
          <td>${fmtUsd(preTotal)}</td>
          <td>${fmtNum(driftVal, 1)} bps</td>
          <td>${fmtUsd(reqAmt)}</td>
          <td>${fmtUsd(tr.available_idle_usdc ?? execAmt)}</td>
          <td class="text-green"><strong>${fmtUsd(execAmt)}</strong></td>
          <td class="${(tr.blocked_remainder_usdc || 0) > 0 ? 'text-amber' : 'text-muted'}">${fmtUsd(tr.blocked_remainder_usdc || 0)}</td>
          <td class="${stClass}"><strong>${escapeHtml(tr.status)}</strong></td>
        </tr>
      `;
    }).join('');
  }

  // 2. PIT (BOOK A) PAGE
  const pitInitCap = p.pit_initial_capital_usdc || 5000;
  document.getElementById('kpi-equity').textContent = fmtUsd(pitEq);
  const deltaEl = document.getElementById('kpi-equity-delta');
  applyPnlElement(deltaEl, pitPnl, pitPnlPct);
  document.getElementById('kpi-cash').textContent = `Idle USDC: ${fmtUsd(p.pit_idle_usdc ?? p.available_cash_usdc)} | Baseline: ${fmtUsd(pitInitCap)}`;

  applyPnlElement(document.getElementById('pit-kpi-total-pnl'), pitPnl);
  applyPnlElement(document.getElementById('pit-kpi-pnl-pct-badge'), pitPnl, pitPnlPct, true);
  applyPnlElement(document.getElementById('pit-kpi-realized-pnl'), pitRealized);
  applyPnlElement(document.getElementById('pit-kpi-unrealized-pnl'), pitUnrealized);
  applyKpiCardPnlBorder(document.getElementById('pit-card-total-pnl'), pitPnl);

  const maxHeat = p.max_portfolio_heat_usdc || (pitInitCap * 0.04);
  const heatPct = (p.portfolio_heat_pct || 0) * 100;
  document.getElementById('kpi-heat').innerHTML = `${fmtUsd(p.portfolio_heat_usdc)} <span style="font-size:13px; color:var(--text-muted)">/ ${fmtUsd(maxHeat)}</span>`;
  document.getElementById('kpi-heat-pct').textContent = fmtNum(heatPct, 2) + '%';
  document.getElementById('bar-heat').style.width = Math.min(100, (heatPct / 4.0) * 100) + '%';

  const lev = p.gross_leverage || 0;
  document.getElementById('kpi-leverage').textContent = fmtNum(lev, 2) + 'x';
  document.getElementById('kpi-notional').textContent = fmtUsd(p.deployed_notional_usdc);
  document.getElementById('bar-leverage').style.width = Math.min(100, (lev / 1.5) * 100) + '%';

  document.getElementById('kpi-basis').textContent = fmtUsd(p.basis_harvest_notional_usdc);
  applyPnlElement(document.getElementById('kpi-yield'), p.accrued_yield_usdc || 0);

  const regime = p.hmm_regime || 'CHOP';
  const regBadge = document.getElementById('kpi-regime');
  regBadge.textContent = regime;
  regBadge.className = regimeBadgeClass(regime);
  document.getElementById('prob-bull').textContent = fmtNum((p.prob_bull || 0) * 100, 1) + '%';
  document.getElementById('prob-bear').textContent = fmtNum((p.prob_bear || 0) * 100, 1) + '%';
  document.getElementById('prob-chop').textContent = fmtNum((p.prob_chop || 0) * 100, 1) + '%';

  const sigs = data.signals || [];
  const latestSigTs = sigs.length > 0 ? (sigs[0].timestamp_ms || sigs[0].created_at) : null;
  document.getElementById('signals-count').textContent = latestSigTs
    ? `${sigs.length} Pairs | Eval: ${formatLocalTimestamp(latestSigTs)}`
    : `${sigs.length} Pairs`;
  const sigTbody = document.getElementById('signals-tbody');
  sigTbody.innerHTML = sigs.map(s => {
    const execSym = s.symbol || s.exec_symbol || '-';
    const tf = s.timeframe || '4h';
    const ensScore = s.ensemble_score ?? s.blended_conviction ?? 0;
    const entryThresh = s.entry_threshold ?? 0.65;
    const volTier = s.volatility_tier || `RANK_${s.rs_rank || 1}`;
    const ensClass = ensScore >= entryThresh ? 'text-green' : (ensScore <= -entryThresh ? 'text-red' : 'text-muted');
    let actionBadge = `<span class="text-muted">${escapeHtml(s.signal_action)}</span>`;
    if (s.signal_action === 'LONG_ENTRY') actionBadge = `<span class="badge badge-bull">LONG_ENTRY</span>`;
    else if (s.signal_action === 'SHORT_ENTRY') actionBadge = `<span class="badge badge-bear">SHORT_ENTRY</span>`;
    else if (s.signal_action === 'BASIS_HARVEST') actionBadge = `<span class="badge badge-info">BASIS_HARVEST</span>`;

    return `
      <tr>
        <td><strong>${escapeHtml(execSym)}</strong></td>
        <td class="text-muted">${escapeHtml(s.synth_symbol || '-')}</td>
        <td>${escapeHtml(tf)}</td>
        <td class="${s.mace_score >= 0 ? 'text-green' : 'text-red'}">${s.mace_score >= 0 ? '+' : ''}${fmtNum(s.mace_score, 3)}</td>
        <td class="${s.snr_score >= 0 ? 'text-green' : 'text-red'}">${s.snr_score >= 0 ? '+' : ''}${fmtNum(s.snr_score, 3)}</td>
        <td class="${ensClass}"><strong>${ensScore >= 0 ? '+' : ''}${fmtNum(ensScore, 3)}</strong></td>
        <td>±${fmtNum(entryThresh, 2)}</td>
        <td>${escapeHtml(volTier)}</td>
        <td>${fmtNum((s.funding_rate_hourly || 0) * 100, 4)}%</td>
        <td>${actionBadge}</td>
      </tr>
    `;
  }).join('');

  const openTrades = data.open_trades || [];
  document.getElementById('open-trades-count').textContent = `${openTrades.length} OPEN`;
  const openTbody = document.getElementById('open-trades-tbody');
  if (openTrades.length === 0) {
    openTbody.innerHTML = `<tr><td colspan="10" class="text-muted" style="text-align:center; padding:16px;">No active perpetual positions open</td></tr>`;
  } else {
    openTbody.innerHTML = openTrades.map(t => {
      const upnl = (t.unrealized_pnl_usdc || 0) + (t.funding_accrued_usdc || 0);
      return `
        <tr>
          <td><strong>${escapeHtml(t.symbol)}</strong></td>
          <td><span class="badge ${t.side === 'LONG' ? 'badge-bull' : 'badge-bear'}">${escapeHtml(t.side)}</span></td>
          <td>${fmtUsd(t.notional_usdc)}</td>
          <td>${fmtNum(t.entry_price, 4)}</td>
          <td>${fmtNum(t.current_price, 4)}</td>
          <td class="text-amber">${fmtNum(t.chandelier_stop_price, 4)}</td>
          <td>${fmtUsd(t.risk_budget_usdc)}</td>
          <td>${formatPnlPillHtml(upnl)}</td>
          <td class="text-muted">${escapeHtml(formatLocalTimestamp(t.opened_at))}</td>
          <td><button type="button" class="success" style="padding: 3px 8px; font-size: 11px;" data-take-profit-trade="${escapeHtml(t.trade_id)}" data-trade-symbol="${escapeHtml(t.symbol)}" title="Manually take profit / close position #${escapeHtml(t.trade_id)}">Take Profit</button></td>
        </tr>
      `;
    }).join('');
  }

  const recentTrades = data.recent_trades || [];
  const recTbody = document.getElementById('recent-trades-tbody');
  recTbody.innerHTML = recentTrades.slice(0, 20).map(t => {
    const tradeState = t.status || t.state || 'CLOSED';
    const pnl = tradeState === 'OPEN' ? ((t.unrealized_pnl_usdc || 0) + (t.funding_accrued_usdc || 0)) : (t.realized_pnl_usdc || 0);
    const fPnl = t.funding_accrued_usdc || 0;
    const tradeTs = tradeState === 'OPEN' ? t.opened_at : (t.closed_at || t.opened_at);
    return `
      <tr>
        <td>#${escapeHtml(t.trade_id)}</td>
        <td class="text-muted">${escapeHtml(formatLocalTimestamp(tradeTs))}</td>
        <td><strong>${escapeHtml(t.symbol)}</strong></td>
        <td><span class="${t.side === 'LONG' ? 'text-green' : 'text-red'}">${escapeHtml(t.side)}</span></td>
        <td>${escapeHtml(replaceUtcTimestampsInText(t.exit_reason || tradeState))}</td>
        <td>${fmtNum(t.entry_price, 4)}</td>
        <td>${t.exit_price ? fmtNum(t.exit_price, 4) : fmtNum(t.current_price, 4)}</td>
        <td>${fmtUsd(t.notional_usdc)}</td>
        <td class="${getPnlClass(fPnl)}">${formatPnlText(fPnl)}</td>
        <td>${formatPnlPillHtml(pnl)}</td>
      </tr>
    `;
  }).join('');

  // 3. POND (BOOK B) PAGE
  const pondInitCap = pondInfo.initial_capital_usdc ?? pond.initial_capital_usdc ?? (initEq * 0.5);
  document.getElementById('pond-kpi-equity').textContent = fmtUsd(pondEq);
  document.getElementById('pond-kpi-idle').textContent = fmtUsd(pond.idle_usdc ?? pondEq);
  const pondBaselineEl = document.getElementById('pond-kpi-baseline');
  if (pondBaselineEl) pondBaselineEl.textContent = fmtUsd(pondInitCap);
  document.getElementById('pond-champion-badge').textContent = pond.active_champion_id || 'NPS-01';

  applyPnlElement(document.getElementById('pond-kpi-total-pnl'), pondPnl);
  applyPnlElement(document.getElementById('pond-kpi-pnl-pct-badge'), pondPnl, pondPnlPct, true);
  applyPnlElement(document.getElementById('pond-kpi-realized-pnl'), pondRealized);
  applyPnlElement(document.getElementById('pond-kpi-unrealized-pnl'), pond.unrealized_pnl_usdc || 0);
  applyKpiCardPnlBorder(document.getElementById('pond-card-total-pnl'), pondPnl);

  const pondFunding = pond.funding_accrued_usdc || 0;
  applyPnlElement(document.getElementById('pond-kpi-funding-pnl'), pondFunding);
  applyKpiCardPnlBorder(document.getElementById('pond-card-funding-pnl'), pondFunding);
  document.getElementById('pond-kpi-short-calls').textContent = pond.short_call_count ?? 0;
  document.getElementById('pond-kpi-short-puts').textContent = pond.short_put_count ?? 0;

  const pondOpenCount = pond.open_cells_count ?? 0;
  const pondMaxCells = pond.max_concurrent_cells ?? 8;
  const pondHeat = pond.open_heat_usdc ?? 0;
  const pondMaxHeat = pond.max_open_heat_usdc ?? 200;
  document.getElementById('pond-kpi-cells').textContent = `${pondOpenCount} / ${pondMaxCells} Cells`;
  document.getElementById('pond-open-cells-badge').textContent = `${pondOpenCount} OPEN`;
  document.getElementById('pond-kpi-heat').innerHTML = `${fmtUsd(pondHeat)} <span style="font-size:13px; color:var(--text-muted)">/ ${fmtUsd(pondMaxHeat)}</span>`;
  document.getElementById('pond-bar-heat').style.width = `${Math.min(100, (pondHeat / Math.max(pondMaxHeat, 1)) * 100)}%`;

  const pondLev = pond.gross_leverage ?? 0;
  const pondMaxLev = pond.max_gross_leverage ?? 1.5;
  document.getElementById('pond-kpi-leverage').textContent = `${fmtNum(pondLev, 2)}x`;
  document.getElementById('pond-kpi-notional').textContent = fmtUsd(pond.deployed_notional_usdc || 0);
  document.getElementById('pond-bar-leverage').style.width = `${Math.min(100, (pondLev / Math.max(pondMaxLev, 0.1)) * 100)}%`;

  document.getElementById('pond-kpi-policy-badge').textContent = pondPolicy;
  const policySelect = document.getElementById('pond-policy-select');
  if (policySelect && document.activeElement !== policySelect) {
    policySelect.value = pondPolicy;
  }

  const recentCells = pond.recent_cells || [];
  const pondCellsTbody = document.getElementById('pond-cells-tbody');
  if (recentCells.length === 0) {
    pondCellsTbody.innerHTML = '<tr><td colspan="18" class="text-muted">No Pond cells opened yet. Click "Run Hourly S-Cycle Scan" to evaluate the 80-coin universe.</td></tr>';
  } else {
    pondCellsTbody.innerHTML = recentCells.map((c) => {
      const isOpen = ['OPEN', 'TRAILED', 'ARMED'].includes(c.state);
      const netPnl = isOpen
        ? ((c.unrealized_pnl_usdc || 0) + (c.funding_accrued_usdc || 0) - (c.fees_paid_usdc || 0))
        : (c.realized_pnl_usdc || 0);
      const sideBadge = c.option_analogue === 'SHORT_CALL'
        ? '<span class="badge badge-bear">SHORT / SHORT_CALL</span>'
        : '<span class="badge badge-bull">LONG / SHORT_PUT</span>';
      const stateBadge = isOpen
        ? `<span class="badge badge-info">${escapeHtml(c.state)}</span>`
        : `<span class="badge">${escapeHtml(c.state)}</span>`;
      const cellTs = isOpen ? (c.opened_at || c.updated_at) : (c.closed_at || c.updated_at || c.opened_at);
      const exitFallback = isOpen
        ? 'HOLDING'
        : (c.cooldown_until ? `COOLDOWN (${formatLocalTimestamp(c.cooldown_until)})` : '-');
      const reasonText = c.exit_reason ? replaceUtcTimestampsInText(c.exit_reason) : exitFallback;
      const exitLabel = escapeHtml(`${c.exit_tier ? `T${c.exit_tier}: ` : ''}${reasonText}`);
      const actionCell = isOpen
        ? `<button type="button" class="success" style="padding: 3px 8px; font-size: 11px;" data-take-profit-cell="${escapeHtml(c.cell_id)}" data-cell-symbol="${escapeHtml(c.symbol)}" title="Manually take profit / close Pond cell #${escapeHtml(c.cell_id)}">Take Profit</button>`
        : '<span class="text-muted">-</span>';
      return `
        <tr>
          <td>#${escapeHtml(c.cell_id)}</td>
          <td class="text-muted">${escapeHtml(formatLocalTimestamp(cellTs))}</td>
          <td><strong>${escapeHtml(c.symbol)}</strong></td>
          <td>${stateBadge}</td>
          <td>${sideBadge}</td>
          <td>${fmtUsd(c.notional_usdc)}</td>
          <td>${fmtUsd(c.risk_budget_usdc)}</td>
          <td>${c.entry_price ? fmtNum(c.entry_price, 4) : '-'}</td>
          <td>${fmtNum(c.current_price, 4)}</td>
          <td class="text-amber">${fmtNum(c.initial_stop ?? c.initial_stop_price, 4)}</td>
          <td class="text-blue">${fmtNum(c.chandelier_stop ?? c.chandelier_stop_price, 4)}</td>
          <td>${fmtNum(c.funding_z_entry ?? c.entry_funding_z, 2)}σ → <strong>${fmtNum(c.current_funding_z, 2)}σ</strong></td>
          <td>${fmtNum(c.premium_score, 2)}</td>
          <td class="${getPnlClass(c.funding_accrued_usdc)}">${formatPnlText(c.funding_accrued_usdc)}</td>
          <td class="${getPnlClass(c.unrealized_pnl_usdc)}">${formatPnlText(c.unrealized_pnl_usdc)}</td>
          <td>${formatPnlPillHtml(netPnl)}</td>
          <td class="text-muted">${exitLabel}</td>
          <td>${actionCell}</td>
        </tr>
      `;
    }).join('');
  }

  cachedPondUniverse = pond.universe || [];
  document.getElementById('pond-universe-count-badge').textContent = `${pond.universe_active_count ?? cachedPondUniverse.length} ACTIVE / ${cachedPondUniverse.length} TOTAL`;
  renderPondUniverseTable();

  const cycles = pond.scan_cycles || [];
  const cyclesTbody = document.getElementById('pond-cycles-tbody');
  if (cycles.length === 0) {
    cyclesTbody.innerHTML = '<tr><td colspan="9" class="text-muted">No S-Cycle scans recorded yet.</td></tr>';
  } else {
    cyclesTbody.innerHTML = cycles.map((cy) => `
      <tr>
        <td>#${escapeHtml(cy.cycle_id)}</td>
        <td>${escapeHtml(formatLocalTimestamp(cy.started_at || cy.completed_at))}</td>
        <td>${escapeHtml(cy.coins_scanned ?? cy.universe_active_count)}</td>
        <td class="text-blue">${escapeHtml(cy.candidates ?? cy.candidates_passed_gates)}</td>
        <td class="text-green"><strong>+${escapeHtml(cy.opened ?? cy.entries_opened)}</strong></td>
        <td class="text-amber">${escapeHtml(cy.exits_evaluated ?? cy.exits_triggered)}</td>
        <td>${escapeHtml(cy.open_cells_after ?? pondOpenCount)}</td>
        <td>${fmtUsd(cy.open_heat_usdc ?? pondHeat)}</td>
        <td>${fmtUsd(cy.pond_equity_usdc ?? pondEq)}</td>
      </tr>
    `).join('');
  }

  // 4. SETTINGS PAGE & SYSTEM CONFIG
  const initCap = p.initial_equity_usdc || 10000;
  const scaleMult = initCap / 10000.0;
  const capInput = document.getElementById('capital-baseline-input');
  if (capInput && !dirtyFormFields.has('capital-baseline-input') && document.activeElement !== capInput) {
    capInput.value = Math.round(initCap);
  }
  const capBadge = document.getElementById('capital-scale-badge');
  if (capBadge) capBadge.textContent = `${fmtNum(scaleMult, 2)}x Baseline`;
  const capSummary = document.getElementById('capital-scale-summary');
  if (capSummary) {
    capSummary.textContent = `Heat Cap: ${fmtUsd(p.max_portfolio_heat_usdc || (initCap * 0.04))} (4.0%) | Base Risk/Trade: ${fmtUsd(p.baseline_risk_per_trade_usdc || (initCap * 0.01))} [${fmtUsd(p.min_risk_per_trade_usdc || (initCap * 0.005))}–${fmtUsd(p.max_risk_per_trade_usdc || (initCap * 0.015))}] | Single Asset Cap: ${fmtUsd(p.single_asset_cap_usdc || (initCap * 0.25))}`;
  }

  if (data.credentials) {
    renderCredentials(data.credentials);
  }

  const schemaRuntimeEl = document.getElementById('schema-runtime-badge');
  if (schemaRuntimeEl) {
    const sv = data.schema_version ?? 7;
    const rt = data.engine_runtime_detail || data.engine_runtime || 'RUST_NATIVE (Axum / Tokio)';
    schemaRuntimeEl.textContent = `Schema v${sv} // ${rt}`;
  }

  cachedConfigRows = data.config || [];
  const cfgHeadingEl = document.getElementById('settings-config-heading');
  if (cfgHeadingEl && cachedConfigRows.length > 0) {
    cfgHeadingEl.textContent = `Live Tunable \`system_config\` Parameters (${cachedConfigRows.length} Keys — SQLite Single Source of Truth)`;
  }
  renderConfigTable();

  const logs = data.logs || [];
  const logsBox = document.getElementById('logs-box');
  logsBox.innerHTML = logs.map(l => `
    <div class="log-entry">
      <span class="text-muted">[${escapeHtml(formatLocalTimestamp(l.timestamp_ms || l.created_at))}]</span>
      <span class="${l.level === 'ERROR' ? 'text-red' : (l.level === 'WARN' ? 'text-amber' : 'text-blue')}">[${escapeHtml(l.level)}]</span>
      <span class="text-muted">${escapeHtml(l.component)}:</span>
      <span>${escapeHtml(replaceUtcTimestampsInText(l.message))}</span>
    </div>
  `).join('');
}

function renderPondUniverseTable() {
  const tbody = document.getElementById('pond-universe-tbody');
  if (!tbody) return;
  const q = (document.getElementById('pond-universe-filter')?.value || '').trim().toUpperCase();
  const filtered = cachedPondUniverse.filter((u) => {
    const sym = (u.symbol || '').toUpperCase();
    const base = (u.base_asset || sym.split('/')[0] || '').toUpperCase();
    return !q || sym.includes(q) || base.includes(q);
  });
  if (filtered.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="text-muted">No matching Pond universe coins.</td></tr>';
    return;
  }
  tbody.innerHTML = filtered.map((u) => {
    const baseAsset = u.base_asset || (u.symbol || '').split('/')[0] || '-';
    let stBadge = `<span class="badge badge-bull">${escapeHtml(u.status)}</span>`;
    if (u.status === 'WARMUP') stBadge = `<span class="badge badge-chop">WARMUP</span>`;
    else if (u.status === 'DELISTING' || u.status === 'RETIRED') stBadge = `<span class="badge badge-bear">${escapeHtml(u.status)}</span>`;
    const streak = u.below_rank95_streak ?? u.delist_streak ?? u.consecutive_below_rank_count ?? 0;
    const rank = u.volume_rank_30d ?? u.rank ?? u.rank_by_volume ?? '-';
    const rawWarmup = u.warmup_until || u.warmup_ready_at;
    const warmupDisplay = rawWarmup ? formatLocalTimestamp(rawWarmup) : 'READY';
    return `
      <tr>
        <td>#${escapeHtml(rank)}</td>
        <td><strong>${escapeHtml(u.symbol)}</strong></td>
        <td><span class="text-blue">${escapeHtml(baseAsset)}</span></td>
        <td>${fmtUsd(u.median_daily_volume_usdc ?? u.median_daily_vol_usdc)}</td>
        <td>${stBadge}</td>
        <td class="text-muted">${escapeHtml(warmupDisplay)}</td>
        <td class="${streak > 0 ? 'text-amber' : 'text-muted'}">${escapeHtml(streak)} / 2</td>
      </tr>
    `;
  }).join('');
}

function setConfigCategoryFilter(cat, btnEl) {
  activeConfigCategory = cat;
  document.querySelectorAll('.filter-pill').forEach((el) => el.classList.remove('active'));
  if (btnEl) btnEl.classList.add('active');
  renderConfigTable();
}

let credModeDirty = false;
let lastCredState = null;
const dirtyConfigInputs = {};
const dirtyFormFields = new Set();
const ROUTING_CONFIG_KEYS = ['execution_mode', 'pond_execution_mode', 'hyperliquid_testnet', 'environment'];

function markFormFieldDirty(fieldId) {
  if (fieldId) dirtyFormFields.add(fieldId);
}

function clearRoutingDirtyInputs() {
  credModeDirty = false;
  ROUTING_CONFIG_KEYS.forEach((k) => {
    delete dirtyConfigInputs[k];
  });
}

function onConfigInputEdit(key, val) {
  dirtyConfigInputs[key] = val;
  const cfgRow = cachedConfigRows.find((r) => r.config_key === key);
  if (cfgRow) {
    const cfgTbody = document.getElementById('config-tbody');
    const tr = cfgTbody ? Array.from(cfgTbody.querySelectorAll('tr[data-config-key]')).find((el) => el.getAttribute('data-config-key') === key) : null;
    const descCell = tr ? tr.querySelector('.cfg-desc-cell') : null;
    if (descCell) {
      descCell.textContent = formatConfigDescriptionWithLocalTime(cfgRow, val);
    }
  }
  if (key === 'settling_time_utc' && /^\d{1,2}:\d{2}(?::\d{2})?$/.test(String(val).trim())) {
    updateTimezoneIndicators(String(val).trim());
  }
}

function renderConfigTable() {
  const cfgTbody = document.getElementById('config-tbody');
  if (!cfgTbody) return;
  const q = (document.getElementById('config-search-input')?.value || '').trim().toLowerCase();
  const filtered = cachedConfigRows.filter((c) => {
    if (c.category === 'credentials') return false;
    if (activeConfigCategory !== 'all') {
      if (activeConfigCategory === 'signals') {
        if (!['hmm', 'mace', 'stops', 'basis'].includes(c.category)) return false;
      } else if (c.category !== activeConfigCategory) {
        return false;
      }
    }
    if (q && !c.config_key.toLowerCase().includes(q) && !(c.description || '').toLowerCase().includes(q)) {
      return false;
    }
    return true;
  });

  const existingRows = Array.from(cfgTbody.querySelectorAll('tr[data-config-key]'));
  const sameRowOrder = existingRows.length === filtered.length &&
    existingRows.every((tr, idx) => tr.getAttribute('data-config-key') === filtered[idx].config_key);

  if (sameRowOrder) {
    filtered.forEach((c, idx) => {
      const row = existingRows[idx];
      const inputEl = document.getElementById(`cfg-${c.config_key}`);
      const targetVal = String(
        Object.prototype.hasOwnProperty.call(dirtyConfigInputs, c.config_key)
          ? dirtyConfigInputs[c.config_key]
          : (c.config_value ?? '')
      );
      if (inputEl && document.activeElement !== inputEl && inputEl.value !== targetVal) {
        inputEl.value = targetVal;
      }
      const descText = formatConfigDescriptionWithLocalTime(c);
      const descCell = row.querySelector('.cfg-desc-cell');
      if (descCell && descCell.textContent !== descText) {
        descCell.textContent = descText;
      }
      const updatedText = formatLocalTimestamp(c.updated_at);
      const updatedCell = row.querySelector('.cfg-updated-cell');
      if (updatedCell && updatedCell.textContent !== updatedText) {
        updatedCell.textContent = updatedText;
      }
      const catBadge = row.querySelector('.cfg-cat-badge');
      if (catBadge && catBadge.textContent !== (c.category || '')) {
        catBadge.textContent = c.category || '';
      }
    });
    return;
  }

  const activeId = document.activeElement ? document.activeElement.id : null;
  cfgTbody.innerHTML = filtered.map((c) => {
    const safeKey = escapeHtml(c.config_key);
    return `
      <tr data-config-key="${safeKey}">
        <td><span class="badge cfg-cat-badge">${escapeHtml(c.category)}</span></td>
        <td><strong>${safeKey}</strong></td>
        <td><input class="config-input" id="cfg-${safeKey}" aria-label="${safeKey}" data-config-input="${safeKey}" /></td>
        <td class="text-muted cfg-desc-cell" style="white-space:normal; max-width:360px;"></td>
        <td class="text-muted cfg-updated-cell">${escapeHtml(formatLocalTimestamp(c.updated_at))}</td>
        <td><button type="button" data-save-config="${safeKey}">Save</button></td>
      </tr>
    `;
  }).join('');

  filtered.forEach((c) => {
    const inputEl = document.getElementById(`cfg-${c.config_key}`);
    if (inputEl) {
      inputEl.value = String(
        Object.prototype.hasOwnProperty.call(dirtyConfigInputs, c.config_key)
          ? dirtyConfigInputs[c.config_key]
          : (c.config_value ?? '')
      );
    }
    const row = Array.from(cfgTbody.querySelectorAll('tr[data-config-key]')).find((el) => el.getAttribute('data-config-key') === c.config_key);
    const descCell = row ? row.querySelector('.cfg-desc-cell') : null;
    if (descCell) {
      descCell.textContent = formatConfigDescriptionWithLocalTime(c);
    }
  });

  if (activeId && activeId.startsWith('cfg-')) {
    const el = document.getElementById(activeId);
    if (el) el.focus();
  }
}

function resolveCredMode(cred) {
  if (!cred) return 'PAPER';
  const raw = String(cred.execution_mode || 'PAPER').toUpperCase();
  if (raw === 'TESTNET' || (raw === 'LIVE' && cred.hyperliquid_testnet) || cred.active_routing === 'LIVE_TESTNET') {
    return 'TESTNET';
  }
  if (raw === 'LIVE' || cred.active_routing === 'LIVE_MAINNET') {
    return 'LIVE';
  }
  return 'PAPER';
}

function modeToRoutingLabel(mode) {
  const m = String(mode || 'PAPER').toUpperCase();
  if (m === 'TESTNET') return 'LIVE_TESTNET';
  if (m === 'LIVE') return 'LIVE_MAINNET';
  return 'PAPER_SIMULATION';
}

function updateRoutingStatusPill(selectedMode, cred) {
  const pill = document.getElementById('cred-status-pill');
  if (!pill || !cred) return;
  const resolvedMode = resolveCredMode(cred);
  const savedRouting = cred.active_routing || modeToRoutingLabel(resolvedMode);
  const isLiveRouting = savedRouting === 'LIVE_TESTNET' || savedRouting === 'LIVE_MAINNET';
  if (credModeDirty && selectedMode && selectedMode !== resolvedMode) {
    pill.textContent = `${savedRouting} → ${modeToRoutingLabel(selectedMode)} (UNSAVED)`;
    pill.style.color = '#fbbf24';
  } else {
    pill.textContent = savedRouting;
    pill.style.color = isLiveRouting ? '#34d399' : '#fbbf24';
  }
}

function updateRoutingEndpointPreview(selectedMode, cred) {
  const endpointLabel = document.getElementById('cred-endpoint-label');
  const vaultInput = document.getElementById('cred-vault');
  const defaultVault = selectedMode === 'LIVE'
    ? '0xdfc24b077bc1425ad1dea75bcb6f8158e10df303'
    : '0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0';
  if (vaultInput) {
    vaultInput.placeholder = defaultVault;
    vaultInput.value = defaultVault;
  }
  if (!endpointLabel) return;
  const pkLabel = cred && cred.private_key_configured ? cred.private_key_masked : 'Not Set';
  const signerRaw = cred && cred.signer_address ? String(cred.signer_address) : '';
  const signerLabel = signerRaw.length >= 10
    ? ` (${signerRaw.slice(0, 6)}...${signerRaw.slice(-4)})`
    : '';
  const geminiLabel = cred && cred.gemini_api_key_configured ? cred.gemini_api_key_masked : 'Not Set';
  let epText = 'Simulated Fill + Slippage';
  if (selectedMode === 'TESTNET') {
    epText = 'https://api.hyperliquid-testnet.xyz';
  } else if (selectedMode === 'LIVE') {
    epText = 'https://api.hyperliquid.xyz';
  } else if (cred && cred.api_endpoint) {
    epText = `Simulated Fill + Slippage (Data: ${cred.api_endpoint})`;
  }
  endpointLabel.textContent = `Endpoint: ${epText} | PK: ${pkLabel}${signerLabel} | Gemini: ${geminiLabel}`;
}

function onRoutingModeChange() {
  const modeSelect = document.getElementById('cred-mode');
  if (!modeSelect) return;
  const resolvedMode = resolveCredMode(lastCredState);
  credModeDirty = modeSelect.value !== resolvedMode;
  updateRoutingStatusPill(modeSelect.value, lastCredState);
  updateRoutingEndpointPreview(modeSelect.value, lastCredState);
}

function renderCredentials(cred) {
  if (!cred) return;
  lastCredState = cred;
  const modeBadge = document.getElementById('routing-mode-badge');
  const modeSelect = document.getElementById('cred-mode');
  const walletInput = document.getElementById('cred-wallet');
  const vaultInput = document.getElementById('cred-vault');

  const resolvedMode = resolveCredMode(cred);
  const isLiveRouting = cred.active_routing === 'LIVE_TESTNET' || cred.active_routing === 'LIVE_MAINNET';

  if (modeBadge) {
    modeBadge.textContent = `MODE: ${cred.active_routing || resolvedMode}`;
    modeBadge.className = isLiveRouting ? 'badge badge-bull' : 'badge badge-info';
  }
  if (modeSelect && credModeDirty && modeSelect.value === resolvedMode) {
    credModeDirty = false;
  }
  if (modeSelect && !credModeDirty && document.activeElement !== modeSelect) {
    modeSelect.value = resolvedMode;
  }
  const currentSelectedMode = modeSelect ? modeSelect.value : resolvedMode;
  updateRoutingStatusPill(currentSelectedMode, cred);
  updateRoutingEndpointPreview(currentSelectedMode, cred);

  if (walletInput && !dirtyFormFields.has('cred-wallet') && document.activeElement !== walletInput && cred.wallet_address !== undefined) {
    walletInput.value = cred.wallet_address;
  }
  if (vaultInput && !dirtyFormFields.has('cred-vault') && document.activeElement !== vaultInput && cred.hyperliquid_vault_address !== undefined) {
    vaultInput.value = cred.hyperliquid_vault_address;
  }

  const alertBadge = document.getElementById('alert-status-badge');
  const tgPill = document.getElementById('tg-status-pill');
  const tgChatInput = document.getElementById('tg-chat-id');
  const tgDdInput = document.getElementById('tg-dd-pct');

  if (alertBadge) {
    alertBadge.textContent = cred.telegram_alerts_ready ? 'TELEGRAM: READY' : 'TELEGRAM: UNCONFIGURED';
    alertBadge.className = cred.telegram_alerts_ready ? 'badge badge-bull' : 'badge';
  }
  if (tgPill) {
    tgPill.textContent = cred.telegram_alerts_ready ? `READY (${cred.telegram_bot_token_masked})` : 'NOT CONFIGURED';
    tgPill.style.color = cred.telegram_alerts_ready ? '#34d399' : 'var(--text-muted)';
  }
  if (tgChatInput && !dirtyFormFields.has('tg-chat-id') && document.activeElement !== tgChatInput && cred.telegram_chat_id !== undefined) {
    tgChatInput.placeholder = cred.telegram_chat_id || 'Enter Telegram Chat ID (leave blank to keep existing)';
    tgChatInput.value = '';
  }
  if (tgDdInput && !dirtyFormFields.has('tg-dd-pct') && document.activeElement !== tgDdInput && cred.max_daily_drawdown_alert_pct !== undefined) {
    tgDdInput.value = Number(cred.max_daily_drawdown_alert_pct * 100).toFixed(1);
  }
}

async function fetchInitialDashboard() {
  try {
    const res = await apiFetch('/api/v1/dashboard');
    if (!res.ok) return;
    const data = await res.json();
    if (data && !data.error) {
      renderDashboard(data);
    }
  } catch (err) {
    console.error('Initial dashboard load error:', err);
  }
}

let activeEventSource = null;
let sseReconnectTimer = null;

function connectSSE() {
  if (sseReconnectTimer) {
    clearTimeout(sseReconnectTimer);
    sseReconnectTimer = null;
  }
  if (activeEventSource) {
    try { activeEventSource.close(); } catch (_) {}
    activeEventSource = null;
  }
  const sseBadge = document.getElementById('sse-status');
  const sseToken = resolveApiAuthToken();
  const sseUrl = sseToken ? `/api/v1/stream?token=${encodeURIComponent(sseToken)}` : '/api/v1/stream';
  const evtSource = new EventSource(sseUrl);
  activeEventSource = evtSource;
  evtSource.onopen = () => {
    sseBadge.textContent = 'SSE: LIVE (3s)';
    sseBadge.className = 'badge badge-bull';
  };
  evtSource.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data);
      if (data && !data.error) {
        sseBadge.textContent = 'SSE: LIVE (3s)';
        sseBadge.className = 'badge badge-bull';
        renderDashboard(data);
      }
    } catch (err) {
      console.error('SSE parse error:', err);
    }
  };
  evtSource.onerror = () => {
    sseBadge.textContent = 'SSE: RECONNECTING';
    sseBadge.className = 'badge badge-chop';
    try { evtSource.close(); } catch (_) {}
    if (activeEventSource === evtSource) {
      activeEventSource = null;
    }
    fetchInitialDashboard();
    if (!sseReconnectTimer) {
      sseReconnectTimer = setTimeout(connectSSE, 3000);
    }
  };
}

setInterval(() => {
  if (!activeEventSource || activeEventSource.readyState !== 1) {
    fetchInitialDashboard();
  }
}, 5000);

async function saveConfig(key) {
  const inputEl = document.getElementById(`cfg-${key}`);
  if (!inputEl) return;
  const val = inputEl.value;
  const res = await apiFetch('/api/v1/config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ config_key: key, config_value: val })
  });
  if (res.ok) {
    delete dirtyConfigInputs[key];
    if (ROUTING_CONFIG_KEYS.includes(key)) {
      clearRoutingDirtyInputs();
    }
    if (key === 'initial_equity_usdc') {
      dirtyFormFields.delete('capital-baseline-input');
    }
    await fetchInitialDashboard();
  } else {
    const err = await res.json().catch(() => ({}));
    alert('Error updating config: ' + (err.detail || res.statusText));
  }
}

async function updatePondDirectionPolicy(policyVal) {
  const res = await apiFetch('/api/v1/config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ config_key: 'pond_direction_policy', config_value: policyVal })
  });
  if (res.ok) {
    await fetchInitialDashboard();
  }
}

async function triggerPondScan() {
  const btn = document.getElementById('btn-pond-scan');
  if (btn) { btn.disabled = true; btn.textContent = 'Scanning 80 Coins...'; }
  try {
    const res = await apiFetch('/api/v1/pond/scan', { method: 'POST' });
    const data = await res.json();
    if (data.dashboard) renderDashboard(data.dashboard);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Run Pond Scan'; }
  }
}

async function triggerSettleRun(dryRun) {
  const btn = document.getElementById('btn-settle-dry');
  if (btn) { btn.disabled = true; btn.textContent = 'Settling...'; }
  try {
    const res = await apiFetch('/api/v1/settle/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dry_run: Boolean(dryRun) })
    });
    const data = await res.json();
    if (data.dashboard) renderDashboard(data.dashboard);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = `Dry-Run ${formatUtcTimeOfDayToLocal(latestSettlingTimeUtc)} Settle`;
    }
  }
}

async function applyCapitalBaseline() {
  const val = parseFloat(document.getElementById('capital-baseline-input').value);
  if (!val || val < 100) {
    alert('Initial equity must be at least $100 USDC.');
    return;
  }
  const res = await apiFetch('/api/v1/config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ config_key: 'initial_equity_usdc', config_value: val, scale_risk_caps: true })
  });
  if (res.ok) {
    dirtyFormFields.delete('capital-baseline-input');
    delete dirtyConfigInputs['initial_equity_usdc'];
    await fetchInitialDashboard();
  }
}

async function syncWalletEquity() {
  const btn = document.getElementById('btn-sync-wallet');
  btn.disabled = true;
  btn.textContent = 'Querying Hyperliquid L1...';
  try {
    const res = await apiFetch('/api/v1/wallet/sync', { method: 'POST' });
    const data = await res.json();
    if (!res.ok) {
      alert('Wallet sync failed: ' + (data.detail || 'Unknown error'));
      return;
    }
    dirtyFormFields.delete('capital-baseline-input');
    await fetchInitialDashboard();
  } finally {
    btn.disabled = false;
    btn.textContent = 'Sync From Hyperliquid L1 Wallet';
  }
}

async function saveCredentials() {
  const mode = document.getElementById('cred-mode').value;
  const wallet = document.getElementById('cred-wallet').value.trim();
  const pk = document.getElementById('cred-pk').value.trim();
  const gemini = document.getElementById('cred-gemini').value.trim();

  const payload = {
    execution_mode: mode,
    hyperliquid_testnet: mode !== 'LIVE',
    wallet_address: wallet
  };
  if (pk.length > 0) payload.private_key = pk;
  if (gemini.length > 0) payload.gemini_api_key = gemini;

  const res = await apiFetch('/api/v1/credentials', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  if (!res.ok) {
    alert('Credential validation error: ' + (data.detail || 'Invalid format'));
    return;
  }
  clearRoutingDirtyInputs();
  dirtyFormFields.delete('cred-wallet');
  dirtyFormFields.delete('cred-vault');
  document.getElementById('cred-pk').value = '';
  document.getElementById('cred-gemini').value = '';
  renderCredentials(data.credentials);
  await fetchInitialDashboard();
}

async function saveTelegramAlerts() {
  const token = document.getElementById('tg-token').value.trim();
  const chatId = document.getElementById('tg-chat-id').value.trim();
  const ddPct = parseFloat(document.getElementById('tg-dd-pct').value || '3.0') / 100.0;

  const payload = {
    max_daily_drawdown_alert_pct: ddPct
  };
  if (chatId.length > 0) payload.telegram_chat_id = chatId;
  if (token.length > 0) payload.telegram_bot_token = token;

  const res = await apiFetch('/api/v1/alerts', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  if (!res.ok) {
    alert('Telegram config error: ' + (data.detail || 'Invalid parameter'));
    return;
  }
  dirtyFormFields.delete('tg-chat-id');
  dirtyFormFields.delete('tg-dd-pct');
  document.getElementById('tg-token').value = '';
  document.getElementById('tg-chat-id').value = '';
  renderCredentials(data.alerts);
  await fetchInitialDashboard();
}

async function sendTestAlert() {
  const res = await apiFetch('/api/v1/alerts/test', { method: 'POST' });
  const data = await res.json();
  dirtyFormFields.delete('tg-chat-id');
  if (data.status === 'DELIVERED') {
    const cid = data.chat_id ? ` (Chat ID: ${data.chat_id})` : '';
    alert(`Telegram Alert Dispatch Status: DELIVERED${cid}`);
  } else {
    const detail = data.error_detail || (data.channels && data.channels[0] && data.channels[0].error) || data.reason || '';
    alert(`Telegram Alert Dispatch Status: ${data.status}${detail ? '\n\n' + detail : ''}`);
  }
  await fetchInitialDashboard();
}

async function triggerCycle() {
  const btn = document.getElementById('btn-cycle');
  btn.disabled = true;
  btn.textContent = 'Running Cycle...';
  try {
    const res = await apiFetch('/api/v1/engine/cycle', { method: 'POST' });
    const data = await res.json();
    if (data.dashboard) renderDashboard(data.dashboard);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Run Platform Cycle';
  }
}

async function triggerCalibration() {
  const btn = document.getElementById('btn-calibrate');
  btn.disabled = true;
  btn.textContent = 'Calibrating...';
  try {
    const res = await apiFetch('/api/v1/engine/calibrate', { method: 'POST' });
    const data = await res.json();
    document.getElementById('report-panel').style.display = 'flex';
    document.getElementById('report-title').textContent = 'Walk-Forward MACE Calibration Report';
    document.getElementById('report-content').textContent = replaceUtcTimestampsInText(JSON.stringify(data.calibration_report, null, 2));
    switchDashboardPage('overall');
    await fetchInitialDashboard();
  } finally {
    btn.disabled = false;
    btn.textContent = 'Calibrate MACE';
  }
}

async function triggerBacktest() {
  const btn = document.getElementById('btn-backtest');
  btn.disabled = true;
  btn.textContent = 'Simulating 365d...';
  try {
    const res = await apiFetch('/api/v1/engine/backtest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ days: 365, use_real_history: true })
    });
    const data = await res.json();
    document.getElementById('report-panel').style.display = 'flex';
    document.getElementById('report-title').textContent = '365-Day Walk-Forward Tear Sheet (0.35% Slippage + 1H Funding)';
    document.getElementById('report-content').textContent = replaceUtcTimestampsInText(data.formatted_report);
    switchDashboardPage('overall');
    await fetchInitialDashboard();
  } finally {
    btn.disabled = false;
    btn.textContent = 'Run 365d Tear Sheet';
  }
}

async function triggerTakeProfit(tradeId, symbol, btnEl) {
  if (btnEl) {
    btnEl.disabled = true;
    btnEl.textContent = 'Closing...';
  }
  try {
    const res = await apiFetch('/api/v1/trades/close', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ trade_id: Number(tradeId), reason: 'MANUAL_TAKE_PROFIT' })
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      alert(`Failed to close ${symbol || '#' + tradeId}: ${data.detail || data.error || res.statusText || 'Unknown error'}`);
      return;
    }
    if (data.dashboard) {
      renderDashboard(data.dashboard);
    } else {
      await fetchInitialDashboard();
    }
  } catch (err) {
    alert(`Failed to close ${symbol || '#' + tradeId}: ${err && err.message ? err.message : String(err)}`);
  } finally {
    if (btnEl) {
      btnEl.disabled = false;
      btnEl.textContent = 'Take Profit';
    }
  }
}

async function triggerPondCellTakeProfit(cellId, symbol, btnEl) {
  if (btnEl) {
    btnEl.disabled = true;
    btnEl.textContent = 'Closing...';
  }
  try {
    const res = await apiFetch('/api/v1/pond/cells/close', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cell_id: Number(cellId), reason: 'MANUAL_TAKE_PROFIT' })
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      alert(`Failed to close Pond cell ${symbol || '#' + cellId}: ${data.detail || data.error || res.statusText || 'Unknown error'}`);
      return;
    }
    if (data.dashboard) {
      renderDashboard(data.dashboard);
    } else {
      await fetchInitialDashboard();
    }
  } catch (err) {
    alert(`Failed to close Pond cell ${symbol || '#' + cellId}: ${err && err.message ? err.message : String(err)}`);
  } finally {
    if (btnEl) {
      btnEl.disabled = false;
      btnEl.textContent = 'Take Profit';
    }
  }
}

