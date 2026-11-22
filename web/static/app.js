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
  tableSort: {},
  closingPositionIds: new Set(),
  closingSymbols: new Set(),
  closeInFlightCount: 0,
  sse: null,
  authRequired: false,
  equityChartPoints: [],
  equityHoverIdx: null,
  vaultModal: {
    selectedAddress: '',
    isPreviewMode: false,
    previewVault: null,
    previewMeta: null,
    inFlightSave: false,
    inFlightInspect: false,
    inFlightSync: false,
  },
};

const OFFICIAL_HLP_TESTNET = '0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0';
const OFFICIAL_HLP_MAINNET = '0xdfc24b077bc1425ad1dea75bcb6f8158e10df303';

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
  'live_vault_info_json',
  'live_exchange_equity_synced_at_ms',
  'live_exchange_margin_used_usdc',
  'live_exchange_withdrawable_usdc',
  'live_exchange_open_orders_count',
  'live_exchange_spot_usdc',
  'live_exchange_spot_synced_at_ms',
  'live_exchange_account_mode',
  'live_exchange_perp_margin_equity_usdc',
  'live_exchange_portfolio_pnl_json',
]);

const SORT_COLLATOR = new Intl.Collator(undefined, {
  numeric: true,
  sensitivity: 'base',
});

const ROW_SORT_DATA = new WeakMap();

const SUPPORTS_INVOKER_COMMANDS =
  typeof HTMLButtonElement !== 'undefined' &&
  'commandForElement' in HTMLButtonElement.prototype &&
  'command' in HTMLButtonElement.prototype;

function toFiniteNumber(val) {
  if (val === null || val === undefined || val === '') return null;
  const n = Number(val);
  return Number.isFinite(n) ? n : null;
}

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

function fmtCompactUsd(val) {
  const n = toFiniteNumber(val);
  if (n === null) return '—';
  const abs = Math.abs(n);
  const sign = n < 0 ? '-' : '';
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(2)}M`;
  return fmtUsd(n, 2);
}

function isOfficialHlpAddress(rawAddr) {
  const norm = String(rawAddr || '').trim().toLowerCase();
  return norm === OFFICIAL_HLP_TESTNET || norm === OFFICIAL_HLP_MAINNET;
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

function fmtSignedPct(pct, decimals = 2) {
  const n = toFiniteNumber(pct);
  if (n === null) return null;
  const eps = 0.5 * Math.pow(10, -decimals);
  const sign = n >= eps ? '+' : n <= -eps ? '-' : '';
  return `${sign}${Math.abs(n).toFixed(decimals)}%`;
}

function pnlToneClass(val, eps = 0.001) {
  const n = toFiniteNumber(val);
  if (n === null) return 'pnl-flat';
  if (n > eps) return 'pnl-pos';
  if (n < -eps) return 'pnl-neg';
  return 'pnl-flat';
}

function computeOpenPnlPct(h) {
  const pnl = toFiniteNumber(h?.unrealized_pnl_usdc);
  const entryPx = toFiniteNumber(h?.entry_price);
  const qty = toFiniteNumber(h?.quantity ?? h?.filled_quantity);
  let notional =
    entryPx !== null && qty !== null && entryPx > 0 && Math.abs(qty) > 0
      ? entryPx * Math.abs(qty)
      : null;
  if (!(notional > 0)) {
    const committed = toFiniteNumber(h?.committed_notional_usdc);
    notional = committed !== null && committed > 0 ? committed : null;
  }

  const backendPct = toFiniteNumber(h?.unrealized_pnl_pct);
  if (backendPct !== null) {
    if (backendPct === 0 && pnl !== null && Math.abs(pnl) > 0.005 && !(notional > 0)) {
      return null;
    }
    return backendPct;
  }
  if (pnl === null || !(notional > 0)) return null;
  const pct = (pnl / notional) * 100.0;
  return Number.isFinite(pct) ? pct : null;
}

function computeClosedEntryNotional(p) {
  const entryPx = toFiniteNumber(p?.entry_price);
  const qty = toFiniteNumber(p?.filled_quantity ?? p?.quantity ?? p?.requested_quantity);
  if (entryPx !== null && qty !== null && entryPx > 0 && Math.abs(qty) > 0) {
    const prod = entryPx * Math.abs(qty);
    if (Number.isFinite(prod) && prod > 0) return prod;
  }
  const serverNotional = toFiniteNumber(p?.entry_notional_usdc);
  if (serverNotional !== null && serverNotional > 0) return serverNotional;
  const committed = toFiniteNumber(p?.committed_notional_usdc);
  if (committed !== null && committed > 0) return committed;
  return null;
}

function computeClosedPnlPct(p) {
  const pnl = toFiniteNumber(p?.realized_pnl_usdc);
  const notional = computeClosedEntryNotional(p);

  const backendPct = toFiniteNumber(p?.realized_pnl_pct);
  if (backendPct !== null) {
    if (backendPct === 0 && pnl !== null && Math.abs(pnl) > 0.005 && !(notional > 0)) {
      return null;
    }
    return backendPct;
  }
  if (pnl === null || !(notional > 0)) return null;
  const pct = (pnl / notional) * 100.0;
  return Number.isFinite(pct) ? pct : null;
}

function computeVaultPnlPct(v, pctKey, pnlKey) {
  const eq = toFiniteNumber(v?.equity_usdc);
  const pnl = toFiniteNumber(v?.[pnlKey]);
  const basis = eq !== null && pnl !== null ? eq - pnl : null;

  const backendPct = toFiniteNumber(v?.[pctKey]);
  if (backendPct !== null) {
    if (backendPct === 0 && pnl !== null && Math.abs(pnl) > 0.005 && !(basis > 1e-6)) {
      return null;
    }
    return backendPct;
  }
  if (pnl === null || !(basis > 1e-6)) return null;
  const pct = (pnl / basis) * 100.0;
  return Number.isFinite(pct) ? pct : null;
}

function populatePnlElement(el, pnlUsdc, pnlPct, extraClass = '') {
  const numPnl = toFiniteNumber(pnlUsdc);
  const tone = pnlToneClass(numPnl);
  el.className = extraClass ? `${extraClass} pnl-cell ${tone}` : `pnl-cell ${tone}`;
  const valSpan = document.createElement('span');
  valSpan.className = 'pnl-val';
  valSpan.textContent = numPnl !== null ? fmtSignedUsd(numPnl, 2) : '—';
  const pctStr = fmtSignedPct(pnlPct, 2);
  if (pctStr !== null && numPnl !== null) {
    const pctSpan = document.createElement('span');
    pctSpan.className = 'pnl-pct';
    pctSpan.textContent = `(${pctStr})`;
    el.replaceChildren(valSpan, document.createTextNode(' '), pctSpan);
  } else {
    el.replaceChildren(valSpan);
  }
}

function createAddressChip(rawAddress) {
  const full = String(rawAddress || '').trim();
  const code = document.createElement('code');
  code.className = 'addr-chip';
  if (!full) {
    code.textContent = '—';
    return code;
  }
  code.title = full;
  if (full.length <= 10) {
    code.textContent = full;
    return code;
  }
  // Keep full address intact in the DOM; CSS truncates the head while preserving the last 4 hex chars
  const head = document.createElement('span');
  head.className = 'addr-head';
  head.textContent = full.slice(0, -4);
  const tail = document.createTextNode(full.slice(-4));
  code.append(head, tail);
  return code;
}

/* ============================================================================
   ACCESSIBLE TYPE-AWARE TABLE SORTING ENGINE (SSE-PERSISTENT)
   ============================================================================ */

function parseSortValue(raw, type) {
  if (raw === null || raw === undefined || raw === '') return null;
  if (type === 'number' || type === 'date') {
    const n = Number(raw);
    return Number.isFinite(n) ? n : null;
  }
  const s = String(raw).trim();
  return s.length > 0 && s !== '—' ? s : null;
}

function compareSortValues(rawA, rawB, type, dir) {
  const a = parseSortValue(rawA, type);
  const b = parseSortValue(rawB, type);
  const aMissing = a === null;
  const bMissing = b === null;
  if (aMissing && bMissing) return 0;
  if (aMissing) return 1;
  if (bMissing) return -1;

  const factor = dir === 'ascending' ? 1 : -1;
  if (type === 'number' || type === 'date') {
    if (a === b) return 0;
    return factor * (a < b ? -1 : 1);
  }
  return factor * SORT_COLLATOR.compare(a, b);
}

function syncTableSortHeaders(tableId) {
  const table = document.getElementById(tableId);
  if (!table || !table.tHead || !table.tHead.rows[0]) return;
  const sortSpec = state.tableSort[tableId];
  for (const th of table.tHead.rows[0].cells) {
    const key = th.dataset.sortKey;
    if (sortSpec && key && key === sortSpec.key) {
      if (th.getAttribute('aria-sort') !== sortSpec.dir) {
        th.setAttribute('aria-sort', sortSpec.dir);
      }
    } else if (th.hasAttribute('aria-sort')) {
      th.removeAttribute('aria-sort');
    }
  }
}

function sortItemsForTable(items, tableId, extractors) {
  syncTableSortHeaders(tableId);
  const sortSpec = state.tableSort[tableId];
  if (!sortSpec || !extractors || typeof extractors[sortSpec.key] !== 'function') {
    return items;
  }
  const getter = extractors[sortSpec.key];
  const indexed = items.map((item, idx) => ({
    item,
    idx,
    val: getter(item),
  }));
  indexed.sort((a, b) => {
    const cmp = compareSortValues(a.val, b.val, sortSpec.type, sortSpec.dir);
    return cmp !== 0 ? cmp : a.idx - b.idx;
  });
  return indexed.map((entry) => entry.item);
}

function attachRowSortData(tbody, items, extractors) {
  if (!tbody || !Array.isArray(items)) return;
  const rows = tbody.rows;
  const keys = Object.keys(extractors);
  const count = Math.min(rows.length, items.length);
  for (let i = 0; i < count; i++) {
    const tr = rows[i];
    if (tr.dataset.emptyRow === 'true') continue;
    const item = items[i];
    const data = {};
    for (const key of keys) {
      data[key] = extractors[key](item);
    }
    ROW_SORT_DATA.set(tr, data);
  }
}

function reorderDomRows(tbody, orderedRows) {
  const canMoveBefore =
    typeof Element !== 'undefined' &&
    Object.hasOwn(Element.prototype, 'moveBefore');
  for (const row of orderedRows) {
    if (canMoveBefore) {
      tbody.moveBefore(row, null);
    } else {
      tbody.append(row);
    }
  }
}

function reorderTableRows(table, tbody, orderedRows) {
  const update = () => reorderDomRows(tbody, orderedRows);
  const reduceMotion =
    typeof window !== 'undefined' &&
    typeof window.matchMedia === 'function' &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (
    !reduceMotion &&
    table.dataset.vt === 'true' &&
    orderedRows.length > 1 &&
    orderedRows.length <= 80 &&
    typeof table.startViewTransition === 'function'
  ) {
    table.startViewTransition(update);
  } else {
    update();
  }
}

function rerenderTableById(tableId) {
  const dash = state.dashboard;
  if (!dash) return;
  switch (tableId) {
    case 'table-vaults':
      renderVaultPanel(dash);
      break;
    case 'table-priority-ordered':
      renderPriorityAndWorkingTable(dash, true);
      break;
    case 'table-assets-held':
      renderOpenHoldingsTable(dash, true);
      break;
    case 'table-closed-positions':
      renderClosedPositionsTable(dash);
      break;
    case 'table-cooldowns':
      renderCooldownsPanel(dash);
      break;
    case 'table-system-config':
    case 'table-modal-system-config':
      renderSystemConfigTables(true);
      break;
    default:
      renderActiveExplorerTab(false);
      break;
  }
}

function handleTableSortActivation(table, button) {
  if (!table || !button) return;
  const th = button.closest('th');
  if (!th || !th.dataset.sortKey) return;

  const tableId = table.id;
  const key = th.dataset.sortKey;
  const type = th.dataset.type || 'string';
  const prev = state.tableSort[tableId];
  let dir;
  if (prev && prev.key === key) {
    dir = prev.dir === 'ascending' ? 'descending' : 'ascending';
  } else {
    dir = type === 'number' || type === 'date' ? 'descending' : 'ascending';
  }

  state.tableSort[tableId] = { key, type, dir };
  syncTableSortHeaders(tableId);

  const tbody = table.tBodies[0];
  let rowCount = 0;
  if (tbody) {
    if (tbody.dataset.truncated === 'true' && state.dashboard) {
      rerenderTableById(tableId);
      rowCount = Array.from(tbody.rows).filter((r) => r.dataset.emptyRow !== 'true').length;
    } else {
      const dataRows = Array.from(tbody.rows).filter((r) => r.dataset.emptyRow !== 'true');
      rowCount = dataRows.length;
      if (dataRows.length > 1) {
        const keyed = dataRows.map((tr, idx) => ({
          tr,
          idx,
          val: ROW_SORT_DATA.get(tr)?.[key] ?? null,
        }));
        keyed.sort((a, b) => {
          const cmp = compareSortValues(a.val, b.val, type, dir);
          return cmp !== 0 ? cmp : a.idx - b.idx;
        });
        reorderTableRows(
          table,
          tbody,
          keyed.map((entry) => entry.tr)
        );
      }
    }
  }

  const statusEl = document.getElementById('table-sort-status');
  if (statusEl) {
    const tableLabel = table.caption?.textContent?.trim() || tableId;
    const colLabel = button.textContent.trim();
    statusEl.textContent = `Sorted ${tableLabel} by ${colLabel}, ${dir}, ${rowCount} rows`;
  }
}

function initSortableTables() {
  document.querySelectorAll('table.quant-table[id]').forEach((table) => {
    if (table.dataset.sortWired === 'true') return;
    table.dataset.sortWired = 'true';

    if (SUPPORTS_INVOKER_COMMANDS) {
      table.addEventListener('command', (evt) => {
        if (evt.command !== '--sort') return;
        const btn =
          evt.source && evt.source.closest
            ? evt.source.closest('button.sort-btn')
            : null;
        if (btn && table.contains(btn)) {
          handleTableSortActivation(table, btn);
        }
      });
    } else {
      table.addEventListener('click', (evt) => {
        const btn =
          evt.target && evt.target.closest
            ? evt.target.closest('button.sort-btn[command="--sort"]')
            : null;
        if (btn && table.contains(btn)) {
          handleTableSortActivation(table, btn);
        }
      });
    }
  });
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

function fmtLocalMonthDayTime(tsMs) {
  const n = toFiniteNumber(tsMs);
  if (n === null || n <= 0) return '';
  const d = new Date(n);
  if (Number.isNaN(d.getTime())) return '';
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const hh = String(d.getHours()).padStart(2, '0');
  const mi = String(d.getMinutes()).padStart(2, '0');
  return `${mm}/${dd} ${hh}:${mi}`;
}

function fmtCompactDuration(ms) {
  const safeMs = Math.max(0, Number(ms || 0));
  if (safeMs >= 86_400_000) {
    return `${Math.max(1, Math.round(safeMs / 86_400_000))}d`;
  }
  if (safeMs >= 3_600_000) {
    return `${Math.max(1, Math.round(safeMs / 3_600_000))}h`;
  }
  return `${Math.max(1, Math.round(safeMs / 60_000))}m`;
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
    k === 'hyperliquid_vault_address' ||
    k === 'hyperliquid_agent_address' ||
    k.startsWith('vault_') ||
    k === 'live_vault_info_json'
  ) {
    return 'wallet';
  }
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
  const heldAssets = (dash && dash.v_assets_held) || [];
  const heldIds = new Set(heldAssets.map((h) => Number(h.asset_id)));
  const heldInWatch = prioWatch.filter((p) => {
    const aid = Number(p.asset_id);
    const st = String(p.position_status || '').toUpperCase();
    return heldIds.has(aid) || st === 'OPEN';
  }).length;
  const prioEl = document.getElementById('prio-watch-count');
  if (prioEl) {
    prioEl.textContent =
      heldInWatch > 0
        ? `${prioWatch.length} (${heldInWatch} Held)`
        : String(prioWatch.length);
  }

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
  const totalEq =
    toFiniteNumber(dash?.equity_basis?.effective_equity_usdc) ??
    toFiniteNumber(dash?.snapshot?.total_equity_usdc) ??
    0;
  const capEpochMs = Number(dash?.risk_capital_cap_updated_at_ms ?? 0);

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
    const winStartMs = Number(h.window_start_ms ?? 0);
    const isClamped =
      Boolean(h.is_cap_clamped) ||
      (capEpochMs > 0 && winStartMs > 0 && capEpochMs > winStartMs);
    const clampTs = capEpochMs > 0 ? capEpochMs : winStartMs;

    const tone = netPnl > 0.005 ? 'pos' : netPnl < -0.005 ? 'neg' : 'flat';

    const cardEl = document.getElementById(`pnl-card-${key}`);
    const pctEl = document.getElementById(`pnl-${key}-pct`);
    const usdcEl = document.getElementById(`pnl-${key}-usdc`);
    const subEl = document.getElementById(`pnl-${key}-sub`);
    const clampEl = document.getElementById(`pnl-${key}-clamp`);
    const sourceEl = document.getElementById(`pnl-${key}-source`);

    if (cardEl) {
      cardEl.classList.remove('pos', 'neg', 'flat');
      cardEl.classList.add(tone);
    }
    if (sourceEl) {
      if (String(h.pnl_source || '').toUpperCase() === 'HL_PORTFOLIO') {
        sourceEl.textContent = 'HL Perps';
        sourceEl.setAttribute(
          'aria-label',
          'Hyperliquid Perps exchange-reconciled PnL'
        );
        sourceEl.hidden = false;
      } else {
        sourceEl.textContent = '';
        sourceEl.removeAttribute('aria-label');
        sourceEl.hidden = true;
      }
    }
    if (clampEl) {
      if (isClamped && clampTs > 0) {
        const dtLabel = fmtLocalMonthDayTime(clampTs);
        clampEl.textContent = dtLabel ? `Since Cap · ${dtLabel}` : 'Since Cap';
        clampEl.hidden = false;
      } else {
        clampEl.textContent = '';
        clampEl.hidden = true;
      }
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
      const leftSpan = document.createElement('span');
      leftSpan.textContent = `Realized: ${fmtSignedUsd(rpnl)} · Unrealized: ${fmtSignedUsd(upnl)}`;
      const rightSpan = document.createElement('span');
      rightSpan.textContent = tradeStr;
      subEl.replaceChildren(leftSpan, rightSpan);
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

function getConfigNonNegFloatFromDash(dash, key, fallback = 0) {
  const row = (dash?.system_config || []).find((r) => r.config_key === key);
  if (!row) return fallback;
  const n = toFiniteNumber(row.config_value);
  return n !== null && n >= 0 ? n : fallback;
}

function getConfigValueFromDash(dash, key, fallback = '') {
  const row = (dash?.system_config || []).find((r) => r.config_key === key);
  if (!row || row.config_value === null || row.config_value === undefined) {
    return fallback;
  }
  return String(row.config_value);
}

function createWalletStripItem(label, valText) {
  const item = document.createElement('span');
  item.className = 'kpi-wallet-item';
  const lbl = document.createElement('span');
  lbl.className = 'kpi-wallet-label';
  lbl.textContent = label;
  const val = document.createElement('span');
  val.className = 'kpi-wallet-val';
  val.textContent = valText;
  item.append(lbl, val);
  return item;
}

function renderKpiCards(dash) {
  const snap = (dash && dash.snapshot) || {};
  const eb = (dash && dash.equity_basis) || {};
  const dd = (dash && dash.drawdown_breaker) || {};
  const execMode = String(dash?.execution_mode || '').toUpperCase();
  const isLiveOrTestnet = execMode === 'LIVE' || execMode === 'TESTNET';

  const totalEq =
    toFiniteNumber(eb.effective_equity_usdc) ??
    toFiniteNumber(snap.total_equity_usdc) ??
    0;
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

  // Equity KPI card title, Option B effective figure, wallet strip & sync status
  const isCapBinding = Boolean(eb.is_cap_binding);
  const isUnified = Boolean(eb.is_unified_account);
  const perpMarginEq = toFiniteNumber(eb.perp_margin_equity_usdc) ?? 0;
  setTxt(
    'kpi-equity-title',
    isCapBinding
      ? 'Trading Equity (Cap Active)'
      : isUnified
      ? 'Trading Equity (Unified)'
      : isLiveOrTestnet
      ? 'Trading Equity (Perp)'
      : 'Total Portfolio Equity'
  );
  setTxt('kpi-total-equity', fmtUsd(totalEq));

  const walletEl = document.getElementById('kpi-wallet-summary');
  const syncEl = document.getElementById('kpi-equity-sync-sub');
  const perpWalletEq = toFiniteNumber(eb.perp_wallet_equity_usdc) ?? totalEq;
  const spotUsdc = toFiniteNumber(eb.spot_usdc) ?? 0;
  const vaultEq = toFiniteNumber(dash?.vault_info?.total_vault_equity_usdc) ?? 0;
  const totalWalletEq =
    toFiniteNumber(eb.total_wallet_equity_usdc) ??
    (isUnified ? perpWalletEq + vaultEq : perpWalletEq + spotUsdc + vaultEq);
  const capUsdc = toFiniteNumber(eb.risk_capital_cap_usdc) ?? 0;
  const syncedAtMs = toFiniteNumber(eb.synced_at_ms) ?? 0;
  const isStale = Boolean(eb.is_stale);

  if (walletEl) {
    if (isLiveOrTestnet) {
      const items = isUnified
        ? [createWalletStripItem('Unified Wallet:', fmtUsd(perpWalletEq))]
        : [createWalletStripItem('Perp Wallet:', fmtUsd(perpWalletEq))];
      if (isUnified) {
        if (perpMarginEq > 0.005) {
          items.push(createWalletStripItem('Perp Margin:', fmtUsd(perpMarginEq)));
        }
        if (vaultEq > 0.005) {
          items.push(createWalletStripItem('Total Wallet:', fmtUsd(totalWalletEq)));
        }
      } else {
        if (spotUsdc > 0.005) {
          items.push(createWalletStripItem('Spot:', fmtUsd(spotUsdc)));
        }
        if (spotUsdc > 0.005 || vaultEq > 0.005) {
          items.push(createWalletStripItem('Total Wallet:', fmtUsd(totalWalletEq)));
        }
      }
      walletEl.replaceChildren(...items);
      walletEl.hidden = false;
    } else {
      walletEl.replaceChildren();
      walletEl.hidden = true;
    }
  }

  if (syncEl) {
    if (isLiveOrTestnet) {
      const capSpan = document.createElement('span');
      capSpan.textContent =
        capUsdc >= 10 ? `Cap: ${fmtUsd(capUsdc)}` : 'Uncapped Perp';

      const syncSpan = document.createElement('span');
      syncSpan.className = 'kpi-sync-status';
      const syncTimeStr =
        syncedAtMs > 0 ? `Synced ${fmtLocalShortTime(syncedAtMs)}` : 'Awaiting Sync';
      syncSpan.append(document.createTextNode(syncTimeStr));
      if (isStale) {
        const stalePill = document.createElement('span');
        stalePill.className = 'sync-stale-pill';
        stalePill.textContent = 'STALE';
        syncSpan.append(stalePill);
      }

      syncEl.replaceChildren(capSpan, syncSpan);
      syncEl.hidden = false;
    } else {
      syncEl.replaceChildren();
      syncEl.hidden = true;
    }
  }

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

  const workingCnt = Number(
    snap.working_order_count ?? (dash.v_assets_ordered || []).length
  );
  const attachedTpSl = Number(dash.attached_tpsl_order_count ?? 0);
  const exOpenOrders = Number(
    dash.exchange_open_orders_count ?? workingCnt + attachedTpSl
  );
  const resPct = totalEq > 0 ? (reservedMargin / totalEq) * 100.0 : 0.0;
  setTxt('kpi-reserved-margin', fmtUsd(reservedMargin));
  setTxt('kpi-working-count', `${workingCnt} Working Entry ALO`);
  setTxt(
    'kpi-exchange-orders-sub',
    isLiveOrTestnet || attachedTpSl > 0
      ? `${attachedTpSl} Attached SL/TP · ${exOpenOrders} HL Open`
      : '1s Maker Chaser'
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

  const ddPill = document.getElementById('kpi-drawdown-pill');
  if (ddPill) {
    if (dd.tripped) {
      const ddPctVal = Math.abs(Number(dd.drawdown_pct ?? 0)).toFixed(1);
      const ddCapVal = Number(dd.max_daily_drawdown_pct ?? 15.0).toFixed(1);
      ddPill.textContent = `DD HALT -${ddPctVal}% / ${ddCapVal}%`;
      ddPill.hidden = false;
    } else {
      ddPill.textContent = '';
      ddPill.hidden = true;
    }
  }

  setTxt('kpi-regime-label', regime);
  setTxt('kpi-breadth-pct', `Bull Breadth: ${(breadth * 100.0).toFixed(1)}%`);
  setTxt('kpi-beta-exp', `Beta: ${beta >= 0 ? '+' : ''}${beta.toFixed(2)}`);
  setWidth('kpi-breadth-meter', breadth * 100.0);

  // Sleeve attribution stat boxes (gross % of equity + relative share)
  const s1Gross = Math.abs(toFiniteNumber(snap.s1_gross_weight) ?? 0);
  const s2Gross = Math.abs(toFiniteNumber(snap.s2_gross_weight) ?? 0);
  const s3Gross = Math.abs(toFiniteNumber(snap.s3_gross_weight) ?? 0);
  const sleeveTotal = s1Gross + s2Gross + s3Gross;
  setTxt('sleeve-total-gross', `Total Gross: ${(sleeveTotal * 100.0).toFixed(1)}%`);

  const fmtSleeveBox = (w) => {
    const grossPct = (w * 100.0).toFixed(1);
    if (sleeveTotal > 1e-6) {
      const sharePct = ((w / sleeveTotal) * 100.0).toFixed(1);
      return `${grossPct}% · ${sharePct}% share`;
    }
    return `${grossPct}%`;
  };
  setTxt('sleeve-s1-weight', fmtSleeveBox(s1Gross));
  setTxt('sleeve-s2-weight', fmtSleeveBox(s2Gross));
  setTxt('sleeve-s3-weight', fmtSleeveBox(s3Gross));
}

/* ============================================================================
   PER-TABLE SORT EXTRACTORS & VAULT / SURVEILLANCE / TRADE HISTORY RENDERERS
   ============================================================================ */

const VAULT_SORT_EXTRACTORS = {
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
  symbol: (h) => String(h?.symbol ?? ''),
  side: (h) => String(h?.side ?? ''),
  committed_notional_usdc: (h) => toFiniteNumber(h?.committed_notional_usdc),
  entry_price: (h) => toFiniteNumber(h?.entry_price),
  protective_stop_px: (h) => {
    const px = toFiniteNumber(h?.protective_stop_px);
    return px !== null && px > 0 ? px : null;
  },
  take_profit_px: (h) => {
    const px = toFiniteNumber(h?.take_profit_px);
    return px !== null && px > 0 ? px : null;
  },
  unrealized_pnl_usdc: (h) => toFiniteNumber(h?.unrealized_pnl_usdc),
  sleeve_mix: (h) =>
    Math.abs(toFiniteNumber(h?.s1_weight_contrib) ?? 0) +
    Math.abs(toFiniteNumber(h?.s2_weight_contrib) ?? 0) +
    Math.abs(toFiniteNumber(h?.s3_weight_contrib) ?? 0),
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

const CONFIG_SORT_EXTRACTORS = {
  config_key: (c) => String(c?.config_key ?? ''),
  config_value: (c) => String(c?.config_value ?? ''),
  value_type: (c) => String(c?.value_type ?? ''),
  description: (c) => String(c?.description ?? ''),
  updated_at_ms: (c) => {
    const ts = toFiniteNumber(c?.updated_at_ms);
    return ts !== null && ts > 0 ? ts : null;
  },
};

function renderVaultPanel(dash) {
  const tbody = document.getElementById('tbody-vaults');
  if (!tbody) return;

  const vi = (dash && dash.vault_info) || {};
  const eb = (dash && dash.equity_basis) || {};
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
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  };

  // Configured vault badge in header
  const cfgBadge = document.getElementById('vault-configured-badge');
  if (cfgBadge) {
    if (configuredAddr) {
      cfgBadge.hidden = false;
      const lbl = document.createElement('span');
      lbl.textContent = 'Configured:';
      cfgBadge.replaceChildren(lbl, createAddressChip(configuredAddr));
    } else {
      cfgBadge.hidden = true;
      cfgBadge.replaceChildren();
    }
  }

  // Sync pill in header
  const syncPill = document.getElementById('vault-sync-pill');
  if (syncPill) {
    const countLabel = `${vaultCount} ${vaultCount === 1 ? 'VAULT' : 'VAULTS'}`;
    const shortSync = syncedAtMs > 0 ? fmtLocalShortTime(syncedAtMs) : '';
    syncPill.textContent = shortSync ? `${countLabel} · ${shortSync}` : countLabel;
    syncPill.title = syncedAtMs > 0 ? `Last synced: ${fmtLocalTime(syncedAtMs)}` : 'No vault sync recorded yet';
  }

  // KPI cards
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

  // Aggregate vault PnL & weighted APR
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

  const totalPnlEl = document.getElementById('vault-total-pnl');
  if (totalPnlEl) {
    populatePnlElement(
      totalPnlEl,
      rawVaults.length > 0 ? sumPnl : 0,
      rawVaults.length > 0 ? aggPnlPct : null,
      'vault-kpi-value'
    );
  }

  const allTimeEl = document.getElementById('vault-alltime-pnl');
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

  // Sort vault rows using persistent sort state
  const sortedVaults = sortItemsForTable(rawVaults, 'table-vaults', VAULT_SORT_EXTRACTORS);
  tbody.dataset.truncated = 'false';

  if (sortedVaults.length === 0) {
    const tr = document.createElement('tr');
    tr.dataset.emptyRow = 'true';
    const td = document.createElement('td');
    td.colSpan = 7;
    td.className = 'table-empty-cell';
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
    const tr = document.createElement('tr');

    // 1. Vault Name + truncated address with full address tooltip (full address kept in DOM)
    const tdName = document.createElement('td');
    const nameWrap = document.createElement('div');
    nameWrap.className = 'vault-name-cell';
    const nameSpan = document.createElement('span');
    nameSpan.className = 'sym-badge';
    nameSpan.textContent = String(v?.name || 'Hyperliquid Vault');
    if (v?.leader) {
      nameSpan.title = `Leader: ${String(v.leader)}`;
    }
    nameWrap.appendChild(nameSpan);
    if (v?.is_closed) {
      const closedBadge = document.createElement('span');
      closedBadge.className = 'vault-closed-badge';
      closedBadge.textContent = 'CLOSED';
      nameWrap.appendChild(closedBadge);
    }
    if (v?.vault_address) {
      nameWrap.appendChild(createAddressChip(v.vault_address));
    }
    const inspectBtn = document.createElement('button');
    inspectBtn.type = 'button';
    inspectBtn.className = 'vault-row-inspect-btn';
    inspectBtn.dataset.action = 'inspect-vault';
    inspectBtn.dataset.vaultAddress = String(v?.vault_address || '');
    inspectBtn.setAttribute(
      'aria-label',
      `Inspect live facts for ${String(v?.name || v?.vault_address || 'Hyperliquid Vault')}`
    );
    inspectBtn.textContent = 'Facts';
    nameWrap.appendChild(inspectBtn);
    tdName.appendChild(nameWrap);

    // 2. Equity USDC
    const tdEq = document.createElement('td');
    tdEq.className = 'num-cell cell-committed';
    tdEq.textContent = fmtUsd(v?.equity_usdc ?? 0, 2);

    // 3. PnL ($ and %)
    const tdPnl = document.createElement('td');
    populatePnlElement(
      tdPnl,
      v?.pnl_usdc,
      computeVaultPnlPct(v, 'pnl_pct', 'pnl_usdc')
    );

    // 4. All-Time PnL ($ and %)
    const tdAtPnl = document.createElement('td');
    populatePnlElement(
      tdAtPnl,
      v?.all_time_pnl_usdc,
      computeVaultPnlPct(v, 'all_time_pnl_pct', 'all_time_pnl_usdc')
    );

    // 5. APR %
    const tdApr = document.createElement('td');
    const aprVal = toFiniteNumber(v?.apr_pct);
    const aprStr = fmtSignedPct(aprVal, 2);
    tdApr.className = `num-cell ${pnlToneClass(aprVal, 0.005)}`;
    tdApr.textContent = aprStr ?? '—';

    // 6. Days Following
    const tdDays = document.createElement('td');
    const daysVal = toFiniteNumber(v?.days_following);
    tdDays.className = 'num-cell';
    tdDays.textContent =
      daysVal !== null ? `${Math.max(0, Math.floor(daysVal))}d` : '—';

    // 7. Lockup Status
    const tdLock = document.createElement('td');
    const lockedUntil = toFiniteNumber(v?.locked_until_ms) ?? 0;
    const pill = document.createElement('span');
    if (v?.is_locked) {
      pill.className = 'status-pill working';
      pill.textContent =
        lockedUntil > 0
          ? `Locked · ${fmtLocalTime(lockedUntil)}`
          : 'Locked';
    } else {
      pill.className = 'status-pill open';
      pill.textContent = 'Unlocked';
      if (lockedUntil > 0) {
        pill.title = `Unlocked since ${fmtLocalTime(lockedUntil)}`;
      }
    }
    tdLock.appendChild(pill);

    tr.append(tdName, tdEq, tdPnl, tdAtPnl, tdApr, tdDays, tdLock);
    return tr;
  });

  tbody.replaceChildren(...rows);
  attachRowSortData(tbody, sortedVaults, VAULT_SORT_EXTRACTORS);
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
  const heldInWatch = prio.filter((p) => {
    const aid = Number(p.asset_id);
    const st = String(p.position_status || '').toUpperCase();
    return heldAssetIds.has(aid) || st === 'OPEN';
  }).length;
  const splitNoteEl = document.getElementById('watch-split-note');
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
    if (passiveWatched.length > maxPassiveDefault) {
      toggleBtn.style.display = 'inline-flex';
      toggleBtn.textContent = state.showAllPriorityWatch
        ? `Show Top ${maxPassiveDefault}`
        : `Show All Watchlist (${ordered.length + passiveWatched.length})`;
    } else {
      toggleBtn.style.display = 'none';
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

  const isSorted = Boolean(state.tableSort['table-priority-ordered']);
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
    tbody.innerHTML = `
      <tr data-empty-row="true">
        <td colspan="8" class="table-empty-cell">
          No active WORKING orders or priority triggers — monitoring 1s watchlist stream...
        </td>
      </tr>
    `;
    return;
  }

  const rowsHtml = visibleEntries.map((entry) => {
    if (entry.kind === 'working') {
      const o = entry.item;
      const sideCls = String(o.side).toUpperCase() === 'SHORT' ? 'short' : 'long';
      const hasTpsl = Number(o.protective_stop_px || 0) > 0 || Number(o.take_profit_px || 0) > 0;
      const tpslHint = hasTpsl
        ? `<br><small style="color:var(--text-muted)">SL ${fmtPrice(o.protective_stop_px)} · TP ${fmtPrice(o.take_profit_px)}</small>`
        : '';
      return `
        <tr>
          <td><span class="sym-badge">${escapeHtml(o.symbol)}</span></td>
          <td><span class="status-pill working">${escapeHtml(o.fsm_state || 'WORKING_ALO')}</span></td>
          <td><span class="side-pill ${sideCls}">${escapeHtml(o.side)}</span></td>
          <td class="num-cell">${fmtPrice(o.limit_price)} <small style="color:var(--text-muted)">/ ${fmtPrice(o.mark_price)}</small>${tpslHint}</td>
          <td class="num-cell" style="color: var(--neon-cyan); font-weight: 700;">${Number(o.distance_from_mid_bps || 0).toFixed(2)} bps</td>
          <td class="num-cell" style="color: #fbbf24; font-weight: 700;">${fmtUsd(o.reserved_margin_usdc)}</td>
          <td class="num-cell"><span class="tab-count">${Number(o.chase_count || 0)}</span></td>
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
      `;
    }
    const p = entry.item;
    const statusLabel = p.event_trigger || 'PRIORITY_WATCH';
    return `
      <tr>
        <td><span class="sym-badge">${escapeHtml(p.symbol)}</span></td>
        <td><span class="status-pill watch">${escapeHtml(statusLabel)}</span></td>
        <td><span class="pnl-flat">—</span></td>
        <td class="num-cell">${fmtPrice(p.mark_price)} <small style="color:var(--text-muted)">/ ${fmtPrice(p.mid_price || p.mark_price)}</small></td>
        <td class="num-cell">0.00 bps</td>
        <td class="num-cell">$0.00</td>
        <td><span class="tab-count">1s</span></td>
        <td><span class="pnl-flat">1s Watch</span></td>
      </tr>
    `;
  });

  tbody.innerHTML = rowsHtml.join('');
  attachRowSortData(tbody, visibleEntries, PRIORITY_ORDERED_SORT_EXTRACTORS);
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
  const heldFiltered = rawHeld.filter(
    (h) =>
      !state.closingPositionIds.has(Number(h.position_id)) &&
      !state.closingSymbols.has(String(h.symbol))
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
    emptyRow.dataset.emptyRow = 'true';
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
    const isShort = sideCls === 'short';
    const upnl = toFiniteNumber(h.unrealized_pnl_usdc) ?? 0;
    const upnlPct = computeOpenPnlPct(h);
    const stopPx = toFiniteNumber(h.protective_stop_px);
    const markPx = toFiniteNumber(h.mark_price);
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
    } else if (h.stop_distance_pct !== null && h.stop_distance_pct !== undefined) {
      stopDistDisplay = `(${Number(h.stop_distance_pct).toFixed(2)}%)`;
    }
    const tpNum = toFiniteNumber(h.tp_distance_pct);
    const tpDist =
      tpNum !== null
        ? `(${tpNum >= 0 ? '+' : ''}${tpNum.toFixed(2)}%)`
        : '(—)';
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
    tdCommitted.className = 'num-cell cell-committed';
    tdCommitted.append(
      document.createTextNode(`${fmtUsd(h.committed_notional_usdc)} `),
      createTableSmall('cell-muted-sub', `(${fmtPct(h.portfolio_weight || 0)})`)
    );

    const tdEntryMark = document.createElement('td');
    tdEntryMark.className = 'num-cell';
    tdEntryMark.append(
      document.createTextNode(`${fmtPrice(h.entry_price)} `),
      createTableSmall('cell-mark-price', `→ ${fmtPrice(h.mark_price)}`)
    );

    const tdSl = document.createElement('td');
    tdSl.className = 'num-cell';
    const slBadge = createSleeveTag(
      slOid ? 's1' : 's3',
      slOid ? `Exchange SL Order: ${slOid}` : 'SL Order Pending',
      slOid ? 'SL' : 'SL?'
    );
    tdSl.append(
      document.createTextNode(`${fmtPrice(h.protective_stop_px)} `),
      createTableSmall(
        isStopBreached ? 'cell-sl-breached' : 'cell-sl-dist',
        stopDistDisplay
      ),
      document.createTextNode(' '),
      slBadge
    );

    const tdTp = document.createElement('td');
    tdTp.className = 'num-cell';
    const tpPxText = Number(h.take_profit_px || 0) > 0 ? fmtPrice(h.take_profit_px) : '—';
    const tpBadge = createSleeveTag(
      tpOid ? 's2' : 's3',
      tpOid ? `Exchange TP Order: ${tpOid}` : 'TP Order Pending',
      tpOid ? 'TP' : 'TP?'
    );
    tdTp.append(
      document.createTextNode(`${tpPxText} `),
      createTableSmall('cell-tp-dist', tpDist),
      document.createTextNode(' '),
      tpBadge
    );

    const tdPnl = document.createElement('td');
    populatePnlElement(tdPnl, upnl, upnlPct);

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
  attachRowSortData(tbody, held, HELD_SORT_EXTRACTORS);
}

function renderClosedPositionsTable(dash) {
  const tbody = document.getElementById('tbody-closed-positions');
  const pill = document.getElementById('closed-count-pill');
  if (!tbody) return;

  const rawClosed =
    (dash && (dash.recent_closed_positions || dash.closed_positions)) || [];
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

  const query = (document.getElementById('closed-search-input')?.value || '')
    .trim()
    .toUpperCase();
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

  const sorted = sortItemsForTable(
    filtered,
    'table-closed-positions',
    CLOSED_SORT_EXTRACTORS
  );
  tbody.dataset.truncated = 'false';

  if (sorted.length === 0) {
    const emptyRow = document.createElement('tr');
    emptyRow.dataset.emptyRow = 'true';
    const emptyCell = document.createElement('td');
    emptyCell.colSpan = 8;
    emptyCell.className = 'table-empty-cell';
    emptyCell.textContent = query
      ? 'No closed trades matching search filter.'
      : 'No closed trades recorded yet.';
    emptyRow.appendChild(emptyCell);
    tbody.replaceChildren(emptyRow);
    return;
  }

  const rows = sorted.map((p) => {
    const tr = document.createElement('tr');

    const tdTime = document.createElement('td');
    tdTime.className = 'num-cell';
    tdTime.textContent = fmtLocalTime(p.closed_at_ms ?? p.updated_at_ms);

    const tdSym = document.createElement('td');
    const symBadge = document.createElement('span');
    symBadge.className = 'sym-badge';
    symBadge.textContent = String(p._resolved_symbol || '—');
    tdSym.appendChild(symBadge);

    const tdSide = document.createElement('td');
    const sideStr = String(p.side || 'LONG');
    const sidePill = document.createElement('span');
    sidePill.className = `side-pill ${sideStr.toUpperCase() === 'SHORT' ? 'short' : 'long'}`;
    sidePill.textContent = sideStr;
    tdSide.appendChild(sidePill);

    const notional = computeClosedEntryNotional(p);
    const tdNotional = document.createElement('td');
    tdNotional.className = 'num-cell cell-committed';
    tdNotional.textContent = notional !== null && notional > 0 ? fmtUsd(notional, 2) : '—';

    const tdPx = document.createElement('td');
    tdPx.className = 'num-cell';
    const entryPx = toFiniteNumber(p.entry_price);
    const exitPx = toFiniteNumber(p.exit_price ?? p.current_price);
    tdPx.append(
      document.createTextNode(`${entryPx !== null && entryPx > 0 ? fmtPrice(entryPx) : '—'} `),
      createTableSmall(
        'cell-mark-price',
        `→ ${exitPx !== null && exitPx > 0 ? fmtPrice(exitPx) : '—'}`
      )
    );

    const tdRpnl = document.createElement('td');
    populatePnlElement(tdRpnl, p.realized_pnl_usdc, computeClosedPnlPct(p));

    const netFeesFunding =
      (toFiniteNumber(p.funding_accrued_usdc) ?? 0) -
      (toFiniteNumber(p.fees_paid_usdc) ?? 0);
    const tdFees = document.createElement('td');
    tdFees.className = `num-cell ${pnlToneClass(netFeesFunding, 0.001)}`;
    tdFees.textContent = fmtSignedUsd(netFeesFunding, 2);

    const tdReason = document.createElement('td');
    tdReason.className = 'reason-cell';
    const reasonText = String(p.exit_reason || '').trim() || '—';
    tdReason.title = reasonText;
    tdReason.textContent = reasonText;

    tr.append(tdTime, tdSym, tdSide, tdNotional, tdPx, tdRpnl, tdFees, tdReason);
    return tr;
  });

  tbody.replaceChildren(...rows);
  attachRowSortData(tbody, sorted, CLOSED_SORT_EXTRACTORS);
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
  const tbody = document.getElementById('tbody-cooldowns');
  const pillEl = document.getElementById('cooldown-count-pill');
  if (!tbody) return;

  const rawCooldowns = (dash && dash.v_assets_in_cooldown) || [];
  if (pillEl) {
    pillEl.textContent = `${rawCooldowns.length} ACTIVE`;
  }

  const cooldowns = sortItemsForTable(
    rawCooldowns,
    'table-cooldowns',
    COOLDOWN_SORT_EXTRACTORS
  );
  tbody.dataset.truncated = 'false';

  if (cooldowns.length === 0) {
    const emptyRow = document.createElement('tr');
    emptyRow.dataset.emptyRow = 'true';
    const emptyCell = document.createElement('td');
    emptyCell.colSpan = 5;
    emptyCell.className = 'table-empty-cell';
    emptyCell.textContent = 'No assets currently in cooldown.';
    emptyRow.appendChild(emptyCell);
    tbody.replaceChildren(emptyRow);
    return;
  }

  const rows = cooldowns.map((c) => {
    const tr = document.createElement('tr');

    const tdSym = document.createElement('td');
    const symBadge = document.createElement('span');
    symBadge.className = 'sym-badge';
    symBadge.textContent = String(c.symbol ?? '');
    tdSym.appendChild(symBadge);

    const tdType = document.createElement('td');
    const typeTag = document.createElement('span');
    typeTag.className = 'sp-tag s3';
    typeTag.textContent = String(c.cooldown_type || 'COOLDOWN');
    tdType.appendChild(typeTag);

    const tdRem = document.createElement('td');
    tdRem.className = 'num-cell cell-sl-dist';
    tdRem.textContent = `${formatCooldownRemaining(c.remaining_seconds)} left`;

    const tdExp = document.createElement('td');
    tdExp.className = 'num-cell';
    tdExp.textContent = fmtLocalTime(c.expires_at_ms);

    const tdReason = document.createElement('td');
    tdReason.className = 'reason-cell';
    const reasonStr = String(c.reason || '').trim() || '—';
    tdReason.title = reasonStr;
    tdReason.textContent = reasonStr;

    tr.append(tdSym, tdType, tdRem, tdExp, tdReason);
    return tr;
  });

  tbody.replaceChildren(...rows);
  attachRowSortData(tbody, cooldowns, COOLDOWN_SORT_EXTRACTORS);
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
  } else if (key === 'vault_role') {
    enumOptions = ['follower', 'operator', 'none'];
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
          <td><code class="cfg-val-code" title="${escapeHtml(c.config_value)}">${escapeHtml(c.config_value)}</code></td>
          <td>${buildInlineConfigControlHtml(c, prefix, idx)}</td>
          <td><span class="tab-count">${escapeHtml(c.value_type)}</span></td>
          <td style="color:var(--text-muted); white-space: normal; max-width: 340px;">${escapeHtml(c.description)}</td>
          <td class="num-cell">${fmtLocalTime(c.updated_at_ms)}</td>
        </tr>
      `
      )
      .join('') ||
    '<tr data-empty-row="true"><td colspan="6" class="table-empty-cell">No configuration keys matching filter.</td></tr>';

  const tbTab = document.getElementById('tbody-system-config');
  if (tbTab) {
    const tabRows = sortItemsForTable(
      filterRows(tabQuery),
      'table-system-config',
      CONFIG_SORT_EXTRACTORS
    );
    tbTab.dataset.truncated = 'false';
    tbTab.innerHTML = renderRows(tabRows, 'tab');
    attachRowSortData(tbTab, tabRows, CONFIG_SORT_EXTRACTORS);
  }

  const tbModal = document.getElementById('tbody-modal-system-config');
  if (tbModal) {
    const modalRows = sortItemsForTable(
      filterRows(modalQuery),
      'table-modal-system-config',
      CONFIG_SORT_EXTRACTORS
    );
    tbModal.dataset.truncated = 'false';
    tbModal.innerHTML = renderRows(modalRows, 'modal');
    attachRowSortData(tbModal, modalRows, CONFIG_SORT_EXTRACTORS);
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
    const sortedS1 = sortItemsForTable(s1, 'table-sleeve-s1', S1_SORT_EXTRACTORS);
    const slicedS1 = sortedS1.slice(0, 60);
    tbS1.dataset.truncated = sortedS1.length > slicedS1.length ? 'true' : 'false';
    tbS1.innerHTML =
      slicedS1
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
              <td class="num-cell">${fmtPrice(r.mark_price)}</td>
              <td class="num-cell">${fmtUsd(r.day_volume_usdc, 0)}</td>
              <td class="num-cell">${(Number(r.funding_rate_hourly || 0) * 100).toFixed(4)}%</td>
              <td class="num-cell ${r.funding_apr_pct >= 0 ? 'pnl-pos' : 'pnl-neg'}">${Number(r.funding_apr_pct || 0).toFixed(2)}%</td>
              <td class="num-cell">${(Number(r.annualized_vol || 0) * 100).toFixed(1)}%</td>
              <td class="num-cell">${Number(r.market_beta ?? 1).toFixed(2)}</td>
              <td class="num-cell">${Number(r.s1_mom_score || 0).toFixed(3)}</td>
              <td class="num-cell">${Number(r.s1_carry_score || 0).toFixed(3)}</td>
              <td class="num-cell ${wCls}">${fmtPct(r.w_s1_carry_mom)}</td>
              <td class="num-cell" style="font-weight:700;color:#fff;">${fmtPct(r.target_weight)}</td>
            </tr>
          `;
        })
        .join('') ||
      '<tr data-empty-row="true"><td colspan="11" class="table-empty-cell">No Sleeve 1 assets matching filter.</td></tr>';
    attachRowSortData(tbS1, slicedS1, S1_SORT_EXTRACTORS);
  }

  // Render S2
  const tbS2 = document.getElementById('tbody-sleeve-s2');
  if (tbS2) {
    const sortedS2 = sortItemsForTable(s2, 'table-sleeve-s2', S2_SORT_EXTRACTORS);
    const slicedS2 = sortedS2.slice(0, 60);
    tbS2.dataset.truncated = sortedS2.length > slicedS2.length ? 'true' : 'false';
    tbS2.innerHTML =
      slicedS2
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
              <td class="num-cell">${fmtPrice(r.mark_price)}</td>
              <td class="num-cell">${fmtUsd(r.day_volume_usdc, 0)}</td>
              <td class="num-cell">${(Number(r.annualized_vol || 0) * 100).toFixed(1)}%</td>
              <td>${alignBadge}</td>
              <td class="num-cell" style="color:var(--neon-cyan);font-weight:700;">${Number(r.s2_vol_surge_ratio ?? 1).toFixed(2)}x</td>
              <td>${r.event_trigger ? `<span class="status-pill watch">${escapeHtml(r.event_trigger)}</span>` : '—'}</td>
              <td class="num-cell ${wCls}">${fmtPct(r.w_s2_trend_breakout)}</td>
              <td class="num-cell" style="font-weight:700;color:#fff;">${fmtPct(r.target_weight)}</td>
              <td class="num-cell">${fmtPct(r.current_weight)}</td>
            </tr>
          `;
        })
        .join('') ||
      '<tr data-empty-row="true"><td colspan="10" class="table-empty-cell">No Sleeve 2 assets matching filter.</td></tr>';
    attachRowSortData(tbS2, slicedS2, S2_SORT_EXTRACTORS);
  }

  // Render S3
  const tbS3 = document.getElementById('tbody-sleeve-s3');
  if (tbS3) {
    const sortedS3 = sortItemsForTable(s3, 'table-sleeve-s3', S3_SORT_EXTRACTORS);
    const slicedS3 = sortedS3.slice(0, 60);
    tbS3.dataset.truncated = sortedS3.length > slicedS3.length ? 'true' : 'false';
    tbS3.innerHTML =
      slicedS3
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
              <td class="num-cell">${fmtPrice(r.mark_price)}</td>
              <td class="num-cell">${fmtUsd(r.day_volume_usdc, 0)}</td>
              <td class="num-cell">${Number(r.funding_apr_pct || 0).toFixed(2)}%</td>
              <td class="num-cell" style="color:#fbbf24;font-weight:700;">${Number(r.s3_funding_zscore || 0).toFixed(2)}σ</td>
              <td class="num-cell">${Number(r.s3_flush_atr_mult || 0).toFixed(2)} ATR</td>
              <td>${r.event_trigger ? `<span class="status-pill working">${escapeHtml(r.event_trigger)}</span>` : '—'}</td>
              <td class="num-cell ${wCls}">${fmtPct(r.w_s3_event_reversion)}</td>
              <td class="num-cell" style="font-weight:700;color:#fff;">${fmtPct(r.target_weight)}</td>
              <td class="num-cell">${fmtPct(r.current_weight)}</td>
            </tr>
          `;
        })
        .join('') ||
      '<tr data-empty-row="true"><td colspan="10" class="table-empty-cell">No Sleeve 3 assets matching filter.</td></tr>';
    attachRowSortData(tbS3, slicedS3, S3_SORT_EXTRACTORS);
  }

  // Render 150+ Perp Scanner
  const tbScan = document.getElementById('tbody-perp-scanner');
  if (tbScan) {
    const sortedScanner = sortItemsForTable(
      scanner,
      'table-perp-scanner',
      SCANNER_SORT_EXTRACTORS
    );
    const slicedScanner = sortedScanner.slice(0, 200);
    tbScan.dataset.truncated =
      sortedScanner.length > slicedScanner.length ? 'true' : 'false';
    tbScan.innerHTML =
      slicedScanner
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
              <td class="num-cell">${fmtPrice(a.mark_price)}</td>
              <td class="num-cell">${fmtPrice(a.mid_price)}</td>
              <td class="num-cell">${fmtPrice(a.oracle_price)}</td>
              <td class="num-cell">${fmtUsd(a.day_volume_usdc, 0)}</td>
              <td class="num-cell">${fmtUsd(a.open_interest_usdc, 0)}</td>
              <td class="num-cell">${(Number(a.funding_rate_hourly || 0) * 100).toFixed(4)}%</td>
              <td class="num-cell">${Number(a.max_leverage ?? 20).toFixed(0)}x</td>
              <td><div class="sleeve-mix-pills">${tags.join('') || '<span class="pnl-flat">SCAN</span>'}</div></td>
            </tr>
          `;
        })
        .join('') ||
      '<tr data-empty-row="true"><td colspan="10" class="table-empty-cell">No Hyperliquid perp assets matching search filter.</td></tr>';
    attachRowSortData(tbScan, slicedScanner, SCANNER_SORT_EXTRACTORS);
  }

  // Render Order Audit & Telemetry Console
  const tbAudit = document.getElementById('tbody-order-audit');
  if (tbAudit) {
    const sortedAudits = sortItemsForTable(
      audits,
      'table-order-audit',
      AUDIT_SORT_EXTRACTORS
    );
    const slicedAudits = sortedAudits.slice(0, 40);
    tbAudit.dataset.truncated =
      sortedAudits.length > slicedAudits.length ? 'true' : 'false';
    tbAudit.innerHTML =
      slicedAudits
        .map((a) => {
          const sideCls =
            String(a.side).toUpperCase() === 'SHORT' ? 'short' : 'long';
          return `
            <tr>
              <td class="num-cell">${fmtLocalTime(a.created_at_ms)}</td>
              <td><code>${escapeHtml(a.client_order_id)}</code></td>
              <td><span class="side-pill ${sideCls}">${escapeHtml(a.side)}</span></td>
              <td><span class="status-pill watch">${escapeHtml(a.from_state)} → ${escapeHtml(a.to_state)}</span></td>
              <td class="num-cell">${fmtPrice(a.limit_price)} / ${fmtPrice(a.avg_fill_price)}</td>
              <td class="num-cell">${fmtUsd(a.reserved_usdc)} / <strong style="color:#34d399">${fmtUsd(a.committed_usdc)}</strong></td>
              <td class="reason-cell" title="${escapeHtml(a.reason)}">${escapeHtml(a.reason)}</td>
            </tr>
          `;
        })
        .join('') ||
      '<tr data-empty-row="true"><td colspan="7" class="table-empty-cell">No order audit state transitions recorded yet.</td></tr>';
    attachRowSortData(tbAudit, slicedAudits, AUDIT_SORT_EXTRACTORS);
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

  const rawHistory = (dash && dash.snapshots_history) || [];
  let history = rawHistory.length > 0 ? rawHistory : dash && dash.snapshot ? [dash.snapshot] : [];
  const fallbackNow = toFiniteNumber(dash?.timestamp_ms) ?? Date.now();

  let points = history
    .map((pt, idx) => {
      const tsRaw = toFiniteNumber(pt?.created_at_ms ?? pt?.updated_at_ms);
      const ts = tsRaw !== null && tsRaw > 0 ? tsRaw : fallbackNow + idx * 1000;
      return { pt, ts, origIdx: idx };
    })
    .toSorted((a, b) => a.ts - b.ts || a.origIdx - b.origIdx);

  if (points.length === 1) {
    points = [
      { pt: { ...points[0].pt, created_at_ms: points[0].ts - 60_000 }, ts: points[0].ts - 60_000, origIdx: 0 },
      points[0],
    ];
  }

  // Update inline live values in Equity Curve legend
  const latestSrc =
    points.length > 0 ? points[points.length - 1].pt : dash?.snapshot || {};
  const liveEqVal =
    toFiniteNumber(dash?.equity_basis?.effective_equity_usdc) ??
    toFiniteNumber(dash?.snapshot?.total_equity_usdc) ??
    toFiniteNumber(latestSrc?.total_equity_usdc) ??
    0;
  const liveCommVal =
    toFiniteNumber(dash?.snapshot?.committed_notional_usdc) ??
    toFiniteNumber(latestSrc?.committed_notional_usdc) ??
    0;
  const liveResVal =
    toFiniteNumber(dash?.snapshot?.reserved_order_cash_usdc) ??
    toFiniteNumber(latestSrc?.reserved_order_cash_usdc) ??
    0;
  const setLegend = (id, v) => {
    const el = document.getElementById(id);
    if (el) el.textContent = fmtUsd(v);
  };
  setLegend('legend-eq-val', liveEqVal);
  setLegend('legend-comm-val', liveCommVal);
  setLegend('legend-res-val', liveResVal);

  const padL = 64;
  const padR = 18;
  const padT = 16;
  const padB = 26;
  const plotW = Math.max(10, w - padL - padR);
  const plotH = Math.max(10, h - padT - padB);

  const vals = [];
  points.forEach(({ pt }) => {
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
  ctx.fillStyle = '#94a3b8';
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

  const tMin = points.length > 0 ? points[0].ts : fallbackNow;
  const tMax = points.length > 0 ? points[points.length - 1].ts : fallbackNow;
  const spanMs = tMax - tMin;

  const xForTime = (tsMs, idx) => {
    if (spanMs > 0) {
      return padL + ((tsMs - tMin) / spanMs) * plotW;
    }
    return padL + (points.length <= 1 ? 0 : (idx / (points.length - 1)) * plotW);
  };
  const yFor = (v) =>
    padT + plotH - ((v - minVal) / Math.max(1e-6, maxVal - minVal)) * plotH;

  const coords = points.map((item, idx) => {
    const pt = item.pt;
    const eq = Number(pt.total_equity_usdc ?? 0);
    const comm = Number(pt.committed_notional_usdc ?? 0);
    const res = Number(pt.reserved_order_cash_usdc ?? 0);
    return {
      x: xForTime(item.ts, idx),
      y: yFor(eq),
      yComm: yFor(comm),
      yRes: yFor(res),
      ts: item.ts,
      pt,
    };
  });

  // Store point coordinates for local-time hover tooltip & crosshair
  state.equityChartPoints = coords;

  // Detect snapshot gaps (dt > 15m && dt > 10 * medianDt) and split into contiguous segments
  const posDts = [];
  for (let i = 1; i < coords.length; i++) {
    const dt = coords[i].ts - coords[i - 1].ts;
    if (dt > 0) posDts.push(dt);
  }
  const sortedDts = posDts.toSorted((a, b) => a - b);
  const medianDt =
    sortedDts.length > 0
      ? sortedDts[Math.floor((sortedDts.length - 1) / 2)]
      : 0;
  const gapThresholdMs = Math.max(15 * 60 * 1000, 10 * medianDt);

  const segments = [];
  const gaps = [];
  if (coords.length > 0) {
    let curSeg = [coords[0]];
    for (let i = 1; i < coords.length; i++) {
      const prev = coords[i - 1];
      const next = coords[i];
      const dt = next.ts - prev.ts;
      if (dt > gapThresholdMs) {
        segments.push(curSeg);
        gaps.push({ prev, next, dtMs: dt });
        curSeg = [next];
      } else {
        curSeg.push(next);
      }
    }
    segments.push(curSeg);
  }

  // Local-timezone X-axis timestamps (time-proportional across [tMin, tMax])
  if (coords.length > 0) {
    const dMin = new Date(tMin);
    const dMax = new Date(tMax);
    const crossesMidnight =
      dMin.getFullYear() !== dMax.getFullYear() ||
      dMin.getMonth() !== dMax.getMonth() ||
      dMin.getDate() !== dMax.getDate();
    const useLongFmt = spanMs >= 18 * 3600 * 1000 || crossesMidnight;
    const formatAxisTs = (tsMs) =>
      useLongFmt ? fmtLocalMonthDayTime(tsMs) : fmtLocalShortTime(tsMs);

    const tickPositions =
      spanMs > 0
        ? [
            { ts: tMin, x: padL, align: 'left', isEnd: false },
            { ts: tMin + spanMs * 0.5, x: padL + plotW * 0.5, align: 'center', isEnd: false },
            { ts: tMax, x: padL + plotW, align: 'right', isEnd: true },
          ]
        : [
            { ts: tMin, x: padL, align: 'left', isEnd: false },
            { ts: tMax, x: padL + plotW, align: 'right', isEnd: true },
          ];

    ctx.font = '10px "JetBrains Mono", monospace';
    ctx.fillStyle = '#94a3b8';
    tickPositions.forEach((tk) => {
      const baseTime = formatAxisTs(tk.ts);
      if (!baseTime) return;
      const tzSuffix = tk.isEnd ? ` ${getLocalTzAbbrev(new Date(tk.ts))}` : '';
      ctx.textAlign = tk.align;
      ctx.fillText(baseTime + tzSuffix, tk.x, padT + plotH + 17);
    });
    ctx.textAlign = 'left';
  }

  // Layer 1: Gap bands & badges
  gaps.forEach((g) => {
    const bandW = Math.max(2, g.next.x - g.prev.x);
    ctx.fillStyle = 'rgba(148, 163, 184, 0.07)';
    ctx.fillRect(g.prev.x, padT, bandW, plotH);

    ctx.save();
    ctx.strokeStyle = 'rgba(148, 163, 184, 0.22)';
    ctx.lineWidth = 1;
    ctx.setLineDash([3, 3]);
    ctx.beginPath();
    ctx.moveTo(g.prev.x, padT);
    ctx.lineTo(g.prev.x, padT + plotH);
    ctx.moveTo(g.next.x, padT);
    ctx.lineTo(g.next.x, padT + plotH);
    ctx.stroke();
    ctx.restore();

    if (bandW >= 46) {
      const label = `gap ${fmtCompactDuration(g.dtMs)}`;
      ctx.save();
      ctx.font = '700 9.5px "JetBrains Mono", monospace';
      const tw = ctx.measureText(label).width;
      const pillW = tw + 12;
      const pillH = 16;
      const pillX = g.prev.x + bandW / 2 - pillW / 2;
      const pillY = padT + 6;
      ctx.fillStyle = 'rgba(10, 15, 29, 0.9)';
      ctx.fillRect(pillX, pillY, pillW, pillH);
      ctx.strokeStyle = 'rgba(148, 163, 184, 0.35)';
      ctx.lineWidth = 1;
      ctx.strokeRect(pillX, pillY, pillW, pillH);
      ctx.fillStyle = '#cbd5e1';
      ctx.textAlign = 'center';
      ctx.fillText(label, g.prev.x + bandW / 2, pillY + 11.5);
      ctx.restore();
    }
  });

  // Layer 2: Translucent gradient area fills FIRST across all contiguous segments
  const drawSegmentFill = (seg, yKey, fillTop) => {
    if (seg.length < 2) return;
    const grad = ctx.createLinearGradient(0, padT, 0, padT + plotH);
    grad.addColorStop(0, fillTop);
    grad.addColorStop(1, 'rgba(6, 9, 17, 0.0)');
    ctx.beginPath();
    seg.forEach((pt, idx) => {
      if (idx === 0) ctx.moveTo(pt.x, pt[yKey]);
      else ctx.lineTo(pt.x, pt[yKey]);
    });
    ctx.lineTo(seg[seg.length - 1].x, padT + plotH);
    ctx.lineTo(seg[0].x, padT + plotH);
    ctx.closePath();
    ctx.fillStyle = grad;
    ctx.fill();
  };

  segments.forEach((seg) => {
    drawSegmentFill(seg, 'y', 'rgba(0, 242, 254, 0.18)');
    drawSegmentFill(seg, 'yComm', 'rgba(16, 185, 129, 0.14)');
  });

  // Layer 3: Dashed gap bridges across gaps
  const drawGapBridge = (yKey, color) => {
    if (gaps.length === 0) return;
    ctx.save();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.setLineDash([4, 4]);
    gaps.forEach((g) => {
      ctx.beginPath();
      ctx.moveTo(g.prev.x, g.prev[yKey]);
      ctx.lineTo(g.next.x, g.next[yKey]);
      ctx.stroke();
    });
    ctx.restore();
  };
  drawGapBridge('yComm', 'rgba(16, 185, 129, 0.45)');
  drawGapBridge('yRes', 'rgba(245, 158, 11, 0.45)');
  drawGapBridge('y', 'rgba(0, 242, 254, 0.6)');

  // Layer 4: Crisp opaque strokes ON TOP (Committed, Reserved, and Total Equity LAST!)
  const drawSeriesStroke = (yKey, strokeColor, lineWidth) => {
    ctx.strokeStyle = strokeColor;
    ctx.fillStyle = strokeColor;
    ctx.lineWidth = lineWidth;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    segments.forEach((seg) => {
      if (seg.length === 1) {
        ctx.beginPath();
        ctx.arc(seg[0].x, seg[0][yKey], 2.8, 0, Math.PI * 2);
        ctx.fill();
        return;
      }
      ctx.beginPath();
      seg.forEach((pt, idx) => {
        if (idx === 0) ctx.moveTo(pt.x, pt[yKey]);
        else ctx.lineTo(pt.x, pt[yKey]);
      });
      ctx.stroke();
    });
  };

  drawSeriesStroke('yComm', '#10b981', 2.0);
  drawSeriesStroke('yRes', '#f59e0b', 2.0);
  drawSeriesStroke('y', '#00f2fe', 2.4);

  // Layer 5: Hover crosshair & highlighted point markers
  if (
    state.equityHoverIdx !== null &&
    state.equityHoverIdx >= 0 &&
    state.equityHoverIdx < coords.length
  ) {
    const hp = coords[state.equityHoverIdx];
    ctx.save();
    ctx.strokeStyle = 'rgba(0, 242, 254, 0.5)';
    ctx.lineWidth = 1;
    ctx.setLineDash([3, 3]);
    ctx.beginPath();
    ctx.moveTo(hp.x, padT);
    ctx.lineTo(hp.x, padT + plotH);
    ctx.stroke();
    ctx.restore();

    const drawHoverDot = (cx, cy, color, r = 4) => {
      ctx.beginPath();
      ctx.arc(cx, cy, r, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
      ctx.lineWidth = 1.5;
      ctx.strokeStyle = '#060911';
      ctx.stroke();
    };
    drawHoverDot(hp.x, hp.yComm, '#10b981', 3.5);
    drawHoverDot(hp.x, hp.yRes, '#f59e0b', 3.5);
    drawHoverDot(hp.x, hp.y, '#00f2fe', 4.5);
  }

  // Accessible chart summary for screen readers
  const eqCanvasEl = document.getElementById('equity-curve-canvas');
  const eqCaptionEl = document.getElementById('equity-curve-caption');
  const ariaSummary = `Portfolio equity curve from ${fmtLocalTime(tMin)} to ${fmtLocalTime(tMax)}: current equity ${fmtUsd(liveEqVal)}, committed notional ${fmtUsd(liveCommVal)}, reserved margin ${fmtUsd(liveResVal)}${gaps.length > 0 ? `, with ${gaps.length} snapshot gap(s)` : ''}.`;
  if (eqCanvasEl) eqCanvasEl.setAttribute('aria-label', ariaSummary);
  if (eqCaptionEl) eqCaptionEl.textContent = ariaSummary;
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
  const padR = 56;
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

  const slCanvasEl = document.getElementById('sleeve-attr-canvas');
  const slCaptionEl = document.getElementById('sleeve-attr-caption');
  const totalPct = ((s1 + s2 + s3) * 100).toFixed(1);
  const slSummary = `Gross target sleeve exposure total ${totalPct}% of equity: S1 Carry-Momentum ${(s1 * 100).toFixed(1)}%, S2 Trend-Breakout ${(s2 * 100).toFixed(1)}%, S3 Event-Reversion ${(s3 * 100).toFixed(1)}%.`;
  if (slCanvasEl) slCanvasEl.setAttribute('aria-label', slSummary);
  if (slCaptionEl) slCaptionEl.textContent = slSummary;
}

/* ============================================================================
   NATIVE <dialog> HYPERLIQUID VAULT MAINTENANCE & LIVE FACTS MODAL
   ============================================================================ */

const VaultFormValidation = (() => {
  let announceTimer = null;
  let announcedField = null;
  const flagged = new WeakSet();
  const announced = new WeakSet();
  const fallbackDirtyState = new WeakMap();

  const getAnnouncer = () => document.getElementById('vault-form-announcer');

  // List the error message (from data-error) in aria-describedby ONLY while invalid
  const linkError = (el, shown) => {
    const errorId = el?.dataset?.error;
    if (!errorId) return;
    const ids = (el.getAttribute('aria-describedby') || '')
      .split(/\s+/)
      .filter((id) => id && id !== errorId);
    if (shown) ids.push(errorId);
    if (ids.length) el.setAttribute('aria-describedby', ids.join(' '));
    else el.removeAttribute('aria-describedby');
  };

  // Text as a screen reader reads it: icons' text alternatives in, aria-hidden text out
  const spokenText = (node) => {
    const copy = node.cloneNode(true);
    copy.querySelectorAll('[aria-hidden="true"]').forEach((n) => n.remove());
    copy.querySelectorAll('[role="img"][aria-label]').forEach((n) => {
      n.replaceWith(`${n.getAttribute('aria-label')}: `);
    });
    return copy.textContent.replace(/\s+/g, ' ').trim();
  };

  const clearAnnouncer = (el = null) => {
    if (el !== null && announcedField !== el) return;
    clearTimeout(announceTimer);
    const announcer = getAnnouncer();
    if (announcer) announcer.textContent = '';
    if (el === null || announcedField === el) {
      announcedField = null;
    }
  };

  const announceError = (el) => {
    const announcer = getAnnouncer();
    const errorId = el?.dataset?.error;
    if (!announcer || !errorId) return;
    const errorEl = document.getElementById(errorId);
    if (!errorEl) return;
    const label = el.labels?.[0] ? `${spokenText(el.labels[0])}: ` : '';
    clearTimeout(announceTimer);
    announcer.textContent = '';
    announcedField = el;
    // Wait 500ms until the screen reader has announced the next focused field
    announceTimer = setTimeout(() => {
      announcer.textContent = label + spokenText(errorEl);
    }, 500);
  };

  const trimAddressIfNeeded = (el) => {
    if (el && el.id === 'vault-address-input' && typeof el.value === 'string') {
      const trimmed = el.value.trim();
      if (trimmed !== el.value) {
        el.value = trimmed;
      }
    }
  };

  // Fallback for browsers without :user-invalid support (WeakMap dirty tracking)
  const UserInvalidFallback = (() => {
    const updateState = (input, shouldAnnounce = false) => {
      const isValid = input.validity.valid;
      input.classList.toggle('user-invalid-fallback', !isValid);
      input.classList.toggle('user-valid-fallback', isValid);
      input.setAttribute('aria-invalid', isValid ? 'false' : 'true');
      linkError(input, !isValid);
      if (!isValid) {
        if (shouldAnnounce && !announced.has(input)) {
          announceError(input);
          announced.add(input);
        }
      } else {
        announced.delete(input);
        clearAnnouncer(input);
      }
    };

    const handleEvent = (event) => {
      const input = event.target;
      if (event.type === 'reset') {
        const controls = input.elements || [];
        for (const control of controls) {
          fallbackDirtyState.delete(control);
          announced.delete(control);
          control.classList.remove('user-invalid-fallback', 'user-valid-fallback');
          if (control.matches?.('input, select, textarea')) {
            control.setAttribute('aria-invalid', 'false');
          }
          if (control.dataset?.error) linkError(control, false);
        }
        clearAnnouncer(null);
        return;
      }

      if (!input || !input.checkValidity) return;

      if (event.type === 'invalid') {
        fallbackDirtyState.set(input, { hasInteracted: true, hasBlurred: true });
        announced.add(input);
        updateState(input, false);
        return;
      }

      if (event.type === 'input' || event.type === 'change') {
        const st = fallbackDirtyState.get(input) || {
          hasInteracted: false,
          hasBlurred: false,
        };
        st.hasInteracted = true;
        fallbackDirtyState.set(input, st);
        if (st.hasBlurred || event.type === 'change') {
          updateState(input, event.type === 'change');
        }
      } else if (event.type === 'blur') {
        trimAddressIfNeeded(input);
        const st = fallbackDirtyState.get(input) || {
          hasInteracted: false,
          hasBlurred: false,
        };
        st.hasBlurred = true;
        fallbackDirtyState.set(input, st);
        if (st.hasInteracted) {
          updateState(input, true);
        }
      }
    };

    const init = (root) => {
      if (CSS.supports('selector(:user-invalid)')) return false;
      root.querySelectorAll('input, select, textarea').forEach((el) => {
        if (!el.hasAttribute('aria-invalid')) {
          el.setAttribute('aria-invalid', 'false');
        }
      });
      root.addEventListener('blur', handleEvent, true);
      root.addEventListener('input', handleEvent);
      root.addEventListener('change', handleEvent, true);
      root.addEventListener('invalid', handleEvent, true);
      root.addEventListener('reset', handleEvent, true);
      return true;
    };

    return { init, updateState };
  })();

  const syncAria = (el, announce = false) => {
    if (!el || !el.matches?.('input, select, textarea')) return;
    if (!CSS.supports('selector(:user-invalid)')) {
      if (el.validity?.valid) {
        el.classList.remove('user-invalid-fallback');
        if (!String(el.value || '').trim()) {
          el.classList.remove('user-valid-fallback');
        }
        fallbackDirtyState.delete(el);
      }
      const invalid =
        Boolean(el.validity ? !el.validity.valid : true) &&
        el.classList.contains('user-invalid-fallback');
      el.setAttribute('aria-invalid', invalid ? 'true' : 'false');
      linkError(el, invalid);
      if (!invalid) {
        announced.delete(el);
        clearAnnouncer(el);
      }
      return;
    }
    const invalid = el.matches(':user-invalid');
    if (invalid) {
      if (announce && !announced.has(el)) {
        announceError(el);
        announced.add(el);
      }
      el.setAttribute('aria-invalid', 'true');
      linkError(el, true);
      flagged.add(el);
    } else if (flagged.has(el) || el.getAttribute('aria-invalid') !== 'true') {
      el.setAttribute('aria-invalid', 'false');
      linkError(el, false);
      flagged.delete(el);
      announced.delete(el);
      clearAnnouncer(el);
    }
  };

  const resetFormState = (form) => {
    if (!form) return;
    for (const el of form.elements || []) {
      if (typeof el.setCustomValidity === 'function') {
        el.setCustomValidity('');
      }
      fallbackDirtyState.delete(el);
      flagged.delete(el);
      announced.delete(el);
      el.classList?.remove('user-invalid-fallback', 'user-valid-fallback');
      if (el.matches?.('input, select, textarea')) {
        el.setAttribute('aria-invalid', 'false');
        linkError(el, false);
      }
    }
    clearAnnouncer(null);
  };

  const init = (form) => {
    if (!form || form.dataset.validationWired === 'true') return;
    form.dataset.validationWired = 'true';

    // Helpful native constraint message for EVM address pattern mismatch
    const addrInput = document.getElementById('vault-address-input');
    if (addrInput) {
      addrInput.addEventListener('invalid', () => {
        if (addrInput.validity.patternMismatch) {
          addrInput.setCustomValidity(
            'Enter a valid 42-character EVM address starting with 0x (40 hex digits), or leave blank.'
          );
        }
      });
      addrInput.addEventListener('input', () => {
        addrInput.setCustomValidity('');
      });
    }

    const usedFallback = UserInvalidFallback.init(form);
    if (!usedFallback) {
      form.querySelectorAll('input, select, textarea').forEach((el) => syncAria(el));

      form.addEventListener(
        'blur',
        (e) => {
          trimAddressIfNeeded(e.target);
          syncAria(e.target, true);
        },
        true
      );
      form.addEventListener('input', (e) => {
        if (e.target?.hasAttribute?.('aria-invalid')) syncAria(e.target, false);
      });
      form.addEventListener('change', (e) => syncAria(e.target, true), true);
      form.addEventListener(
        'invalid',
        (e) => {
          announced.add(e.target);
          syncAria(e.target, false);
        },
        true
      );
      form.addEventListener(
        'reset',
        (e) => {
          setTimeout(() => {
            [...(e.target.elements || [])].forEach((el) => syncAria(el, false));
          });
        },
        true
      );
    }
  };

  return { init, syncAria, resetFormState };
})();

function initVaultModalDialogAndForm() {
  const dialog = document.getElementById('vault-manager-modal');
  const form = document.getElementById('vault-manager-form');
  if (!dialog) return;

  if (dialog.dataset.dialogWired !== 'true') {
    dialog.dataset.dialogWired = 'true';

    // Read and reset dialog.returnValue on every close so stale values never leak
    dialog.addEventListener('close', () => {
      const closedWith = dialog.returnValue;
      dialog.returnValue = '';
      state.vaultModal.isPreviewMode = false;
      state.vaultModal.previewVault = null;
      state.vaultModal.previewMeta = null;
      if (closedWith) {
        // Explicit close value consumed and cleared
      }
    });

    // Progressive light-dismiss fallback when <dialog closedby="any"> is unsupported
    if (!('closedBy' in HTMLDialogElement.prototype)) {
      const isOutsideDialog = (e, dlg) => {
        const r = dlg.getBoundingClientRect();
        return (
          e.clientX < r.left ||
          e.clientX > r.right ||
          e.clientY < r.top ||
          e.clientY > r.bottom
        );
      };
      let pressedOutside = false;
      dialog.addEventListener('pointerdown', (e) => {
        pressedOutside = dialog.open && isOutsideDialog(e, dialog);
      });
      dialog.addEventListener('pointerup', (e) => {
        if (pressedOutside && dialog.open && isOutsideDialog(e, dialog)) {
          dialog.close();
        }
        pressedOutside = false;
      });
      dialog.addEventListener('pointercancel', () => {
        pressedOutside = false;
      });
    }
  }

  if (form) {
    VaultFormValidation.init(form);
    if (form.dataset.submitWired !== 'true') {
      form.dataset.submitWired = 'true';
      form.addEventListener('submit', submitVaultManagerForm);
    }
  }

  const notesInput = document.getElementById('vault-notes-input');
  if (notesInput && notesInput.dataset.countWired !== 'true') {
    notesInput.dataset.countWired = 'true';
    notesInput.addEventListener('input', updateVaultNotesCharCount);
  }

  const allocInput = document.getElementById('vault-target-alloc-input');
  const sweepInput = document.getElementById('vault-sweep-threshold-input');
  const previewBarbell = () => {
    if (state.dashboard && dialog.open) {
      renderVaultBarbellCard(state.dashboard);
    }
  };
  if (allocInput && allocInput.dataset.previewWired !== 'true') {
    allocInput.dataset.previewWired = 'true';
    allocInput.addEventListener('input', previewBarbell);
  }
  if (sweepInput && sweepInput.dataset.previewWired !== 'true') {
    sweepInput.dataset.previewWired = 'true';
    sweepInput.addEventListener('input', previewBarbell);
  }
  if (form && form.dataset.rolePreviewWired !== 'true') {
    form.dataset.rolePreviewWired = 'true';
    form.addEventListener('change', (evt) => {
      if (evt.target?.name === 'vault_role') {
        previewBarbell();
      }
    });
  }
}

function updateVaultNotesCharCount() {
  const notesInput = document.getElementById('vault-notes-input');
  const countEl = document.getElementById('vault-notes-char-count');
  if (notesInput && countEl) {
    countEl.textContent = String((notesInput.value || '').length);
  }
}

function setVaultFormStatus(message, tone = 'info') {
  const el = document.getElementById('vault-form-status');
  if (!el) return;
  if (!message) {
    el.textContent = '';
    el.className = 'vault-form-status';
    el.hidden = true;
    return;
  }
  el.className = `vault-form-status status-${tone}`;
  el.textContent = message;
  el.hidden = false;
}

function getOfficialHlpForCurrentMode(dash = state.dashboard) {
  const mode = String(
    dash?.execution_mode || state.health?.execution_mode || 'TESTNET'
  ).toUpperCase();
  const isLive = mode === 'LIVE';
  return {
    isLive,
    mode,
    address: isLive ? OFFICIAL_HLP_MAINNET : OFFICIAL_HLP_TESTNET,
    networkLabel: isLive ? 'Mainnet (LIVE)' : 'Testnet',
  };
}

function renderVaultModalHeaderMeta(dash) {
  const pill = document.getElementById('vault-modal-network-pill');
  const hlpBtn = document.getElementById('btn-vault-use-hlp');
  const info = getOfficialHlpForCurrentMode(dash);
  if (pill) {
    pill.textContent = info.mode;
    pill.className = info.isLive ? 'status-pill open' : 'status-pill working';
  }
  if (hlpBtn) {
    hlpBtn.title = `Fill Official Hyperliquid HLP address for ${info.networkLabel}: ${info.address}`;
  }
}

function hydrateVaultModalFormFromDashboard(dash) {
  const form = document.getElementById('vault-manager-form');
  if (!form) return;
  form.reset();
  VaultFormValidation.resetFormState(form);

  const cfgAddr = getConfigValueFromDash(
    dash,
    'hyperliquid_vault_address',
    dash?.vault_info?.configured_vault_address || ''
  ).trim();
  const cfgRoleRaw = getConfigValueFromDash(dash, 'vault_role', 'follower')
    .trim()
    .toLowerCase();
  const cfgRole = ['follower', 'operator', 'none'].includes(cfgRoleRaw)
    ? cfgRoleRaw
    : 'follower';
  const cfgTargetAlloc = getConfigNonNegFloatFromDash(
    dash,
    'vault_target_allocation_pct',
    0
  );
  const cfgSweepThresh = getConfigNonNegFloatFromDash(
    dash,
    'vault_profit_sweep_threshold_usdc',
    0
  );
  const cfgNotes = getConfigValueFromDash(dash, 'vault_notes', '');

  const addrInput = document.getElementById('vault-address-input');
  if (addrInput) addrInput.value = cfgAddr;

  const roleRadio = form.querySelector(
    `input[name="vault_role"][value="${cfgRole}"]`
  );
  if (roleRadio) roleRadio.checked = true;

  const allocInput = document.getElementById('vault-target-alloc-input');
  if (allocInput) allocInput.value = String(cfgTargetAlloc);

  const sweepInput = document.getElementById('vault-sweep-threshold-input');
  if (sweepInput) sweepInput.value = String(cfgSweepThresh);

  const notesInput = document.getElementById('vault-notes-input');
  if (notesInput) notesInput.value = cfgNotes;
  updateVaultNotesCharCount();
}

function openVaultModal(focusVaultAddress = null) {
  const dialog = document.getElementById('vault-manager-modal');
  if (!dialog) return;
  initVaultModalDialogAndForm();

  const dash = state.dashboard || {};
  hydrateVaultModalFormFromDashboard(dash);
  setVaultFormStatus('');

  state.vaultModal.isPreviewMode = false;
  state.vaultModal.previewVault = null;
  state.vaultModal.previewMeta = null;

  const vaults = Array.isArray(dash?.vault_info?.vaults) ? dash.vault_info.vaults : [];
  const focusNorm = String(focusVaultAddress || '').trim().toLowerCase();
  const cfgAddrNorm = String(
    getConfigValueFromDash(
      dash,
      'hyperliquid_vault_address',
      dash?.vault_info?.configured_vault_address || ''
    )
  )
    .trim()
    .toLowerCase();

  if (focusNorm) {
    state.vaultModal.selectedAddress = focusNorm;
  } else if (
    cfgAddrNorm &&
    vaults.some((v) => String(v?.vault_address || '').trim().toLowerCase() === cfgAddrNorm)
  ) {
    state.vaultModal.selectedAddress = cfgAddrNorm;
  } else if (vaults.length > 0) {
    state.vaultModal.selectedAddress = String(vaults[0]?.vault_address || '')
      .trim()
      .toLowerCase();
  } else {
    state.vaultModal.selectedAddress = '';
  }

  renderVaultModalHeaderMeta(dash);
  renderVaultModalSyncedTabs(dash);
  renderVaultFactsDisplay(dash);
  renderVaultBarbellCard(dash);
  renderVaultReadinessCard(dash);

  // Guard against double-open when browser natively invokes command="show-modal"
  if (!dialog.open) {
    dialog.showModal();
  }

  if (
    focusNorm &&
    !vaults.some((v) => String(v?.vault_address || '').trim().toLowerCase() === focusNorm) &&
    /^0[xX][0-9a-fA-F]{40}$/.test(focusNorm)
  ) {
    inspectVaultAddress(focusNorm);
  }
}

function refreshOpenVaultModalFromDashboard(dash) {
  const dialog = document.getElementById('vault-manager-modal');
  if (!dialog || !dialog.open || !dash) return;
  renderVaultModalHeaderMeta(dash);
  renderVaultModalSyncedTabs(dash);
  renderVaultBarbellCard(dash);
  renderVaultReadinessCard(dash);
  if (!state.vaultModal.isPreviewMode) {
    renderVaultFactsDisplay(dash);
  }
}

function renderVaultModalSyncedTabs(dash) {
  const container = document.getElementById('vault-modal-synced-tabs');
  if (!container) return;

  const vaults = Array.isArray(dash?.vault_info?.vaults) ? dash.vault_info.vaults : [];
  const selectedNorm = String(state.vaultModal.selectedAddress || '').toLowerCase();
  const nodes = [];

  if (state.vaultModal.previewVault) {
    const pv = state.vaultModal.previewVault;
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = `vault-chip-btn preview${state.vaultModal.isPreviewMode ? ' active' : ''}`;
    btn.dataset.action = 'select-preview-vault';
    btn.setAttribute('aria-pressed', state.vaultModal.isPreviewMode ? 'true' : 'false');
    const previewName = String(pv.name || pv.vault_address || 'Inspected Vault');
    btn.textContent = `LIVE PREVIEW · ${previewName}`;
    nodes.push(btn);
  }

  for (const v of vaults) {
    const addr = String(v?.vault_address || '').trim();
    const addrNorm = addr.toLowerCase();
    const isActive = !state.vaultModal.isPreviewMode && addrNorm && addrNorm === selectedNorm;
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = `vault-chip-btn${isActive ? ' active' : ''}`;
    btn.dataset.action = 'select-synced-vault';
    btn.dataset.vaultAddress = addr;
    btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');

    const label = document.createElement('span');
    label.textContent = String(v?.name || 'Hyperliquid Vault');
    btn.appendChild(label);

    const eq = toFiniteNumber(v?.equity_usdc);
    if (eq !== null && eq > 0.005) {
      const eqSpan = document.createElement('span');
      eqSpan.className = 'tab-count';
      eqSpan.textContent = fmtUsd(eq, 0);
      btn.appendChild(eqSpan);
    }
    nodes.push(btn);
  }

  if (nodes.length === 0) {
    const emptyNote = document.createElement('span');
    emptyNote.className = 'vault-empty-tabs-note';
    emptyNote.textContent =
      'No synced wallet vaults yet — click "Use Official HLP" or inspect a 0x vault address.';
    container.replaceChildren(emptyNote);
    return;
  }

  container.replaceChildren(...nodes);
}

function createVaultFactTile(labelText, valueText, subText = '', extraValueClass = '', valueTitle = '') {
  const tile = document.createElement('div');
  tile.className = 'vault-fact-tile';

  const lbl = document.createElement('span');
  lbl.className = 'vault-fact-label';
  lbl.textContent = labelText;

  const val = document.createElement('div');
  val.className = extraValueClass ? `vault-fact-value ${extraValueClass}` : 'vault-fact-value';
  val.textContent = valueText;
  if (valueTitle) val.title = valueTitle;

  tile.append(lbl, val);

  if (subText) {
    const sub = document.createElement('span');
    sub.className = 'vault-fact-sub';
    sub.textContent = subText;
    tile.appendChild(sub);
  }

  return { tile, val };
}

function createStatusPillEl(cls, text) {
  const span = document.createElement('span');
  span.className = `status-pill ${cls}`;
  span.textContent = text;
  return span;
}

function renderVaultFactsDisplay(dash) {
  const sourcePill = document.getElementById('vault-fact-source-pill');
  const nameEl = document.getElementById('vault-fact-name');
  const pillsEl = document.getElementById('vault-fact-status-pills');
  const addrsEl = document.getElementById('vault-fact-addresses');
  const descEl = document.getElementById('vault-fact-description');
  const gridEl = document.getElementById('vault-facts-grid');
  if (!gridEl) return;

  const vaults = Array.isArray(dash?.vault_info?.vaults) ? dash.vault_info.vaults : [];
  let activeVault = null;
  let sourceText = 'AWAITING SELECTION';
  let sourceCls = 'status-pill watch';

  if (state.vaultModal.isPreviewMode && state.vaultModal.previewVault) {
    activeVault = state.vaultModal.previewVault;
    const isTestnet = Boolean(state.vaultModal.previewMeta?.is_testnet);
    const inspectedAt = toFiniteNumber(state.vaultModal.previewMeta?.inspected_at_ms);
    const tsStr = inspectedAt ? ` · ${fmtLocalShortTime(inspectedAt)}` : '';
    sourceText = `LIVE PREVIEW · ${isTestnet ? 'TESTNET' : 'MAINNET'}${tsStr}`;
    sourceCls = 'status-pill relationship';
  } else if (vaults.length > 0) {
    const selNorm = String(state.vaultModal.selectedAddress || '').toLowerCase();
    activeVault =
      vaults.find(
        (v) => String(v?.vault_address || '').trim().toLowerCase() === selNorm
      ) || vaults[0];
    if (activeVault?.vault_address) {
      state.vaultModal.selectedAddress = String(activeVault.vault_address)
        .trim()
        .toLowerCase();
    }
    const mode = String(dash?.execution_mode || 'TESTNET').toUpperCase();
    const syncedAt = toFiniteNumber(dash?.vault_info?.synced_at_ms);
    const tsStr = syncedAt ? ` · ${fmtLocalShortTime(syncedAt)}` : '';
    sourceText = `LIVE SYNCED · ${mode === 'LIVE' ? 'MAINNET' : mode}${tsStr}`;
    sourceCls = 'status-pill open';
  }

  if (sourcePill) {
    sourcePill.className = sourceCls;
    sourcePill.textContent = sourceText;
  }

  if (!activeVault) {
    if (nameEl) nameEl.textContent = 'No Vault Selected';
    if (pillsEl) pillsEl.replaceChildren();
    if (addrsEl) addrsEl.replaceChildren();
    if (descEl) {
      descEl.textContent =
        'Select a synced vault above, click "Use Official HLP", or enter a 0x vault address and click "Inspect Live Facts".';
      descEl.classList.add('is-empty');
    }
    const placeholderTiles = [
      createVaultFactTile('Vault TVL', '—', 'Total Value Locked').tile,
      createVaultFactTile('Vault APR', '—', 'Annualized return').tile,
      createVaultFactTile('Followers', '—', 'Active depositors').tile,
      createVaultFactTile('Leader Stake %', '—', 'HL User Vault rule: ≥5%').tile,
      createVaultFactTile('Leader Profit Share', '—', 'Commission on profits').tile,
      createVaultFactTile('Your Equity', '—', 'Segregated from perp margin').tile,
      createVaultFactTile('Your PnL ($ / %)', '—', 'Current vault position').tile,
      createVaultFactTile('All-Time PnL ($ / %)', '—', 'Cumulative wallet PnL').tile,
      createVaultFactTile('Days Following', '—', 'Deposit tenure').tile,
      createVaultFactTile('Lockup Status', '—', '4-day deposit lock rule').tile,
      createVaultFactTile('Close-on-Withdraw', '—', 'Withdrawal policy').tile,
      createVaultFactTile('Max Withdrawable / Vol', '—', 'All-Time Vol: —').tile,
    ];
    gridEl.replaceChildren(...placeholderTiles);
    return;
  }

  const v = activeVault;
  const isHlp = isOfficialHlpAddress(v.vault_address);

  if (nameEl) {
    nameEl.textContent = String(v.name || 'Hyperliquid Vault');
  }

  if (pillsEl) {
    const pills = [];
    pills.push(
      v.is_closed
        ? createStatusPillEl('closed', 'CLOSED')
        : createStatusPillEl('open', 'OPEN')
    );
    const allowDeposits = v.allow_deposits !== undefined ? Boolean(v.allow_deposits) : !v.is_closed;
    pills.push(
      allowDeposits
        ? createStatusPillEl('open', 'DEPOSITS OPEN')
        : createStatusPillEl('working', 'DEPOSITS PAUSED')
    );

    const relRaw = String(v.relationship_type || 'normal').trim().toLowerCase();
    const childCount = Math.max(0, Math.floor(Number(v.child_vault_count || 0)));
    let relLabel = 'STANDARD VAULT';
    if (relRaw === 'parent') {
      relLabel =
        childCount > 0
          ? `PARENT (${childCount} ${childCount === 1 ? 'SUB-VAULT' : 'SUB-VAULTS'})`
          : 'PARENT VAULT';
    } else if (relRaw === 'child') {
      relLabel = 'CHILD VAULT';
    } else if (relRaw && relRaw !== 'normal') {
      relLabel = relRaw.toUpperCase();
    }
    pills.push(createStatusPillEl('relationship', relLabel));

    if (isHlp) {
      pills.push(createStatusPillEl('watch', 'OFFICIAL HLP'));
    }
    pillsEl.replaceChildren(...pills);
  }

  if (addrsEl) {
    const pairVault = document.createElement('span');
    pairVault.className = 'vault-addr-pair';
    const lblVault = document.createElement('span');
    lblVault.className = 'vault-addr-label';
    lblVault.textContent = 'Vault:';
    pairVault.append(lblVault, createAddressChip(v.vault_address));

    const pairLeader = document.createElement('span');
    pairLeader.className = 'vault-addr-pair';
    const lblLeader = document.createElement('span');
    lblLeader.className = 'vault-addr-label';
    lblLeader.textContent = 'Leader:';
    pairLeader.append(lblLeader, createAddressChip(v.leader));

    addrsEl.replaceChildren(pairVault, pairLeader);
  }

  if (descEl) {
    const desc = String(v.description || '').trim();
    if (desc) {
      descEl.textContent = desc;
      descEl.classList.remove('is-empty');
    } else {
      descEl.textContent = 'No description published for this vault on Hyperliquid L1.';
      descEl.classList.add('is-empty');
    }
  }

  // Build 12-tile Live Vault Facts Grid (strictly DOM API, zero innerHTML)
  const tvlVal = toFiniteNumber(v.tvl_usdc);
  const tileTvl = createVaultFactTile(
    'Vault TVL',
    tvlVal !== null ? fmtCompactUsd(tvlVal) : '—',
    'Total Value Locked',
    '',
    tvlVal !== null ? `Exact TVL: ${fmtUsd(tvlVal, 2)}` : ''
  );

  const aprVal = toFiniteNumber(v.apr_pct);
  const tileApr = createVaultFactTile(
    'Vault APR',
    fmtSignedPct(aprVal, 2) ?? '—',
    'Annualized vault return',
    pnlToneClass(aprVal, 0.005)
  );

  const followerCount = Math.max(0, Math.floor(Number(v.follower_count ?? 0)));
  const followersCapped = Boolean(v.followers_capped);
  const tileFollowers = createVaultFactTile(
    'Followers',
    followersCapped ? `${Math.max(100, followerCount)}+` : followerCount.toLocaleString('en-US'),
    followersCapped ? 'HL API follower list capped at 100+' : 'Active vault depositors'
  );

  const leaderStakePct = toFiniteNumber(v.leader_fraction_pct) ?? 0;
  let leaderStakeSub = 'Meets ≥5% HL User Vault rule';
  let leaderStakeTone = 'pnl-pos';
  if (isHlp) {
    leaderStakeSub = 'Protocol vault (HLP exempt from ≥5% rule)';
    leaderStakeTone = '';
  } else if (leaderStakePct < 5.0) {
    leaderStakeSub = 'Below ≥5% HL User Vault leader floor';
    leaderStakeTone = 'cell-sl-dist';
  }
  const tileLeaderStake = createVaultFactTile(
    'Leader Stake %',
    `${leaderStakePct.toFixed(2)}%`,
    leaderStakeSub,
    leaderStakeTone
  );

  const leaderCommPct = toFiniteNumber(v.leader_commission_pct) ?? 0;
  const tileLeaderComm = createVaultFactTile(
    'Leader Profit Share',
    `${leaderCommPct.toFixed(2)}%`,
    isHlp ? '0% protocol vault fee' : 'Leader commission on profits'
  );

  const yourEq = toFiniteNumber(v.equity_usdc) ?? 0;
  const tileEquity = createVaultFactTile(
    'Your Equity',
    fmtUsd(yourEq, 2),
    'Segregated from perp margin'
  );

  const tilePnl = createVaultFactTile('Your PnL ($ / %)', '—', 'Current vault position');
  populatePnlElement(
    tilePnl.val,
    v.pnl_usdc,
    computeVaultPnlPct(v, 'pnl_pct', 'pnl_usdc'),
    'vault-fact-value'
  );

  const tileAtPnl = createVaultFactTile(
    'All-Time PnL ($ / %)',
    '—',
    'Cumulative wallet vault PnL'
  );
  populatePnlElement(
    tileAtPnl.val,
    v.all_time_pnl_usdc,
    computeVaultPnlPct(v, 'all_time_pnl_pct', 'all_time_pnl_usdc'),
    'vault-fact-value'
  );

  const daysVal = toFiniteNumber(v.days_following);
  const tileDays = createVaultFactTile(
    'Days Following',
    daysVal !== null ? `${Math.max(0, Math.floor(daysVal))}d` : '—',
    'Active deposit tenure'
  );

  const isLocked = Boolean(v.is_locked);
  const lockedUntilMs = toFiniteNumber(v.locked_until_ms) ?? 0;
  const tileLockup = createVaultFactTile(
    'Lockup Status',
    isLocked ? 'LOCKED' : 'UNLOCKED',
    isLocked
      ? lockedUntilMs > 0
        ? `Until ${fmtLocalTime(lockedUntilMs)}`
        : 'Deposit lock active'
      : lockedUntilMs > 0
      ? `Unlocked since ${fmtLocalMonthDayTime(lockedUntilMs)}`
      : 'No active deposit lock',
    isLocked ? 'cell-sl-dist' : 'pnl-pos'
  );

  const alwaysClose = Boolean(v.always_close_on_withdraw);
  const tileClosePolicy = createVaultFactTile(
    'Close-on-Withdraw',
    alwaysClose ? 'ALWAYS CLOSE' : 'PROPORTIONAL',
    alwaysClose
      ? 'Closes vault positions upon withdrawal'
      : 'Proportional share withdrawal policy'
  );

  const maxWithdrawable = toFiniteNumber(v.max_withdrawable_usdc) ?? 0;
  const maxDistributable = toFiniteNumber(v.max_distributable_usdc) ?? 0;
  const allTimeVol = toFiniteNumber(v.all_time_volume_usdc) ?? 0;
  const tileWithdrawable = createVaultFactTile(
    'Max Withdrawable / Vol',
    fmtCompactUsd(maxWithdrawable),
    `All-Time Vol: ${fmtCompactUsd(allTimeVol)}`,
    '',
    `Max Withdrawable: ${fmtUsd(maxWithdrawable, 2)} · Max Distributable: ${fmtUsd(
      maxDistributable,
      2
    )} · All-Time Volume: ${fmtUsd(allTimeVol, 2)}`
  );

  gridEl.replaceChildren(
    tileTvl.tile,
    tileApr.tile,
    tileFollowers.tile,
    tileLeaderStake.tile,
    tileLeaderComm.tile,
    tileEquity.tile,
    tilePnl.tile,
    tileAtPnl.tile,
    tileDays.tile,
    tileLockup.tile,
    tileClosePolicy.tile,
    tileWithdrawable.tile
  );
}

function readCurrentVaultModalDraftTargets(dash) {
  const form = document.getElementById('vault-manager-form');
  const allocInput = document.getElementById('vault-target-alloc-input');
  const sweepInput = document.getElementById('vault-sweep-threshold-input');

  const checkedRole = form?.querySelector('input[name="vault_role"]:checked')?.value;
  const role =
    checkedRole ||
    getConfigValueFromDash(dash, 'vault_role', 'follower').trim().toLowerCase() ||
    'follower';

  const allocDraft = toFiniteNumber(allocInput?.value);
  const targetPctRaw =
    allocDraft !== null
      ? allocDraft
      : getConfigNonNegFloatFromDash(dash, 'vault_target_allocation_pct', 0);
  const targetPct = Math.max(0, Math.min(100, targetPctRaw));

  const sweepDraft = toFiniteNumber(sweepInput?.value);
  const sweepThreshold = Math.max(
    0,
    sweepDraft !== null
      ? sweepDraft
      : getConfigNonNegFloatFromDash(dash, 'vault_profit_sweep_threshold_usdc', 0)
  );

  return { role, targetPct, sweepThreshold };
}

function createLegendSwatchItem(colorHex, labelText, valText) {
  const item = document.createElement('span');
  item.className = 'legend-item';
  const swatch = document.createElement('span');
  swatch.className = 'legend-swatch';
  swatch.style.background = colorHex;
  const lbl = document.createElement('span');
  lbl.textContent = labelText;
  const strong = document.createElement('strong');
  strong.className = 'legend-val';
  strong.textContent = valText;
  item.append(swatch, lbl, strong);
  return item;
}

function renderVaultBarbellCard(dash) {
  const rolePill = document.getElementById('vault-barbell-role-pill');
  const barEl = document.getElementById('vault-barbell-bar');
  const segPerp = document.getElementById('vault-bar-seg-perp');
  const segSpot = document.getElementById('vault-bar-seg-spot');
  const segVault = document.getElementById('vault-bar-seg-vault');
  const legendEl = document.getElementById('vault-barbell-legend');
  const metricsEl = document.getElementById('vault-barbell-metrics');
  if (!barEl || !legendEl || !metricsEl) return;

  const eb = dash?.equity_basis || {};
  const vi = dash?.vault_info || {};
  const isUnified = Boolean(eb.is_unified_account);
  const perpEq = Math.max(
    0,
    toFiniteNumber(eb.perp_wallet_equity_usdc) ??
      toFiniteNumber(dash?.snapshot?.total_equity_usdc) ??
      0
  );
  const spotEq = isUnified ? 0 : Math.max(0, toFiniteNumber(eb.spot_usdc) ?? 0);
  const vaultEq = Math.max(0, toFiniteNumber(vi.total_vault_equity_usdc) ?? 0);
  const combined = perpEq + spotEq + vaultEq;

  const perpPct = combined > 0 ? (perpEq / combined) * 100 : 0;
  const spotPct = combined > 0 ? (spotEq / combined) * 100 : 0;
  const vaultPct = combined > 0 ? (vaultEq / combined) * 100 : 0;

  const { role, targetPct, sweepThreshold } = readCurrentVaultModalDraftTargets(dash);

  if (rolePill) {
    rolePill.textContent = role.toUpperCase();
    rolePill.className =
      role === 'operator'
        ? 'status-pill relationship'
        : role === 'follower'
        ? 'status-pill open'
        : 'status-pill watch';
  }

  if (segPerp) segPerp.style.width = `${perpPct.toFixed(2)}%`;
  if (segSpot) segSpot.style.width = `${spotPct.toFixed(2)}%`;
  if (segVault) segVault.style.width = `${vaultPct.toFixed(2)}%`;

  const perpLabel = isUnified ? 'Unified' : 'Perps';
  barEl.setAttribute(
    'aria-label',
    `Portfolio allocation: ${perpLabel} ${perpPct.toFixed(1)}%${
      !isUnified ? `, Spot ${spotPct.toFixed(1)}%` : ''
    }, Vault ${vaultPct.toFixed(1)}%`
  );

  const legendItems = [
    createLegendSwatchItem('#00f2fe', `${perpLabel}:`, `${fmtUsd(perpEq, 2)} (${perpPct.toFixed(1)}%)`),
  ];
  if (!isUnified) {
    legendItems.push(
      createLegendSwatchItem('#8b5cf6', 'Spot:', `${fmtUsd(spotEq, 2)} (${spotPct.toFixed(1)}%)`)
    );
  }
  legendItems.push(
    createLegendSwatchItem('#10b981', 'Vault:', `${fmtUsd(vaultEq, 2)} (${vaultPct.toFixed(1)}%)`)
  );
  legendEl.replaceChildren(...legendItems);

  // Metric 1: Actual % vs Target % + USDC Delta
  const targetBox = document.createElement('div');
  targetBox.className = 'vault-metric-box';
  const targetLbl = document.createElement('span');
  targetLbl.className = 'vault-metric-label';
  targetLbl.textContent = 'Actual vs Target Vault Share';

  const targetVal = document.createElement('div');
  targetVal.className = 'vault-metric-val';
  targetVal.textContent = `${vaultPct.toFixed(1)}% actual · ${targetPct.toFixed(1)}% target`;

  const targetSub = document.createElement('span');
  targetSub.className = 'vault-metric-sub';
  if (targetPct <= 0.001) {
    targetSub.textContent = 'Target allocation disabled (0%) · Informational barbell benchmark';
  } else {
    const targetUsdc = combined * (targetPct / 100.0);
    const deltaUsdc = targetUsdc - vaultEq;
    if (Math.abs(deltaUsdc) < 1.0) {
      targetSub.classList.add('pnl-pos');
      targetSub.textContent = `On target (${fmtUsd(targetUsdc, 2)} benchmark)`;
    } else if (deltaUsdc > 0) {
      targetSub.classList.add('cell-sl-dist');
      targetSub.textContent = `Delta: +${fmtUsd(deltaUsdc, 2)} under target (${fmtUsd(targetUsdc, 2)})`;
    } else {
      targetSub.classList.add('pnl-pos');
      targetSub.textContent = `Delta: -${fmtUsd(Math.abs(deltaUsdc), 2)} above target (${fmtUsd(targetUsdc, 2)})`;
    }
  }
  targetBox.append(targetLbl, targetVal, targetSub);

  // Metric 2: Realized PnL vs Profit-Sweep Threshold
  const sweepBox = document.createElement('div');
  sweepBox.className = 'vault-metric-box';
  const sweepLbl = document.createElement('span');
  sweepLbl.className = 'vault-metric-label';
  sweepLbl.textContent = 'Realized PnL vs Sweep Threshold';

  const realizedYtd = toFiniteNumber(dash?.pnl_summary?.ytd?.realized_pnl_usdc) ?? 0;
  const realizedMonthly = toFiniteNumber(dash?.pnl_summary?.monthly?.realized_pnl_usdc) ?? 0;

  const sweepVal = document.createElement('div');
  sweepVal.className = `vault-metric-val ${pnlToneClass(realizedYtd, 0.005)}`;
  sweepVal.textContent = `YTD ${fmtSignedUsd(realizedYtd, 2)} (30D ${fmtSignedUsd(realizedMonthly, 2)})`;

  const sweepSub = document.createElement('span');
  sweepSub.className = 'vault-metric-sub';
  if (sweepThreshold <= 0.001) {
    sweepSub.textContent = 'Profit-sweep reminder disabled ($0 threshold)';
  } else if (realizedYtd >= sweepThreshold) {
    sweepSub.classList.add('pnl-pos');
    sweepSub.textContent = `SWEEP READY · Exceeds ${fmtUsd(sweepThreshold, 2)} threshold`;
  } else {
    const remaining = Math.max(0, sweepThreshold - realizedYtd);
    sweepSub.textContent = `${fmtUsd(remaining, 2)} to reach ${fmtUsd(sweepThreshold, 2)} sweep trigger`;
  }
  sweepBox.append(sweepLbl, sweepVal, sweepSub);

  metricsEl.replaceChildren(targetBox, sweepBox);
}

function createReadinessItem(nameText, badgeCls, badgeText, titleText = '') {
  const row = document.createElement('div');
  row.className = 'vault-readiness-item';
  if (titleText) row.title = titleText;

  const nameSpan = document.createElement('span');
  nameSpan.className = 'vault-readiness-name';
  nameSpan.textContent = nameText;

  const pill = createStatusPillEl(badgeCls, badgeText);
  row.append(nameSpan, pill);
  return row;
}

function renderVaultReadinessCard(dash) {
  const container = document.getElementById('vault-readiness-badges');
  if (!container) return;

  const pkVal = getConfigValueFromDash(dash, 'private_key', '').trim();
  const hasAgentKey = Boolean(pkVal) && pkVal !== '[UNSET]';
  const agentAddr = getConfigValueFromDash(dash, 'hyperliquid_agent_address', '').trim();
  const keyItem = createReadinessItem(
    'Trade-Only API Agent Key',
    hasAgentKey ? 'open' : 'working',
    hasAgentKey ? (agentAddr ? 'AGENT BOUND' : 'ENCRYPTED') : 'UNSET',
    'Aegis signs perp orders with a Trade-Only API Agent key (vaultAddress: null); master wallet keys are never required for automated trading.'
  );

  const stopAtr = getConfigFloatFromDash(dash, 'protective_stop_atr_mult', 2.2);
  const tpAtr = getConfigFloatFromDash(dash, 'take_profit_atr_mult', 2.8);
  const attachedTpsl = Number(dash?.attached_tpsl_order_count ?? 0);
  const tpslItem = createReadinessItem(
    'Hard SL/TP Orders',
    'open',
    attachedTpsl > 0
      ? `${attachedTpsl} LIVE · ${stopAtr.toFixed(1)}x/${tpAtr.toFixed(1)}x`
      : `ARMED ${stopAtr.toFixed(1)}x/${tpAtr.toFixed(1)}x ATR`,
    'Protective stop-loss and take-profit trigger orders are enforced on Hyperliquid.'
  );

  const pgEnabled =
    getConfigValueFromDash(dash, 'profit_gate_signal_exits', 'true')
      .trim()
      .toLowerCase() === 'true';
  const pgMinPct =
    getConfigNonNegFloatFromDash(dash, 'signal_exit_min_net_profit_pct', 0.0025) * 100.0;
  const pgItem = createReadinessItem(
    'Profit-Gated Exits',
    pgEnabled ? 'open' : 'working',
    pgEnabled ? `ON (≥${pgMinPct.toFixed(2)}%)` : 'OFF',
    'Blocks low-conviction signal CLOSE/FLIP exits below minimum net profit while keeping hard SL/TP unrestricted.'
  );

  const dd = dash?.drawdown_breaker || {};
  const maxDdPct =
    toFiniteNumber(dd.max_daily_drawdown_pct) ??
    getConfigFloatFromDash(dash, 'max_daily_drawdown_pct', 0.15) * 100.0;
  const ddTripped = Boolean(dd.tripped);
  const ddItem = createReadinessItem(
    'Daily Drawdown Breaker',
    ddTripped ? 'closed' : 'open',
    ddTripped
      ? `HALTED (-${Math.abs(Number(dd.drawdown_pct ?? 0)).toFixed(1)}%)`
      : `ARMED (${maxDdPct.toFixed(1)}% CAP)`,
    '24-hour peak-to-trough perp equity drawdown circuit breaker (vault deposits excluded from drawdown calculation).'
  );

  container.replaceChildren(keyItem, tpslItem, pgItem, ddItem);
}

async function inspectVaultAddress(rawAddress) {
  if (state.vaultModal.inFlightInspect) return;
  const addr = String(rawAddress || '').trim();
  const addrInput = document.getElementById('vault-address-input');

  if (!addr) {
    setVaultFormStatus(
      'Enter a 42-character 0x Hyperliquid vault address first, or click "Use Official HLP".',
      'warn'
    );
    addrInput?.focus();
    return;
  }

  if (!/^0[xX][0-9a-fA-F]{40}$/.test(addr)) {
    if (addrInput) {
      addrInput.value = addr;
      addrInput.reportValidity();
      VaultFormValidation.syncAria(addrInput, true);
    }
    setVaultFormStatus(
      'Invalid vault address format — expected 0x followed by 40 hexadecimal characters.',
      'error'
    );
    return;
  }

  const inspectBtn = document.getElementById('btn-vault-inspect');
  state.vaultModal.inFlightInspect = true;
  if (inspectBtn) {
    inspectBtn.disabled = true;
    inspectBtn.textContent = 'Inspecting...';
  }
  setVaultFormStatus(`Querying live Hyperliquid vaultDetails for ${addr}...`, 'info');

  try {
    const res = await apiFetch('/api/v1/vault/inspect', {
      method: 'POST',
      body: JSON.stringify({ vault_address: addr }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      setVaultFormStatus(
        `Vault inspection failed: ${data.detail || data.error || `HTTP ${res.status}`}`,
        'error'
      );
      return;
    }

    const netLabel = data.is_testnet ? 'Testnet' : 'Mainnet';
    if (!data.found || !data.vault) {
      setVaultFormStatus(
        `No vault found at ${addr} on Hyperliquid ${netLabel}. Confirm the address and active execution mode.`,
        'warn'
      );
      return;
    }

    state.vaultModal.isPreviewMode = true;
    state.vaultModal.previewVault = data.vault;
    state.vaultModal.previewMeta = {
      execution_mode: data.execution_mode,
      is_testnet: Boolean(data.is_testnet),
      inspected_at_ms: data.inspected_at_ms || Date.now(),
    };

    const dash = state.dashboard || {};
    renderVaultModalSyncedTabs(dash);
    renderVaultFactsDisplay(dash);

    const vName = String(data.vault.name || addr);
    setVaultFormStatus(
      `Loaded live ${netLabel} facts for "${vName}". Click "Save & Sync Vault" to persist this configuration.`,
      'success'
    );
  } catch (err) {
    setVaultFormStatus(`Vault inspection error: ${err.message}`, 'error');
  } finally {
    state.vaultModal.inFlightInspect = false;
    if (inspectBtn) {
      inspectBtn.disabled = false;
      inspectBtn.textContent = 'Inspect Live Facts';
    }
  }
}

function triggerVaultInspectFromInput() {
  const addrInput = document.getElementById('vault-address-input');
  if (!addrInput) return;
  addrInput.value = addrInput.value.trim();
  inspectVaultAddress(addrInput.value);
}

function applyOfficialHlpPreset() {
  const addrInput = document.getElementById('vault-address-input');
  if (!addrInput) return;
  const info = getOfficialHlpForCurrentMode(state.dashboard);
  addrInput.value = info.address;
  addrInput.setCustomValidity('');
  VaultFormValidation.syncAria(addrInput, false);
  inspectVaultAddress(info.address);
}

function clearVaultAddressInput() {
  const addrInput = document.getElementById('vault-address-input');
  if (!addrInput) return;
  addrInput.value = '';
  addrInput.setCustomValidity('');
  VaultFormValidation.syncAria(addrInput, false);
  state.vaultModal.isPreviewMode = false;
  state.vaultModal.previewVault = null;
  state.vaultModal.previewMeta = null;
  if (state.dashboard) {
    renderVaultModalSyncedTabs(state.dashboard);
    renderVaultFactsDisplay(state.dashboard);
  }
  setVaultFormStatus(
    'Cleared tracked vault address. Click "Save & Sync Vault" to remove the configured vault.',
    'info'
  );
  addrInput.focus();
}

async function triggerVaultLiveSync() {
  if (state.vaultModal.inFlightSync) return;
  const btn = document.getElementById('btn-vault-refresh-sync');
  state.vaultModal.inFlightSync = true;
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Syncing...';
  }
  setVaultFormStatus('Syncing live Hyperliquid vault equities & L1 metadata...', 'info');

  try {
    const res = await apiFetch('/api/v1/vault/sync', {
      method: 'POST',
      body: JSON.stringify({}),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      setVaultFormStatus(
        `Live vault sync failed: ${data.detail || data.error || `HTTP ${res.status}`}`,
        'error'
      );
      showToast(`Vault sync failed: ${data.detail || data.error || res.status}`, true);
      return;
    }

    state.vaultModal.isPreviewMode = false;
    state.vaultModal.previewVault = null;
    state.vaultModal.previewMeta = null;

    await refreshAll(true);
    if (state.dashboard) {
      refreshOpenVaultModalFromDashboard(state.dashboard);
    }

    const syncedVaultCount = Number(
      data?.vault_info?.vault_count ??
        (Array.isArray(data?.vault_info?.vaults) ? data.vault_info.vaults.length : 0)
    );
    const tsLabel = fmtLocalShortTime(data?.synced_at_ms || Date.now());
    const msg =
      data.synced === false
        ? `Vault sync skipped in ${data.execution_mode || 'PAPER'} mode (no live wallet or configured vault).`
        : `Live Hyperliquid vault sync completed (${syncedVaultCount} ${
            syncedVaultCount === 1 ? 'vault' : 'vaults'
          } · ${tsLabel}).`;
    setVaultFormStatus(msg, data.synced === false ? 'warn' : 'success');
    showToast(msg);
  } catch (err) {
    setVaultFormStatus(`Live vault sync error: ${err.message}`, 'error');
    showToast(`Vault sync error: ${err.message}`, true);
  } finally {
    state.vaultModal.inFlightSync = false;
    if (btn) {
      btn.disabled = false;
      btn.textContent = '⟳ Refresh Live Sync';
    }
  }
}

async function submitVaultManagerForm(evt) {
  evt.preventDefault();
  const form = evt.currentTarget;
  if (!form || state.vaultModal.inFlightSave) return;

  const addrInput = document.getElementById('vault-address-input');
  if (addrInput) {
    addrInput.value = addrInput.value.trim();
    addrInput.setCustomValidity('');
  }

  if (!form.checkValidity()) {
    form.reportValidity();
    return;
  }

  const vaultAddr = addrInput ? addrInput.value.trim() : '';
  const roleVal =
    form.querySelector('input[name="vault_role"]:checked')?.value || 'follower';
  const allocInput = document.getElementById('vault-target-alloc-input');
  const sweepInput = document.getElementById('vault-sweep-threshold-input');
  const notesInput = document.getElementById('vault-notes-input');

  const allocNum = toFiniteNumber(allocInput?.value);
  const sweepNum = toFiniteNumber(sweepInput?.value);
  const notesVal = String(notesInput?.value || '').trim().slice(0, 500);

  const submitBtn = document.getElementById('btn-vault-save-sync');
  state.vaultModal.inFlightSave = true;
  if (submitBtn) {
    submitBtn.disabled = true;
    submitBtn.textContent = 'Saving & Syncing...';
  }
  setVaultFormStatus('Saving vault maintenance settings to MariaDB...', 'info');

  try {
    const cfgRes = await apiFetch('/api/v1/config', {
      method: 'POST',
      body: JSON.stringify({
        updates: {
          hyperliquid_vault_address: vaultAddr,
          vault_role: roleVal,
          vault_target_allocation_pct: String(allocNum !== null ? allocNum : 0),
          vault_profit_sweep_threshold_usdc: String(sweepNum !== null ? sweepNum : 0),
          vault_notes: notesVal,
        },
      }),
    });
    const cfgData = await cfgRes.json().catch(() => ({}));
    if (!cfgRes.ok) {
      const errMsg = `Save failed: ${cfgData.detail || cfgData.error || `HTTP ${cfgRes.status}`}`;
      setVaultFormStatus(errMsg, 'error');
      showToast(errMsg, true);
      return;
    }

    setVaultFormStatus(
      'Configuration saved. Syncing live Hyperliquid vault telemetry...',
      'info'
    );

    const syncRes = await apiFetch('/api/v1/vault/sync', {
      method: 'POST',
      body: JSON.stringify({}),
    });
    const syncData = await syncRes.json().catch(() => ({}));

    state.vaultModal.isPreviewMode = false;
    state.vaultModal.previewVault = null;
    state.vaultModal.previewMeta = null;
    if (vaultAddr) {
      state.vaultModal.selectedAddress = vaultAddr.toLowerCase();
    }

    await refreshAll(true);
    if (state.dashboard) {
      refreshOpenVaultModalFromDashboard(state.dashboard);
    }

    if (!syncRes.ok) {
      const warnMsg = `Saved vault settings, but live sync returned: ${
        syncData.detail || syncData.error || `HTTP ${syncRes.status}`
      }`;
      setVaultFormStatus(warnMsg, 'warn');
      showToast(warnMsg, true);
      return;
    }

    const vCount = Number(
      syncData?.vault_info?.vault_count ??
        (Array.isArray(syncData?.vault_info?.vaults) ? syncData.vault_info.vaults.length : 0)
    );
    const doneMsg =
      syncData.synced === false
        ? `Vault configuration saved (${syncData.execution_mode || 'PAPER'} mode — live sync inactive).`
        : `Saved & synced vault configuration (${vCount} ${
            vCount === 1 ? 'vault' : 'vaults'
          } live).`;
    setVaultFormStatus(doneMsg, 'success');
    showToast(doneMsg);
  } catch (err) {
    const errMsg = `Vault save/sync error: ${err.message}`;
    setVaultFormStatus(errMsg, 'error');
    showToast(errMsg, true);
  } finally {
    state.vaultModal.inFlightSave = false;
    if (submitBtn) {
      submitBtn.disabled = false;
      submitBtn.textContent = 'Save & Sync Vault';
    }
  }
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
  renderVaultPanel(dash);
  renderPriorityAndWorkingTable(dash);
  renderOpenHoldingsTable(dash);
  renderClosedPositionsTable(dash);
  renderCooldownsPanel(dash);
  renderActiveExplorerTab(forceConfig);
  drawEquityCurveCanvas(dash);
  drawSleeveAttributionCanvas(dash);
  const vaultDialog = document.getElementById('vault-manager-modal');
  if (vaultDialog && vaultDialog.open) {
    refreshOpenVaultModalFromDashboard(dash);
  }
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
        if (data && (data.snapshot || data.v_assets_held || data.vault_info)) {
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
  if (target.closest('#btn-open-vault-modal')) {
    openVaultModal(null);
    return;
  }
  if (target.closest('#btn-close-vault-modal')) {
    if (!SUPPORTS_INVOKER_COMMANDS) {
      const dlg = document.getElementById('vault-manager-modal');
      if (dlg && dlg.open) dlg.close('close-button');
    }
    return;
  }
  const inspectVaultRowBtn = target.closest(
    '.vault-row-inspect-btn[data-action="inspect-vault"]'
  );
  if (inspectVaultRowBtn) {
    openVaultModal(inspectVaultRowBtn.getAttribute('data-vault-address') || '');
    return;
  }
  if (target.closest('#btn-vault-inspect')) {
    triggerVaultInspectFromInput();
    return;
  }
  if (target.closest('#btn-vault-use-hlp')) {
    applyOfficialHlpPreset();
    return;
  }
  if (target.closest('#btn-vault-clear-addr')) {
    clearVaultAddressInput();
    return;
  }
  if (target.closest('#btn-vault-refresh-sync')) {
    triggerVaultLiveSync();
    return;
  }
  const syncedVaultChip = target.closest(
    '.vault-chip-btn[data-action="select-synced-vault"]'
  );
  if (syncedVaultChip) {
    state.vaultModal.isPreviewMode = false;
    state.vaultModal.selectedAddress = String(
      syncedVaultChip.getAttribute('data-vault-address') || ''
    )
      .trim()
      .toLowerCase();
    if (state.dashboard) {
      renderVaultModalSyncedTabs(state.dashboard);
      renderVaultFactsDisplay(state.dashboard);
    }
    return;
  }
  if (target.closest('.vault-chip-btn[data-action="select-preview-vault"]')) {
    if (state.vaultModal.previewVault) {
      state.vaultModal.isPreviewMode = true;
      const dash = state.dashboard || {};
      renderVaultModalSyncedTabs(dash);
      renderVaultFactsDisplay(dash);
    }
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
  initSortableTables();
  initVaultModalDialogAndForm();

  const scannerSearch = document.getElementById('scanner-search-input');
  if (scannerSearch) {
    scannerSearch.addEventListener('input', () => renderActiveExplorerTab());
  }
  const closedSearch = document.getElementById('closed-search-input');
  if (closedSearch) {
    closedSearch.addEventListener('input', () => {
      if (state.dashboard) renderClosedPositionsTable(state.dashboard);
    });
  }
  const modalSearch = document.getElementById('modal-cfg-search');
  if (modalSearch) {
    modalSearch.addEventListener('input', () => renderSystemConfigTables());
  }
  const authForm = document.getElementById('auth-login-form');
  if (authForm) {
    authForm.addEventListener('submit', submitAuthLogin);
  }

  // Interactive local-timezone hover tooltip & crosshair on Equity Curve canvas
  const eqCanvas = document.getElementById('equity-curve-canvas');
  const eqTooltip = document.getElementById('equity-chart-tooltip');
  if (eqCanvas && eqTooltip) {
    eqCanvas.addEventListener('mousemove', (evt) => {
      const pts = state.equityChartPoints || [];
      if (pts.length === 0) return;
      const rect = eqCanvas.getBoundingClientRect();
      const mouseX = evt.clientX - rect.left;
      let nearestIdx = 0;
      let bestDist = Math.abs(mouseX - pts[0].x);
      for (let i = 1; i < pts.length; i++) {
        const d = Math.abs(mouseX - pts[i].x);
        if (d < bestDist) {
          bestDist = d;
          nearestIdx = i;
        }
      }
      const nearest = pts[nearestIdx];
      if (state.equityHoverIdx !== nearestIdx) {
        state.equityHoverIdx = nearestIdx;
        if (state.dashboard) drawEquityCurveCanvas(state.dashboard);
      }

      const pt = nearest.pt;
      const timeDiv = document.createElement('div');
      timeDiv.style.color = 'var(--neon-cyan)';
      timeDiv.style.fontWeight = '700';
      timeDiv.style.marginBottom = '3px';
      timeDiv.textContent = fmtLocalTime(pt.created_at_ms);

      const eqDiv = document.createElement('div');
      const eqStrong = document.createElement('strong');
      eqStrong.textContent = fmtUsd(pt.total_equity_usdc ?? 0);
      eqDiv.append(document.createTextNode('Equity: '), eqStrong);

      const commDiv = document.createElement('div');
      commDiv.style.color = '#34d399';
      commDiv.textContent = `Committed: ${fmtUsd(pt.committed_notional_usdc ?? 0)}`;

      const resDiv = document.createElement('div');
      resDiv.style.color = '#fbbf24';
      resDiv.textContent = `Reserved: ${fmtUsd(pt.reserved_order_cash_usdc ?? 0)}`;

      eqTooltip.replaceChildren(timeDiv, eqDiv, commDiv, resDiv);
      eqTooltip.hidden = false;
      const tipX = Math.min(rect.width - 175, Math.max(8, nearest.x + 12));
      const tipY = Math.max(8, Math.min(rect.height - 84, nearest.y - 24));
      eqTooltip.style.left = `${tipX}px`;
      eqTooltip.style.top = `${tipY}px`;
    });
    eqCanvas.addEventListener('mouseleave', () => {
      eqTooltip.hidden = true;
      eqTooltip.replaceChildren();
      if (state.equityHoverIdx !== null) {
        state.equityHoverIdx = null;
        if (state.dashboard) drawEquityCurveCanvas(state.dashboard);
      }
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
