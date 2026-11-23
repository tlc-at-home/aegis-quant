/* ============================================================================
   AEGIS QUANT v2.0 — FORMATTERS, SAFE DOM BUILDER & SORT COMPARATORS
   ============================================================================ */

export const OFFICIAL_HLP_TESTNET = '0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0';
export const OFFICIAL_HLP_MAINNET = '0xdfc24b077bc1425ad1dea75bcb6f8158e10df303';

const SORT_COLLATOR = new Intl.Collator(undefined, {
  numeric: true,
  sensitivity: 'base',
});

export function toFiniteNumber(val) {
  if (val === null || val === undefined || val === '') return null;
  const n = Number(val);
  return Number.isFinite(n) ? n : null;
}

export function fmtUsd(val, decimals = 2) {
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

export function fmtCompactUsd(val) {
  const n = toFiniteNumber(val);
  if (n === null) return '—';
  const abs = Math.abs(n);
  const sign = n < 0 ? '-' : '';
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(2)}M`;
  return fmtUsd(n, 2);
}

export function isOfficialHlpAddress(rawAddr) {
  const norm = String(rawAddr || '').trim().toLowerCase();
  return norm === OFFICIAL_HLP_TESTNET || norm === OFFICIAL_HLP_MAINNET;
}

export function fmtSignedUsd(val, decimals = 2) {
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

export function fmtSignedPct(pct, decimals = 2) {
  const n = toFiniteNumber(pct);
  if (n === null) return null;
  const eps = 0.5 * Math.pow(10, -decimals);
  const sign = n >= eps ? '+' : n <= -eps ? '-' : '';
  return `${sign}${Math.abs(n).toFixed(decimals)}%`;
}

export function pnlToneClass(val, eps = 0.001) {
  const n = toFiniteNumber(val);
  if (n === null) return 'pnl-flat';
  if (n > eps) return 'pnl-pos';
  if (n < -eps) return 'pnl-neg';
  return 'pnl-flat';
}

export function fmtPrice(px) {
  const n = Number(px || 0);
  if (n === 0) return '$0.00';
  if (n >= 1000) return fmtUsd(n, 2);
  if (n >= 1) return fmtUsd(n, 4);
  return '$' + n.toFixed(6);
}

export function fmtPct(fracOrPct, isFraction = true, decimals = 1) {
  const v = Number(fracOrPct || 0) * (isFraction ? 100.0 : 1.0);
  const sign = v > 0.0001 ? '+' : '';
  return `${sign}${v.toFixed(decimals)}%`;
}

export function getLocalTzAbbrev(dateObj) {
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

export function fmtLocalTime(tsMs) {
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

export function fmtLocalShortTime(tsMs) {
  if (!tsMs || tsMs <= 0) return '';
  const d = new Date(Number(tsMs));
  if (Number.isNaN(d.getTime())) return '';
  const hh = String(d.getHours()).padStart(2, '0');
  const mi = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return `${hh}:${mi}:${ss}`;
}

export function fmtLocalMonthDayTime(tsMs) {
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

export function fmtCompactDuration(ms) {
  const safeMs = Math.max(0, Number(ms || 0));
  if (safeMs >= 86_400_000) {
    return `${Math.max(1, Math.round(safeMs / 86_400_000))}d`;
  }
  if (safeMs >= 3_600_000) {
    return `${Math.max(1, Math.round(safeMs / 3_600_000))}h`;
  }
  return `${Math.max(1, Math.round(safeMs / 60_000))}m`;
}

export function formatCooldownRemaining(secRaw) {
  const sec = Math.max(0, Math.floor(Number(secRaw ?? 0)));
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

export const fmtUtcTime = fmtLocalTime;

export function computeOpenPnlPct(hItem) {
  const pnl = toFiniteNumber(hItem?.unrealized_pnl_usdc);
  const entryPx = toFiniteNumber(hItem?.entry_price);
  const qty = toFiniteNumber(hItem?.quantity ?? hItem?.filled_quantity);
  let notional =
    entryPx !== null && qty !== null && entryPx > 0 && Math.abs(qty) > 0
      ? entryPx * Math.abs(qty)
      : null;
  if (!(notional > 0)) {
    const committed = toFiniteNumber(hItem?.committed_notional_usdc);
    notional = committed !== null && committed > 0 ? committed : null;
  }

  const backendPct = toFiniteNumber(hItem?.unrealized_pnl_pct);
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

export function computeClosedEntryNotional(p) {
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

export function computeClosedPnlPct(p) {
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

export function computeVaultPnlPct(v, pctKey, pnlKey) {
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

/** R14-H1: Fall back to immutable created_at_ms when opened_at_ms is null (never updated_at_ms). */
export function isPreCapHolding(heldItem, capEpochMs) {
  if (!(capEpochMs > 0)) return false;
  const rawTs = heldItem?.opened_at_ms ?? heldItem?.created_at_ms;
  if (rawTs === null || rawTs === undefined) return false;
  const tsMs = Number(rawTs);
  return Number.isFinite(tsMs) && tsMs > 0 && tsMs < capEpochMs;
}

export function getConfigFloatFromDash(dash, key, fallback) {
  const row = (dash?.system_config || []).find((r) => r.config_key === key);
  if (!row) return fallback;
  const n = Number(row.config_value);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

export function getConfigNonNegFloatFromDash(dash, key, fallback = 0) {
  const row = (dash?.system_config || []).find((r) => r.config_key === key);
  if (!row) return fallback;
  const n = toFiniteNumber(row.config_value);
  return n !== null && n >= 0 ? n : fallback;
}

export function getConfigValueFromDash(dash, key, fallback = '') {
  const row = (dash?.system_config || []).find((r) => r.config_key === key);
  if (!row || row.config_value === null || row.config_value === undefined) {
    return fallback;
  }
  return String(row.config_value);
}

/**
 * Unified CSP-safe DOM element builder (strictly sets textContent/properties/attributes,
 * never raw markup or inline styles).
 */
export function h(tag, props = null, ...children) {
  const el = document.createElement(tag);
  if (props && typeof props === 'object') {
    for (const [k, v] of Object.entries(props)) {
      if (v === null || v === undefined || v === false) continue;
      if (k === 'className') {
        el.className = String(v);
      } else if (k === 'text') {
        el.textContent = String(v);
      } else if (k === 'dataset' && typeof v === 'object') {
        for (const [dk, dv] of Object.entries(v)) {
          if (dv !== null && dv !== undefined) {
            el.dataset[dk] = String(dv);
          }
        }
      } else if (k === 'attrs' && typeof v === 'object') {
        for (const [ak, av] of Object.entries(v)) {
          if (av !== null && av !== undefined && av !== false) {
            el.setAttribute(ak, av === true ? '' : String(av));
          }
        }
      } else if (
        k === 'id' ||
        k === 'type' ||
        k === 'title' ||
        k === 'colSpan' ||
        k === 'value' ||
        k === 'placeholder' ||
        k === 'disabled' ||
        k === 'selected' ||
        k === 'hidden'
      ) {
        el[k] = v;
      }
    }
  }
  for (const child of children) {
    if (child === null || child === undefined || child === false) continue;
    if (Array.isArray(child)) {
      for (const nested of child) {
        if (nested !== null && nested !== undefined && nested !== false) {
          el.append(typeof nested === 'string' ? document.createTextNode(nested) : nested);
        }
      }
    } else if (typeof child === 'string' || typeof child === 'number') {
      el.append(document.createTextNode(String(child)));
    } else {
      el.append(child);
    }
  }
  return el;
}

export function populatePnlElement(el, pnlUsdc, pnlPct, extraClass = '') {
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

export function createAddressChip(rawAddress) {
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
  const head = document.createElement('span');
  head.className = 'addr-head';
  head.textContent = full.slice(0, -4);
  const tail = document.createTextNode(full.slice(-4));
  code.append(head, tail);
  return code;
}

export function createTableSmall(cls, text) {
  return h('small', { className: cls, text });
}

export function createSleeveTag(cls, title, text) {
  return h('span', { className: `sp-tag ${cls}`, title, text });
}

export function createStatusPillEl(cls, text) {
  return h('span', { className: `status-pill ${cls}`, text });
}

export function createEmptyRow(colSpan, text, cellClass = 'table-empty-cell') {
  return h(
    'tr',
    { dataset: { emptyRow: 'true' } },
    h('td', { colSpan, className: cellClass, text })
  );
}

export function createWalletStripItem(label, valText, valClass = 'kpi-wallet-val') {
  return h(
    'span',
    { className: 'kpi-wallet-item' },
    h('span', { className: 'kpi-wallet-label', text: label }),
    h('span', { className: valClass, text: valText })
  );
}

export function parseSortValue(raw, type) {
  if (raw === null || raw === undefined || raw === '') return null;
  if (type === 'number' || type === 'date') {
    const n = Number(raw);
    return Number.isFinite(n) ? n : null;
  }
  const s = String(raw).trim();
  return s.length > 0 && s !== '—' ? s : null;
}

export function compareSortValues(rawA, rawB, type, dir) {
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
