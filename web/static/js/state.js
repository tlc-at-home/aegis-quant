/* ============================================================================
   AEGIS QUANT v2.0 — APPLICATION STATE, DOM REGISTRY & FINGERPRINT CACHE
   ============================================================================ */

import { compareSortValues } from '/js/format.js';

export const state = {
  health: null,
  dashboard: null,
  degradedError: null,
  activeTab: 's1',
  configCategory: 'all',
  showAllPriorityWatch: false,
  tableSort: {},
  closingPositionIds: new Set(),
  closingSymbols: new Set(),
  closeInFlightCount: 0,
  sse: null,
  authRequired: false,
  lastAppliedSeq: 0,
  lastAppliedAtMs: 0,
  serverBootId: 0,
  refreshInFlight: null,
  refreshAbortController: null,
  consecutiveRefreshFailures: 0,
  sectionFingerprints: new Map(),
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
  runModeModal: {
    inFlight: false,
  },
};

export const DEFAULT_IMMUTABLE_SECURITY_KEYS = new Set([
  'enforce_master_key',
  'web_auth_enabled',
  'web_require_auth',
  'web_allow_remote_bind',
  'web_bind_host',
  'web_csp_policy',
  'web_cors_allowed_origins',
  'web_allowed_hosts',
  'web_trust_proxy_headers',
  'web_trusted_proxies',
  'web_cookie_secure',
  'web_api_token',
  'allow_web_live_mode',
  'telegram_api_base_url',
  'hyperliquid_testnet',
  'hyperliquid_mainnet_url',
  'hyperliquid_testnet_url',
  'hyperliquid_info_url',
  'hyperliquid_testnet_info_url',
  'hyperliquid_exchange_url',
  'hyperliquid_testnet_exchange_url',
  'hyperliquid_allowed_hosts',
  'platform_version',
  'schema_migration_version',
  'trading_run_mode_updated_at_ms',
  'risk_capital_cap_updated_at_ms',
  'hyperliquid_last_nonce',
  'last_db_backup_ms',
  'last_entry_order_ms',
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
  'live_exchange_maker_fee_bps',
  'live_exchange_taker_fee_bps',
  'priority_monitor_health_json',
  'ws_stream_status_json',
]);

export function isImmutableConfigKey(key) {
  const lower = String(key || '').trim().toLowerCase();
  if (!lower) return false;
  if (DEFAULT_IMMUTABLE_SECURITY_KEYS.has(lower)) return true;
  const serverKeys = state.dashboard?.immutable_config_keys;
  if (Array.isArray(serverKeys)) {
    return serverKeys.some((k) => String(k || '').trim().toLowerCase() === lower);
  }
  return false;
}

export const ROW_SORT_DATA = new WeakMap();

export const SUPPORTS_INVOKER_COMMANDS =
  typeof HTMLButtonElement !== 'undefined' &&
  'commandForElement' in HTMLButtonElement.prototype &&
  'command' in HTMLButtonElement.prototype;

const STATIC_ELEMENT_IDS = [
  'platform-version-pill',
  'badge-mariadb',
  'mariadb-pulse-dot',
  'mariadb-version-text',
  'badge-prio-pulse',
  'prio-pulse-dot',
  'prio-watch-label',
  'prio-watch-count',
  'badge-scan-pulse',
  'scanner-asset-count',
  'badge-exec-mode',
  'exec-mode-text',
  'btn-open-run-mode-modal',
  'run-mode-btn-dot',
  'run-mode-btn-label',
  'risk-tier-selector',
  'btn-sync-hyperliquid',
  'btn-telegram-test',
  'btn-open-config-modal',
  'table-sort-status',
  'db-outage-banner',
  'surveillance-alert-banner',
  'run-mode-banner',
  'pnl-card-daily',
  'pnl-daily-source',
  'pnl-daily-clamp',
  'pnl-daily-pct',
  'pnl-daily-usdc',
  'pnl-daily-sub',
  'pnl-card-weekly',
  'pnl-weekly-source',
  'pnl-weekly-clamp',
  'pnl-weekly-pct',
  'pnl-weekly-usdc',
  'pnl-weekly-sub',
  'pnl-card-monthly',
  'pnl-monthly-source',
  'pnl-monthly-clamp',
  'pnl-monthly-pct',
  'pnl-monthly-usdc',
  'pnl-monthly-sub',
  'pnl-card-ytd',
  'pnl-ytd-source',
  'pnl-ytd-clamp',
  'pnl-ytd-pct',
  'pnl-ytd-usdc',
  'pnl-ytd-sub',
  'kpi-equity-title',
  'kpi-total-equity',
  'kpi-wallet-summary',
  'kpi-tier-badge',
  'kpi-eligible-coins',
  'kpi-equity-sync-sub',
  'kpi-equity-meter',
  'kpi-avail-cash',
  'kpi-cash-pct',
  'kpi-cash-meter',
  'kpi-reserved-margin',
  'kpi-working-count',
  'kpi-exchange-orders-sub',
  'kpi-reserved-meter',
  'kpi-committed-notional',
  'kpi-open-counts',
  'kpi-net-notional',
  'kpi-committed-meter',
  'kpi-gross-leverage',
  'kpi-net-leverage',
  'kpi-drawdown-pill',
  'kpi-ruin-halt-pill',
  'kpi-leverage-target',
  'kpi-leverage-cap',
  'kpi-leverage-meter',
  'kpi-regime-label',
  'kpi-breadth-pct',
  'kpi-beta-exp',
  'kpi-breadth-meter',
  'vault-configured-badge',
  'vault-sync-pill',
  'btn-open-vault-modal',
  'vault-total-equity',
  'vault-count-sub',
  'vault-synced-sub',
  'vault-combined-title',
  'vault-combined-equity',
  'vault-combined-breakdown',
  'vault-avg-apr',
  'vault-total-pnl',
  'vault-alltime-pnl',
  'vault-lock-summary',
  'table-vaults',
  'tbody-vaults',
  'legend-eq-val',
  'legend-uncapped-item',
  'legend-uncapped-val',
  'legend-comm-val',
  'legend-res-val',
  'equity-curve-canvas',
  'equity-curve-caption',
  'sleeve-total-gross',
  'sleeve-attr-canvas',
  'sleeve-attr-caption',
  'sleeve-s1-weight',
  'sleeve-s2-weight',
  'sleeve-s3-weight',
  'watch-split-note',
  'btn-toggle-watch-all',
  'table-priority-ordered',
  'tbody-priority-ordered',
  'btn-close-all-open',
  'held-count-pill',
  'table-assets-held',
  'tbody-assets-held',
  'closed-search-input',
  'closed-count-pill',
  'table-closed-positions',
  'tbody-closed-positions',
  'cooldown-count-pill',
  'table-cooldowns',
  'tbody-cooldowns',
  'tab-count-s1',
  'tab-count-s2',
  'tab-count-s3',
  'tab-count-scanner',
  'tab-count-console',
  'scanner-search-input',
  'table-sleeve-s1',
  'tbody-sleeve-s1',
  'table-sleeve-s2',
  'tbody-sleeve-s2',
  'table-sleeve-s3',
  'tbody-sleeve-s3',
  'table-perp-scanner',
  'tbody-perp-scanner',
  'table-order-audit',
  'tbody-order-audit',
  'telemetry-log-container',
  'cfg-key-input',
  'cfg-val-input',
  'btn-submit-config-update',
  'table-system-config',
  'tbody-system-config',
  'config-settings-modal',
  'btn-close-config-modal',
  'modal-cfg-search',
  'table-modal-system-config',
  'tbody-modal-system-config',
  'auth-login-modal',
  'auth-login-form',
  'auth-token-input',
  'auth-login-error',
  'btn-auth-login-submit',
  'vault-manager-modal',
  'vault-form-announcer',
  'vault-modal-network-pill',
  'btn-close-vault-modal',
  'vault-manager-form',
  'vault-address-input',
  'btn-vault-inspect',
  'btn-vault-use-hlp',
  'btn-vault-clear-addr',
  'vault-target-alloc-input',
  'vault-sweep-threshold-input',
  'vault-notes-input',
  'vault-notes-char-count',
  'vault-form-status',
  'btn-vault-save-sync',
  'btn-vault-refresh-sync',
  'vault-fact-source-pill',
  'vault-modal-synced-tabs',
  'vault-fact-name',
  'vault-fact-status-pills',
  'vault-fact-addresses',
  'vault-fact-description',
  'vault-facts-grid',
  'vault-barbell-role-pill',
  'vault-barbell-bar',
  'vault-bar-seg-perp',
  'vault-bar-seg-spot',
  'vault-bar-seg-vault',
  'vault-barbell-legend',
  'vault-barbell-metrics',
  'vault-readiness-badges',
  'run-mode-modal',
  'run-mode-modal-current-pill',
  'btn-close-run-mode-modal',
  'run-mode-exposure-strip',
  'run-mode-form',
  'run-mode-flatten-row',
  'run-mode-flatten-checkbox',
  'run-mode-cooldown-select',
  'run-mode-reason-input',
  'run-mode-impact-warning',
  'run-mode-form-status',
  'btn-cancel-run-mode',
  'btn-apply-run-mode',
  'toast-banner',
];

export const dom = {};

export function initDomRegistry() {
  for (const id of STATIC_ELEMENT_IDS) {
    const el = document.getElementById(id);
    if (el) {
      dom[id] = el;
    }
  }
}

export function getEl(id) {
  if (Object.hasOwn(dom, id) && dom[id] && dom[id].isConnected) {
    return dom[id];
  }
  const el = document.getElementById(id);
  if (el) {
    dom[id] = el;
  }
  return el;
}

/** Fast FNV-1a 32-bit fingerprint for dirty-checking individual UI sections. */
export function computeFingerprint(parts) {
  let hash = 0x811c9dc5;
  const feedStr = (s) => {
    for (let i = 0; i < s.length; i++) {
      hash ^= s.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    hash ^= 0x1f;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  };
  const visit = (val) => {
    if (val === null || val === undefined) {
      feedStr('~');
    } else if (Array.isArray(val)) {
      feedStr(`[${val.length}`);
      for (let i = 0; i < val.length; i++) visit(val[i]);
    } else if (typeof val === 'object') {
      const keys = Object.keys(val).sort();
      feedStr(`{${keys.length}`);
      for (const k of keys) {
        feedStr(k);
        visit(val[k]);
      }
    } else {
      feedStr(String(val));
    }
  };
  visit(parts);
  return hash.toString(36);
}

export function isSectionDirty(sectionKey, fp) {
  if (state.sectionFingerprints.get(sectionKey) === fp) {
    return false;
  }
  state.sectionFingerprints.set(sectionKey, fp);
  return true;
}

export function invalidateSection(sectionKey) {
  state.sectionFingerprints.delete(sectionKey);
}

export function resetAllFingerprints() {
  state.sectionFingerprints.clear();
}

export function syncTableSortHeaders(tableId) {
  const table = getEl(tableId);
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

export function sortItemsForTable(items, tableId, extractors) {
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

export function attachRowSortData(tbody, items, extractors) {
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

export function reorderTableRows(table, tbody, orderedRows) {
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
