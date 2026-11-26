/* ============================================================================
   AEGIS QUANT v2.0 — SYSTEM CONFIGURATION VAULT & FORENSIC CATEGORY MAPPING
   ============================================================================ */

import {
  state,
  getEl,
  isImmutableConfigKey,
  computeFingerprint,
  isSectionDirty,
  invalidateSection,
  sortItemsForTable,
  attachRowSortData,
} from '/js/state.js';
import { toFiniteNumber, fmtLocalTime, h, createEmptyRow } from '/js/format.js';
import { saveConfigKeyValue } from '/js/api.js';
import { showToast } from '/js/modals.js';

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

const PRIORITY_CONFIG_KEYS = [
  'profit_gate_signal_exits',
  'signal_exit_min_net_profit_pct',
  'max_entry_spread_bps',
  'max_single_asset_weight',
  'min_position_hold_sec',
  'max_alo_chase_ticks',
  'alo_chase_threshold_bps',
  's3_event_reversion_budget',
  'risk_capital_cap_usdc',
  'max_daily_drawdown_pct',
  'min_equity_halt_usdc',
  'risk_per_trade_equity_pct',
  'live_exchange_equity_usdc',
  'risk_tier',
  'target_leverage_multiplier',
  'max_open_positions',
  'min_entry_interval_sec',
  'max_gross_leverage_x',
  'execution_mode',
];

export function classifyConfigKeyCategory(key) {
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
    k === 'profit_gate_signal_exits' ||
    k === 'signal_exit_min_net_profit_pct' ||
    k.startsWith('early_loser_') ||
    k === 'min_equity_halt_usdc' ||
    k === 'risk_per_trade_equity_pct' ||
    k.startsWith('take_profit_') ||
    k.startsWith('breakeven_') ||
    k.startsWith('stagnation_') ||
    k.startsWith('hysteresis_') ||
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
    k.startsWith('max_alo_') ||
    k.startsWith('alo_') ||
    k.startsWith('entry_ioc_') ||
    k.startsWith('sl_trigger_') ||
    k.startsWith('passive_close_') ||
    k.startsWith('emergency_ioc_') ||
    k.startsWith('trigger_oracle_') ||
    k.startsWith('min_entry_l2_') ||
    k.startsWith('max_entry_oracle_') ||
    k.startsWith('testnet_') ||
    k.startsWith('core_min_') ||
    k.startsWith('event_min_') ||
    k.startsWith('auto_deleverage_') ||
    k.startsWith('retention_') ||
    k.startsWith('hl_') ||
    k.startsWith('candle_') ||
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
  if (
    k.startsWith('s1_') ||
    k.startsWith('s2_') ||
    k.startsWith('s3_') ||
    k === 'enable_breadth_regime_gate' ||
    k.startsWith('breadth_')
  ) {
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

export function setConfigCategory(cat) {
  state.configCategory = cat;
  document.querySelectorAll('.cfg-cat-btn[data-cat]').forEach((btn) => {
    btn.classList.toggle('active', btn.getAttribute('data-cat') === cat);
  });
  invalidateSection('config-tab');
  invalidateSection('config-modal');
  renderSystemConfigTables(true);
}

export function openConfigModal() {
  const modal = getEl('config-settings-modal');
  if (modal) {
    modal.classList.add('open');
    invalidateSection('config-modal');
    renderSystemConfigTables(true);
  }
}

export function closeConfigModal() {
  const modal = getEl('config-settings-modal');
  if (modal) {
    modal.classList.remove('open');
  }
}

export async function submitConfigUpdate() {
  const keyEl = getEl('cfg-key-input');
  const valEl = getEl('cfg-val-input');
  const key = keyEl ? keyEl.value.trim() : '';
  const val = valEl ? valEl.value.trim() : '';
  if (!key) {
    showToast('Please enter a config_key to update', true);
    return;
  }
  await saveConfigKeyValue(key, val);
  if (valEl) valEl.value = '';
}

export async function saveSingleConfigRow(key, inputId) {
  const el = document.getElementById(inputId);
  if (!el) return;
  const val = el.value.trim();
  if (el.getAttribute('data-is-secret') === 'true' && !val) {
    showToast(`Enter a new secret value before saving '${key}'`, true);
    return;
  }
  await saveConfigKeyValue(key, val);
}

function buildInlineConfigControlNode(row, idPrefix, rowIdx = 0) {
  const key = String(row.config_key || '');
  const val = String(row.config_value ?? '');
  const vtype = String(row.value_type || 'string').toLowerCase();
  const safeSlug = key.replace(/[^a-zA-Z0-9_-]/g, '_');
  const inputId = `${idPrefix}-cfg-input-${rowIdx}-${safeSlug}`;

  if (isImmutableConfigKey(key)) {
    return h('span', {
      className: 'pnl-flat',
      title: 'Immutable security policy or internal state key',
      text: 'Locked Policy',
    });
  }

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

  const saveBtn = h('button', {
    type: 'button',
    className: 'cfg-save-btn',
    text: 'Save',
    dataset: {
      action: 'save-config',
      key,
      inputId,
    },
  });

  if (enumOptions) {
    const sel = h('select', { id: inputId, className: 'cfg-inline-select' });
    for (const opt of enumOptions) {
      sel.appendChild(
        h('option', {
          value: opt,
          text: opt,
          selected: val.toLowerCase() === opt.toLowerCase(),
        })
      );
    }
    return h('div', { className: 'cfg-inline-controls' }, sel, saveBtn);
  }

  const isSecret =
    vtype === 'secret' ||
    val === '[FERNET_ENCRYPTED_SECRET]' ||
    val === '[UNSET]' ||
    key.includes('private_key') ||
    key.includes('token') ||
    key.includes('webhook');

  if (isSecret && key !== 'telegram_chat_id') {
    return h(
      'div',
      { className: 'cfg-inline-controls' },
      h('input', {
        type: 'password',
        id: inputId,
        className: 'cfg-inline-input',
        placeholder: 'Enter new secret to overwrite...',
        dataset: {
          isSecret: 'true',
          action: 'save-config-enter',
          key,
          inputId,
        },
      }),
      saveBtn
    );
  }

  const prefill = val === '[UNSET]' || val.includes('...') ? '' : val;
  const placeholder = val.includes('...') ? `${val} (enter full value)` : 'Enter value...';
  return h(
    'div',
    { className: 'cfg-inline-controls' },
    h('input', {
      type: 'text',
      id: inputId,
      className: 'cfg-inline-input',
      value: prefill,
      placeholder,
      dataset: {
        action: 'save-config-enter',
        key,
        inputId,
      },
    }),
    saveBtn
  );
}

function renderConfigRowsDom(tbody, rows, prefix) {
  if (rows.length === 0) {
    tbody.replaceChildren(
      createEmptyRow(6, 'No configuration keys matching filter.')
    );
    return;
  }
  const trList = rows.map((c, idx) =>
    h(
      'tr',
      null,
      h('td', null, h('strong', { className: 'cfg-key-name', text: String(c.config_key ?? '') })),
      h(
        'td',
        null,
        h('code', {
          className: 'cfg-val-code',
          title: String(c.config_value ?? ''),
          text: String(c.config_value ?? ''),
        })
      ),
      h('td', null, buildInlineConfigControlNode(c, prefix, idx)),
      h('td', null, h('span', { className: 'tab-count', text: String(c.value_type ?? '') })),
      h('td', { className: 'cfg-desc-cell', text: String(c.description ?? '') }),
      h('td', { className: 'num-cell', text: fmtLocalTime(c.updated_at_ms) })
    )
  );
  tbody.replaceChildren(...trList);
}

export function renderSystemConfigTables(force = false) {
  const dash = state.dashboard;
  if (!dash) return;

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
  const tabQuery = (getEl('scanner-search-input')?.value || '').trim().toUpperCase();
  const modalQuery = (getEl('modal-cfg-search')?.value || '').trim().toUpperCase();

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
          String(c.config_key || '').toUpperCase().includes(q) ||
          String(c.description || '').toUpperCase().includes(q)
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

  const tbTab = getEl('tbody-system-config');
  if (tbTab && (force || state.activeTab === 'config')) {
    const tabSort = state.tableSort['table-system-config'];
    const fpTab = computeFingerprint([configs, cat, tabQuery, tabSort, dash.immutable_config_keys]);
    if (force || isSectionDirty('config-tab', fpTab)) {
      const tabRows = sortItemsForTable(
        filterRows(tabQuery),
        'table-system-config',
        CONFIG_SORT_EXTRACTORS
      );
      tbTab.dataset.truncated = 'false';
      renderConfigRowsDom(tbTab, tabRows, 'tab');
      attachRowSortData(tbTab, tabRows, CONFIG_SORT_EXTRACTORS);
    }
  }

  const modalEl = getEl('config-settings-modal');
  const tbModal = getEl('tbody-modal-system-config');
  if (tbModal && modalEl && modalEl.classList.contains('open')) {
    const modalSort = state.tableSort['table-modal-system-config'];
    const fpModal = computeFingerprint([configs, cat, modalQuery, modalSort, dash.immutable_config_keys]);
    if (force || isSectionDirty('config-modal', fpModal)) {
      const modalRows = sortItemsForTable(
        filterRows(modalQuery),
        'table-modal-system-config',
        CONFIG_SORT_EXTRACTORS
      );
      tbModal.dataset.truncated = 'false';
      renderConfigRowsDom(tbModal, modalRows, 'modal');
      attachRowSortData(tbModal, modalRows, CONFIG_SORT_EXTRACTORS);
    }
  }
}
