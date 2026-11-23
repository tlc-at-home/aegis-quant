/* ============================================================================
   AEGIS QUANT v2.0 — REST API CLIENT, REFRESH DEDUP & REAL-TIME SSE STREAM
   ============================================================================ */

import { state, getEl, invalidateSection } from '/js/state.js';
import { fmtUsd, fmtSignedUsd } from '/js/format.js';

let hooks = {
  onAuthRequired: () => {},
  onApplyDashboard: () => {},
  onToast: () => {},
  onHealthUpdate: () => {},
  onOutageState: () => {},
  onRiskTierButtons: () => {},
};

export function configureApiHooks(nextHooks) {
  hooks = { ...hooks, ...nextHooks };
}

export function disconnectSSE() {
  if (state.sse) {
    try {
      state.sse.close();
    } catch (_) {}
    state.sse = null;
  }
}

export async function apiFetch(url, options = {}) {
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
    disconnectSSE();
    hooks.onAuthRequired();
  }
  return res;
}

export async function refreshAll(forceConfig = false) {
  if (state.authRequired) return;

  if (!forceConfig && state.refreshInFlight) {
    return state.refreshInFlight;
  }

  if (forceConfig && state.refreshAbortController) {
    state.refreshAbortController.abort();
    state.refreshAbortController = null;
  }

  const controller = new AbortController();
  state.refreshAbortController = controller;

  const task = (async () => {
    try {
      const [hRes, dRes] = await Promise.all([
        apiFetch('/api/v1/health', { signal: controller.signal }),
        apiFetch('/api/v1/dashboard', { signal: controller.signal }),
      ]);

      if (hRes.ok) {
        state.health = await hRes.json();
        hooks.onHealthUpdate(state.health);
      } else if (hRes.status === 503) {
        const errBody = await hRes.json().catch(() => ({}));
        state.health = {
          status: 'unavailable',
          detail: errBody.detail || 'Database health check failed',
          correlation_id: errBody.correlation_id || null,
        };
        hooks.onHealthUpdate(state.health);
      }

      if (dRes.ok) {
        state.consecutiveRefreshFailures = 0;
        const dash = await dRes.json();
        hooks.onApplyDashboard(dash, forceConfig);
      } else if (dRes.status === 503) {
        state.consecutiveRefreshFailures += 1;
        const errDash = await dRes.json().catch(() => ({}));
        hooks.onOutageState({
          degraded: true,
          database_reachable: errDash.database_reachable ?? false,
          degraded_sections: errDash.degraded_sections || ['database'],
          degraded_errors: errDash.degraded_errors || [
            errDash.detail || 'MariaDB critical dashboard section unavailable',
          ],
          correlation_id: errDash.correlation_id || null,
          snapshot_seq: errDash.snapshot_seq || 0,
          generated_at_ms: errDash.generated_at_ms || Date.now(),
          server_boot_id: errDash.server_boot_id || 0,
        });
      }
    } catch (err) {
      if (err && err.name === 'AbortError') return;
      state.consecutiveRefreshFailures += 1;
      console.warn('Dashboard refresh warning:', err);
      if (state.consecutiveRefreshFailures >= 2) {
        hooks.onOutageState({
          degraded: true,
          database_reachable: false,
          degraded_sections: ['connectivity'],
          degraded_errors: [`Dashboard endpoint unreachable (${err.message || 'network error'})`],
          generated_at_ms: Date.now(),
        });
      }
    } finally {
      if (state.refreshAbortController === controller) {
        state.refreshAbortController = null;
      }
      if (state.refreshInFlight === task) {
        state.refreshInFlight = null;
      }
    }
  })();

  state.refreshInFlight = task;
  return task;
}

export function connectSSE() {
  if (state.authRequired) return;
  disconnectSSE();
  try {
    const es = new EventSource('/api/v1/stream');
    state.sse = es;
    es.onmessage = (evt) => {
      try {
        const data = JSON.parse(evt.data);
        if (data && (data.snapshot || data.v_assets_held || data.vault_info || data.degraded)) {
          hooks.onApplyDashboard(data, false);
        }
      } catch (_) {}
    };
    es.addEventListener('auth_expired', () => {
      state.authRequired = true;
      disconnectSSE();
      hooks.onAuthRequired('Session expired or revoked — please re-authenticate.');
    });
    es.onerror = async () => {
      if (state.sse === es && es.readyState === EventSource.CLOSED && !state.authRequired) {
        try {
          await refreshAll(false);
        } catch (_) {}
      }
    };
  } catch (_) {}
}

export async function switchRiskTier(tier) {
  try {
    const res = await apiFetch('/api/v1/risk-tier', {
      method: 'POST',
      body: JSON.stringify({ risk_tier: tier }),
    });
    const data = await res.json();
    if (!res.ok) {
      hooks.onToast(`Risk tier update failed: ${data.detail || data.error || res.status}`, true);
      return;
    }
    hooks.onRiskTierButtons(data.risk_tier || tier);
    hooks.onToast(
      `Switched active Risk Tier -> ${(data.risk_tier || tier).toUpperCase()} (${Number(
        data.target_leverage_multiplier || 1.85
      ).toFixed(2)}x)`
    );
    await refreshAll(true);
  } catch (err) {
    hooks.onToast(`Failed to switch Risk Tier: ${err.message}`, true);
  }
}

export async function triggerTelegramTestAlert() {
  const btn = getEl('btn-telegram-test');
  if (btn) btn.disabled = true;
  try {
    hooks.onToast('Dispatching formatted HTML Telegram test alert...');
    const res = await apiFetch('/api/v1/alerts/test', {
      method: 'POST',
      body: JSON.stringify({}),
    });
    const data = await res.json();
    if (!res.ok || data.delivered === false) {
      hooks.onToast(
        `Telegram alert status: ${data.detail || data.error || 'Check bot credentials'}`,
        true
      );
    } else {
      hooks.onToast(
        `Telegram HTML alert delivered (${data.provider || 'telegram'} · HTTP ${
          data.http_status || 200
        })`
      );
    }
    await refreshAll(false);
  } catch (err) {
    hooks.onToast(`Telegram test request error: ${err.message}`, true);
  } finally {
    if (btn) btn.disabled = false;
  }
}

export async function triggerHyperliquidReconcile() {
  const btn = getEl('btn-sync-hyperliquid');
  if (btn) btn.disabled = true;
  try {
    hooks.onToast('Synchronizing MariaDB 1:1 with Hyperliquid clearinghouseState & userFills...');
    const res = await apiFetch('/api/v1/reconcile', {
      method: 'POST',
      body: JSON.stringify({ close_legacy_positions: false }),
    });
    const data = await res.json();
    if (!res.ok) {
      hooks.onToast(`Reconcile error: ${data.detail || data.error || res.status}`, true);
      await refreshAll(true);
      return;
    }
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
    hooks.onToast(
      `Synced 1:1 with Hyperliquid (Equity: ${fmtUsd(eq)} · Open: ${Number(
        r.legacy_positions_found || 0
      )} · Closed Trades: ${closedYtd})`
    );
  } catch (err) {
    hooks.onToast(`Hyperliquid sync error: ${err.message}`, true);
  } finally {
    if (btn) btn.disabled = false;
  }
}

export async function closePositionOrOrder(positionId, symbol, applyCooldown = true) {
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
      hooks.onToast(`Close failed: ${data.detail || data.error || res.status}`, true);
      return;
    }
    if (data.new_status === 'OPEN') {
      hooks.onToast(
        `Close order for ${data.symbol || sym} (#${data.position_id || pid}) did not fill; position remains OPEN`,
        true
      );
      return;
    }
    hooks.onToast(
      `Closed ${data.symbol || sym} (#${data.position_id || pid}) -> ${
        data.new_status || 'CLOSED'
      }`
    );
  } catch (err) {
    hooks.onToast(`Error closing position: ${err.message}`, true);
  } finally {
    state.closeInFlightCount = Math.max(0, state.closeInFlightCount - 1);
    if (pid > 0) state.closingPositionIds.delete(pid);
    if (sym) state.closingSymbols.delete(sym);
    invalidateSection('held');
    invalidateSection('priority');
    await refreshAll(true);
  }
}

export async function closeAllOpenPositions() {
  if (!window.confirm('Close ALL open positions and cancel ALL working orders?')) {
    return;
  }
  const btn = getEl('btn-close-all-open');
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Closing All...';
  }
  state.closeInFlightCount += 1;
  try {
    hooks.onToast('Closing all open positions & cancelling working orders...');
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
      hooks.onToast(`Close All failed: ${data.detail || data.error || res.status}`, true);
      return;
    }
    hooks.onToast(
      `Closed ${Number(data.closed_count ?? 0)} position(s)/order(s) (Realized PnL: ${fmtSignedUsd(
        data.realized_pnl_usdc ?? 0
      )})`
    );
  } catch (err) {
    hooks.onToast(`Error closing all positions: ${err.message}`, true);
  } finally {
    state.closeInFlightCount = Math.max(0, state.closeInFlightCount - 1);
    if (btn) {
      btn.disabled = false;
      btn.textContent = 'Close All';
    }
    invalidateSection('held');
    invalidateSection('priority');
    await refreshAll(true);
  }
}

export async function saveConfigKeyValue(key, val) {
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
      hooks.onToast(`Config update failed: ${data.detail || data.error || res.status}`, true);
      return;
    }
    hooks.onToast(`Saved config '${key}'`);
    invalidateSection('config-tab');
    invalidateSection('config-modal');
    await refreshAll(true);
  } catch (err) {
    hooks.onToast(`Config update error: ${err.message}`, true);
  }
}
