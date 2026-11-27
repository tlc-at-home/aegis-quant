/* ============================================================================
   AEGIS QUANT v2.0 — MODALS (AUTH, VAULT <dialog>, TOAST, ALERT BANNERS)
   ============================================================================ */

import { state, getEl } from '/js/state.js';
import {
  OFFICIAL_HLP_MAINNET,
  OFFICIAL_HLP_TESTNET,
  isOfficialHlpAddress,
  toFiniteNumber,
  fmtUsd,
  fmtCompactUsd,
  fmtSignedUsd,
  fmtSignedPct,
  pnlToneClass,
  fmtLocalTime,
  computeVaultPnlPct,
  getConfigFloatFromDash,
  getConfigNonNegFloatFromDash,
  getConfigValueFromDash,
  h,
  populatePnlElement,
  createAddressChip,
  createStatusPillEl,
} from '/js/format.js';
import { apiFetch, refreshAll, connectSSE } from '/js/api.js';

export function showToast(msg, isError = false) {
  const el = getEl('toast-banner');
  if (!el) return;
  el.textContent = msg;
  el.classList.toggle('toast-error', Boolean(isError));
  el.classList.add('visible');
  clearTimeout(window.__aegisToastTimer);
  window.__aegisToastTimer = setTimeout(() => {
    el.classList.remove('visible');
  }, 4200);
}

export function openAuthModal(errMsg = '') {
  const modal = getEl('auth-login-modal');
  const errEl = getEl('auth-login-error');
  const inp = getEl('auth-token-input');
  if (errEl) {
    if (errMsg) {
      errEl.textContent = errMsg;
      errEl.hidden = false;
    } else {
      errEl.textContent = '';
      errEl.hidden = true;
    }
  }
  if (modal && !modal.classList.contains('open')) {
    modal.classList.add('open');
    if (inp) setTimeout(() => inp.focus(), 40);
  }
}

export function closeAuthModal() {
  const modal = getEl('auth-login-modal');
  const errEl = getEl('auth-login-error');
  const inp = getEl('auth-token-input');
  if (modal) modal.classList.remove('open');
  if (errEl) {
    errEl.textContent = '';
    errEl.hidden = true;
  }
  if (inp) inp.value = '';
}

export async function submitAuthLogin(evt) {
  evt.preventDefault();
  const inp = getEl('auth-token-input');
  const btn = getEl('btn-auth-login-submit');
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

const heldFirstObservedAtMs = new Map();
let hasObservedInitialHeldSnapshot = false;

export function renderOutageAndSurveillanceBanners(dash, health) {
  const dbBanner = getEl('db-outage-banner');
  const survBanner = getEl('surveillance-alert-banner');

  const outageSource = state.degradedError || (dash?.degraded || dash?.database_reachable === false ? dash : null);
  const healthDown = health && health.status === 'unavailable';

  if (dbBanner) {
    if (outageSource || healthDown) {
      const dbOffline =
        outageSource?.database_reachable === false || Boolean(healthDown);
      const sections = Array.isArray(outageSource?.degraded_sections)
        ? outageSource.degraded_sections
        : [];
      const errors = Array.isArray(outageSource?.degraded_errors)
        ? outageSource.degraded_errors
        : health?.detail
        ? [health.detail]
        : [];
      const title = dbOffline
        ? '⚠ MARIADB OUTAGE DETECTED — LIVE POSITIONS & BALANCES MAY BE STALE'
        : `⚠ PARTIAL TELEMETRY DEGRADATION (${sections.join(', ') || 'secondary views'})`;
      const detailText =
        errors.length > 0
          ? errors.join(' | ')
          : 'Database query failed; check server logs.';
      const cidText = outageSource?.correlation_id
        ? `Ref: ${outageSource.correlation_id}`
        : health?.correlation_id
        ? `Ref: ${health.correlation_id}`
        : '';

      dbBanner.className = dbOffline
        ? 'alert-banner alert-banner-critical'
        : 'alert-banner alert-banner-warn';
      dbBanner.replaceChildren(
        h(
          'div',
          null,
          h('div', { className: 'alert-banner-title', text: title }),
          h('div', { className: 'alert-banner-meta', text: detailText })
        ),
        cidText ? h('span', { className: 'alert-banner-meta', text: cidText }) : null
      );
      dbBanner.hidden = false;
    } else {
      dbBanner.replaceChildren();
      dbBanner.hidden = true;
    }
  }

  if (survBanner) {
    const activeDash = dash && Array.isArray(dash.v_assets_held) ? dash : state.dashboard;
    const pm = activeDash?.priority_monitor_health || {};
    const pmStatus = String(pm.status || 'IDLE').toUpperCase();
    const breaches = Array.isArray(pm.unfilled_stop_breach_symbols)
      ? pm.unfilled_stop_breach_symbols
      : [];
    const failedSyms = Array.isArray(pm.failed_symbols) ? pm.failed_symbols : [];

    const execMode = String(activeDash?.execution_mode || '').toUpperCase();
    const isLiveOrTestnet = execMode === 'LIVE' || execMode === 'TESTNET';
    const nowTsMs = toFiniteNumber(activeDash?.timestamp_ms) ?? Date.now();
    const heldList = Array.isArray(activeDash?.v_assets_held) ? activeDash.v_assets_held : [];
    const currentHeldKeys = new Set();
    for (const heldItem of heldList) {
      const posKey = String(heldItem?.position_id ?? heldItem?.symbol ?? '');
      if (!posKey) continue;
      currentHeldKeys.add(posKey);
      if (!heldFirstObservedAtMs.has(posKey)) {
        const originTs =
          toFiniteNumber(heldItem?.opened_at_ms ?? heldItem?.created_at_ms) ?? 0;
        heldFirstObservedAtMs.set(
          posKey,
          hasObservedInitialHeldSnapshot ? Math.max(originTs, nowTsMs) : originTs
        );
      }
    }
    for (const oldKey of heldFirstObservedAtMs.keys()) {
      if (!currentHeldKeys.has(oldKey)) heldFirstObservedAtMs.delete(oldKey);
    }
    if (Array.isArray(activeDash?.v_assets_held)) {
      hasObservedInitialHeldSnapshot = true;
    }

    const missingSlSyms = isLiveOrTestnet
      ? heldList
          .filter((heldItem) => {
            const slOid = heldItem?.protective_stop_oid
              ? String(heldItem.protective_stop_oid).trim()
              : '';
            const originTs =
              toFiniteNumber(heldItem?.opened_at_ms ?? heldItem?.created_at_ms) ?? 0;
            const posKey = String(heldItem?.position_id ?? heldItem?.symbol ?? '');
            const graceStartTs = heldFirstObservedAtMs.get(posKey) ?? originTs;
            return !slOid && graceStartTs > 0 && nowTsMs - graceStartTs > 15_000;
          })
          .map((heldItem) => String(heldItem.symbol || heldItem.coin || ''))
      : [];

    const isCritical =
      pmStatus === 'CRITICAL' ||
      pmStatus === 'UNMONITORED' ||
      Boolean(pm.unmonitored_alert_active) ||
      breaches.length > 0 ||
      missingSlSyms.length > 0;
    const isDegraded =
      pmStatus === 'DEGRADED' || Boolean(pm.is_stale) || failedSyms.length > 0;

    if (isCritical || isDegraded) {
      const reasons = [];
      if (pmStatus === 'CRITICAL' || pmStatus === 'UNMONITORED' || pm.unmonitored_alert_active) {
        reasons.push(
          `1s PriorityMonitor ${pmStatus} (${Number(pm.consecutive_error_ticks || 0)} consecutive error ticks)`
        );
      } else if (isDegraded) {
        reasons.push(
          `1s PriorityMonitor ${pmStatus}${pm.is_stale ? ' (STALE HEARTBEAT)' : ''}`
        );
      }
      if (breaches.length > 0) {
        reasons.push(`Unfilled stop-breach resting: ${breaches.join(', ')}`);
      }
      if (failedSyms.length > 0) {
        reasons.push(`Failing symbols: ${failedSyms.join(', ')}`);
      }
      if (missingSlSyms.length > 0) {
        reasons.push(`Missing exchange SL OID (>15s): ${missingSlSyms.join(', ')}`);
      }

      survBanner.className = isCritical
        ? 'alert-banner alert-banner-critical'
        : 'alert-banner alert-banner-warn';
      survBanner.replaceChildren(
        h(
          'div',
          null,
          h('div', {
            className: 'alert-banner-title',
            text: isCritical
              ? '⚠ 1s SURVEILLANCE CRITICAL ALERT — PROTECTIVE STOP ATTENTION REQUIRED'
              : '⚠ 1s SURVEILLANCE DEGRADED — MONITORING PARTIALLY IMPAIRED',
          }),
          h('div', { className: 'alert-banner-meta', text: reasons.join(' · ') })
        )
      );
      survBanner.hidden = false;
    } else {
      survBanner.replaceChildren();
      survBanner.hidden = true;
    }
  }
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

  const getAnnouncer = () => getEl('vault-form-announcer');

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

    const addrInput = getEl('vault-address-input');
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

export function updateVaultNotesCharCount() {
  const notesInput = getEl('vault-notes-input');
  const countEl = getEl('vault-notes-char-count');
  if (notesInput && countEl) {
    countEl.textContent = String((notesInput.value || '').length);
  }
}

export function setVaultFormStatus(message, tone = 'info') {
  const el = getEl('vault-form-status');
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

export function getOfficialHlpForCurrentMode(dash = state.dashboard) {
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
  const pill = getEl('vault-modal-network-pill');
  const hlpBtn = getEl('btn-vault-use-hlp');
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
  const form = getEl('vault-manager-form');
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

  const addrInput = getEl('vault-address-input');
  if (addrInput) addrInput.value = cfgAddr;

  const roleRadio = form.querySelector(`input[name="vault_role"][value="${cfgRole}"]`);
  if (roleRadio) roleRadio.checked = true;

  const allocInput = getEl('vault-target-alloc-input');
  if (allocInput) allocInput.value = String(cfgTargetAlloc);

  const sweepInput = getEl('vault-sweep-threshold-input');
  if (sweepInput) sweepInput.value = String(cfgSweepThresh);

  const notesInput = getEl('vault-notes-input');
  if (notesInput) notesInput.value = cfgNotes;
  updateVaultNotesCharCount();
}

export function initVaultModalDialogAndForm() {
  const dialog = getEl('vault-manager-modal');
  const form = getEl('vault-manager-form');
  if (!dialog) return;

  if (dialog.dataset.dialogWired !== 'true') {
    dialog.dataset.dialogWired = 'true';

    dialog.addEventListener('close', () => {
      dialog.returnValue = '';
      state.vaultModal.isPreviewMode = false;
      state.vaultModal.previewVault = null;
      state.vaultModal.previewMeta = null;
    });

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

  const notesInput = getEl('vault-notes-input');
  if (notesInput && notesInput.dataset.countWired !== 'true') {
    notesInput.dataset.countWired = 'true';
    notesInput.addEventListener('input', updateVaultNotesCharCount);
  }

  const allocInput = getEl('vault-target-alloc-input');
  const sweepInput = getEl('vault-sweep-threshold-input');
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

export function openVaultModal(focusVaultAddress = null) {
  const dialog = getEl('vault-manager-modal');
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

export function refreshOpenVaultModalFromDashboard(dash) {
  const dialog = getEl('vault-manager-modal');
  if (!dialog || !dialog.open || !dash) return;
  renderVaultModalHeaderMeta(dash);
  renderVaultModalSyncedTabs(dash);
  renderVaultBarbellCard(dash);
  renderVaultReadinessCard(dash);
  if (!state.vaultModal.isPreviewMode) {
    renderVaultFactsDisplay(dash);
  }
}

export function renderVaultModalSyncedTabs(dash) {
  const container = getEl('vault-modal-synced-tabs');
  if (!container) return;

  const vaults = Array.isArray(dash?.vault_info?.vaults) ? dash.vault_info.vaults : [];
  const selectedNorm = String(state.vaultModal.selectedAddress || '').toLowerCase();
  const nodes = [];

  if (state.vaultModal.previewVault) {
    const pv = state.vaultModal.previewVault;
    nodes.push(
      h('button', {
        type: 'button',
        className: `vault-chip-btn preview${state.vaultModal.isPreviewMode ? ' active' : ''}`,
        text: `LIVE PREVIEW · ${String(pv.name || pv.vault_address || 'Inspected Vault')}`,
        dataset: { action: 'select-preview-vault' },
        attrs: { 'aria-pressed': state.vaultModal.isPreviewMode ? 'true' : 'false' },
      })
    );
  }

  for (const v of vaults) {
    const addr = String(v?.vault_address || '').trim();
    const addrNorm = addr.toLowerCase();
    const isActive = !state.vaultModal.isPreviewMode && addrNorm && addrNorm === selectedNorm;
    const eq = toFiniteNumber(v?.equity_usdc);
    nodes.push(
      h(
        'button',
        {
          type: 'button',
          className: `vault-chip-btn${isActive ? ' active' : ''}`,
          dataset: { action: 'select-synced-vault', vaultAddress: addr },
          attrs: { 'aria-pressed': isActive ? 'true' : 'false' },
        },
        h('span', { text: String(v?.name || 'Hyperliquid Vault') }),
        eq !== null && eq > 0.005
          ? h('span', { className: 'tab-count', text: fmtUsd(eq, 0) })
          : null
      )
    );
  }

  if (nodes.length === 0) {
    container.replaceChildren(
      h('span', {
        className: 'vault-empty-tabs-note',
        text: 'No synced wallet vaults yet — click "Use Official HLP" or inspect a 0x vault address.',
      })
    );
    return;
  }

  container.replaceChildren(...nodes);
}

function createVaultFactTile(labelText, valueText, subText = '', extraValueClass = '', valueTitle = '') {
  const val = h('div', {
    className: extraValueClass ? `vault-fact-value ${extraValueClass}` : 'vault-fact-value',
    text: valueText,
    title: valueTitle,
  });
  const tile = h(
    'div',
    { className: 'vault-fact-tile' },
    h('span', { className: 'vault-fact-label', text: labelText }),
    val,
    subText ? h('span', { className: 'vault-fact-sub', text: subText }) : null
  );
  return { tile, val };
}

export function renderVaultFactsDisplay(dash) {
  const sourcePill = getEl('vault-fact-source-pill');
  const nameEl = getEl('vault-fact-name');
  const pillsEl = getEl('vault-fact-status-pills');
  const addrsEl = getEl('vault-fact-addresses');
  const descEl = getEl('vault-fact-description');
  const gridEl = getEl('vault-facts-grid');
  if (!gridEl) return;

  const vaults = Array.isArray(dash?.vault_info?.vaults) ? dash.vault_info.vaults : [];
  let activeVault = null;
  let sourceText = 'AWAITING SELECTION';
  let sourceCls = 'status-pill watch';

  if (state.vaultModal.isPreviewMode && state.vaultModal.previewVault) {
    activeVault = state.vaultModal.previewVault;
    const isTestnet = Boolean(state.vaultModal.previewMeta?.is_testnet);
    const inspectedAt = toFiniteNumber(state.vaultModal.previewMeta?.inspected_at_ms);
    const tsLabel = fmtLocalTime(inspectedAt, '');
    const tsStr = tsLabel ? ` · ${tsLabel}` : '';
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
    const tsLabel = fmtLocalTime(syncedAt, '');
    const tsStr = tsLabel ? ` · ${tsLabel}` : '';
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
    gridEl.replaceChildren(
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
      createVaultFactTile('Max Withdrawable / Vol', '—', 'All-Time Vol: —').tile
    );
    return;
  }

  const v = activeVault;
  const isHlp = isOfficialHlpAddress(v.vault_address);

  if (nameEl) nameEl.textContent = String(v.name || 'Hyperliquid Vault');

  if (pillsEl) {
    const pills = [
      v.is_closed ? createStatusPillEl('closed', 'CLOSED') : createStatusPillEl('open', 'OPEN'),
    ];
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
    if (isHlp) pills.push(createStatusPillEl('watch', 'OFFICIAL HLP'));
    pillsEl.replaceChildren(...pills);
  }

  if (addrsEl) {
    addrsEl.replaceChildren(
      h(
        'span',
        { className: 'vault-addr-pair' },
        h('span', { className: 'vault-addr-label', text: 'Vault:' }),
        createAddressChip(v.vault_address)
      ),
      h(
        'span',
        { className: 'vault-addr-pair' },
        h('span', { className: 'vault-addr-label', text: 'Leader:' }),
        createAddressChip(v.leader)
      )
    );
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
  const lockedUntilLabel = fmtLocalTime(v.locked_until_ms, '');
  const tileLockup = createVaultFactTile(
    'Lockup Status',
    isLocked ? 'LOCKED' : 'UNLOCKED',
    isLocked
      ? lockedUntilLabel
        ? `Until ${lockedUntilLabel}`
        : 'Deposit lock active'
      : lockedUntilLabel
      ? `Unlocked since ${lockedUntilLabel}`
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
  const form = getEl('vault-manager-form');
  const allocInput = getEl('vault-target-alloc-input');
  const sweepInput = getEl('vault-sweep-threshold-input');

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

function createLegendSwatchItem(swatchClass, labelText, valText) {
  return h(
    'span',
    { className: 'legend-item' },
    h('span', { className: `legend-swatch ${swatchClass}` }),
    h('span', { text: labelText }),
    h('strong', { className: 'legend-val', text: valText })
  );
}

export function renderVaultBarbellCard(dash) {
  const rolePill = getEl('vault-barbell-role-pill');
  const barEl = getEl('vault-barbell-bar');
  const segPerp = getEl('vault-bar-seg-perp');
  const segSpot = getEl('vault-bar-seg-spot');
  const segVault = getEl('vault-bar-seg-vault');
  const legendEl = getEl('vault-barbell-legend');
  const metricsEl = getEl('vault-barbell-metrics');
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

  const wPerp = Math.max(0, Math.min(100, perpPct));
  const wSpot = Math.max(0, Math.min(100 - wPerp, spotPct));
  const wVault = Math.max(0, Math.min(100 - wPerp - wSpot, vaultPct));
  if (segPerp) {
    segPerp.setAttribute('x', '0');
    segPerp.setAttribute('width', wPerp.toFixed(2));
  }
  if (segSpot) {
    segSpot.setAttribute('x', wPerp.toFixed(2));
    segSpot.setAttribute('width', wSpot.toFixed(2));
  }
  if (segVault) {
    segVault.setAttribute('x', (wPerp + wSpot).toFixed(2));
    segVault.setAttribute('width', wVault.toFixed(2));
  }

  const perpLabel = isUnified ? 'Unified' : 'Perps';
  barEl.setAttribute(
    'aria-label',
    `Portfolio allocation: ${perpLabel} ${perpPct.toFixed(1)}%${
      !isUnified ? `, Spot ${spotPct.toFixed(1)}%` : ''
    }, Vault ${vaultPct.toFixed(1)}%`
  );

  const legendItems = [
    createLegendSwatchItem('swatch-cyan', `${perpLabel}:`, `${fmtUsd(perpEq, 2)} (${perpPct.toFixed(1)}%)`),
  ];
  if (!isUnified) {
    legendItems.push(
      createLegendSwatchItem('swatch-violet', 'Spot:', `${fmtUsd(spotEq, 2)} (${spotPct.toFixed(1)}%)`)
    );
  }
  legendItems.push(
    createLegendSwatchItem('swatch-emerald', 'Vault:', `${fmtUsd(vaultEq, 2)} (${vaultPct.toFixed(1)}%)`)
  );
  legendEl.replaceChildren(...legendItems);

  const targetSub = h('span', { className: 'vault-metric-sub' });
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
  const targetBox = h(
    'div',
    { className: 'vault-metric-box' },
    h('span', { className: 'vault-metric-label', text: 'Actual vs Target Vault Share' }),
    h('div', {
      className: 'vault-metric-val',
      text: `${vaultPct.toFixed(1)}% actual · ${targetPct.toFixed(1)}% target`,
    }),
    targetSub
  );

  const realizedYtd = toFiniteNumber(dash?.pnl_summary?.ytd?.realized_pnl_usdc) ?? 0;
  const realizedMonthly = toFiniteNumber(dash?.pnl_summary?.monthly?.realized_pnl_usdc) ?? 0;

  const sweepSub = h('span', { className: 'vault-metric-sub' });
  if (sweepThreshold <= 0.001) {
    sweepSub.textContent = 'Profit-sweep reminder disabled ($0 threshold)';
  } else if (realizedYtd >= sweepThreshold) {
    sweepSub.classList.add('pnl-pos');
    sweepSub.textContent = `SWEEP READY · Exceeds ${fmtUsd(sweepThreshold, 2)} threshold`;
  } else {
    const remaining = Math.max(0, sweepThreshold - realizedYtd);
    sweepSub.textContent = `${fmtUsd(remaining, 2)} to reach ${fmtUsd(sweepThreshold, 2)} sweep trigger`;
  }
  const sweepBox = h(
    'div',
    { className: 'vault-metric-box' },
    h('span', { className: 'vault-metric-label', text: 'Realized PnL vs Sweep Threshold' }),
    h(
      'div',
      { className: 'vault-metric-val' },
      'YTD ',
      h('span', {
        className: `pnl-inline-val ${pnlToneClass(realizedYtd)}`,
        text: fmtSignedUsd(realizedYtd, 2),
      }),
      ' (30D ',
      h('span', {
        className: `pnl-inline-val ${pnlToneClass(realizedMonthly)}`,
        text: fmtSignedUsd(realizedMonthly, 2),
      }),
      ')'
    ),
    sweepSub
  );

  metricsEl.replaceChildren(targetBox, sweepBox);
}

function createReadinessItem(nameText, badgeCls, badgeText, titleText = '') {
  return h(
    'div',
    { className: 'vault-readiness-item', title: titleText },
    h('span', { className: 'vault-readiness-name', text: nameText }),
    createStatusPillEl(badgeCls, badgeText)
  );
}

export function renderVaultReadinessCard(dash) {
  const container = getEl('vault-readiness-badges');
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
    'Protective Stop-Limit and Take-Profit Limit trigger orders are enforced on Hyperliquid.'
  );

  // Forensic fix: profit_gate_signal_exits defaults to 'false' (Asymmetric Exits ON = healthy emerald)
  const pgEnabled =
    getConfigValueFromDash(dash, 'profit_gate_signal_exits', 'false')
      .trim()
      .toLowerCase() === 'true';
  const pgMinPct =
    getConfigNonNegFloatFromDash(dash, 'signal_exit_min_net_profit_pct', 0.0025) * 100.0;
  const pgItem = createReadinessItem(
    'Profit-Gated Exits',
    pgEnabled ? 'working' : 'open',
    pgEnabled ? `ON (≥${pgMinPct.toFixed(2)}%)` : 'OFF (Asymmetric Exits)',
    'When OFF (recommended), asymmetric exits cut losing positions early instead of holding losers below the profit floor.'
  );

  const dd = dash?.drawdown_breaker || {};
  const maxDdPct =
    toFiniteNumber(dd.max_daily_drawdown_pct) ??
    getConfigFloatFromDash(dash, 'max_daily_drawdown_pct', 0.15) * 100.0;
  const ddTripped = Boolean(dd.tripped);
  const ruinTripped = Boolean(dd.ruin_halt_tripped);
  const ddItem = createReadinessItem(
    'Daily Drawdown Breaker',
    ddTripped || ruinTripped ? 'closed' : 'open',
    ruinTripped
      ? 'RUIN HALT'
      : ddTripped
      ? `HALTED (-${Math.abs(Number(dd.drawdown_pct ?? 0)).toFixed(1)}%)`
      : `ARMED (${maxDdPct.toFixed(1)}% · ${dd.basis === 'PAPER_EQUITY' ? 'Paper' : 'Uncapped'})`,
    '24-hour peak-to-trough uncapped perp equity drawdown & ruin floor circuit breaker (vault deposits excluded).'
  );

  container.replaceChildren(keyItem, tpslItem, pgItem, ddItem);
}

export async function inspectVaultAddress(rawAddress) {
  if (state.vaultModal.inFlightInspect) return;
  const addr = String(rawAddress || '').trim();
  const addrInput = getEl('vault-address-input');

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

  const inspectBtn = getEl('btn-vault-inspect');
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

export function triggerVaultInspectFromInput() {
  const addrInput = getEl('vault-address-input');
  if (!addrInput) return;
  addrInput.value = addrInput.value.trim();
  inspectVaultAddress(addrInput.value);
}

export function applyOfficialHlpPreset() {
  const addrInput = getEl('vault-address-input');
  if (!addrInput) return;
  const info = getOfficialHlpForCurrentMode(state.dashboard);
  addrInput.value = info.address;
  addrInput.setCustomValidity('');
  VaultFormValidation.syncAria(addrInput, false);
  inspectVaultAddress(info.address);
}

export function clearVaultAddressInput() {
  const addrInput = getEl('vault-address-input');
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

export async function triggerVaultLiveSync() {
  if (state.vaultModal.inFlightSync) return;
  const btn = getEl('btn-vault-refresh-sync');
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
    const tsLabel = fmtLocalTime(data?.synced_at_ms || Date.now(), '');
    const msg =
      data.synced === false
        ? `Vault sync skipped in ${data.execution_mode || 'PAPER'} mode (no live wallet or configured vault).`
        : `Live Hyperliquid vault sync completed (${syncedVaultCount} ${
            syncedVaultCount === 1 ? 'vault' : 'vaults'
          }${tsLabel ? ` · ${tsLabel}` : ''}).`;
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

  const addrInput = getEl('vault-address-input');
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
  const allocInput = getEl('vault-target-alloc-input');
  const sweepInput = getEl('vault-sweep-threshold-input');
  const notesInput = getEl('vault-notes-input');

  const allocNum = toFiniteNumber(allocInput?.value);
  const sweepNum = toFiniteNumber(sweepInput?.value);
  const notesVal = String(notesInput?.value || '').trim().slice(0, 500);

  const submitBtn = getEl('btn-vault-save-sync');
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
