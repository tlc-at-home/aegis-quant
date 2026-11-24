/* ============================================================================
   AEGIS QUANT v2.0 — ENTRYPOINT, KPI/HEADER RENDERERS & EVENT ORCHESTRATION
   ============================================================================ */

import {
  state,
  ROW_SORT_DATA,
  SUPPORTS_INVOKER_COMMANDS,
  initDomRegistry,
  getEl,
  computeFingerprint,
  isSectionDirty,
  invalidateSection,
  resetAllFingerprints,
  syncTableSortHeaders,
  reorderTableRows,
} from '/js/state.js';
import {
  toFiniteNumber,
  fmtUsd,
  fmtSignedUsd,
  fmtSignedPct,
  pnlToneClass,
  fmtLocalTime,
  getConfigFloatFromDash,
  createWalletStripItem,
  compareSortValues,
  h,
} from '/js/format.js';
import {
  configureApiHooks,
  refreshAll,
  connectSSE,
  switchRiskTier,
  triggerTelegramTestAlert,
  triggerHyperliquidReconcile,
  closePositionOrOrder,
  closeAllOpenPositions,
} from '/js/api.js';
import {
  renderVaultPanel,
  renderPriorityAndWorkingTable,
  renderOpenHoldingsTable,
  renderClosedPositionsTable,
  renderCooldownsPanel,
  renderExplorerTabs,
} from '/js/tables.js';
import {
  drawEquityCurveCanvas,
  drawSleeveAttributionCanvas,
} from '/js/charts.js';
import {
  setConfigCategory,
  openConfigModal,
  closeConfigModal,
  submitConfigUpdate,
  saveSingleConfigRow,
  renderSystemConfigTables,
} from '/js/config.js';
import {
  showToast,
  openAuthModal,
  submitAuthLogin,
  renderOutageAndSurveillanceBanners,
  initVaultModalDialogAndForm,
  openVaultModal,
  refreshOpenVaultModalFromDashboard,
  renderVaultModalSyncedTabs,
  renderVaultFactsDisplay,
  triggerVaultInspectFromInput,
  applyOfficialHlpPreset,
  clearVaultAddressInput,
  triggerVaultLiveSync,
} from '/js/modals.js';

function updateRiskTierButtons(activeTier) {
  const norm = String(activeTier || 'extreme').toLowerCase();
  document.querySelectorAll('.tier-btn').forEach((btn) => {
    btn.classList.toggle('active', btn.getAttribute('data-tier') === norm);
  });
}

function renderHealthAndTopBar(health, dash) {
  const activeDash = dash && dash.snapshot ? dash : state.dashboard;
  const outage = state.degradedError || (dash?.degraded || dash?.database_reachable === false ? dash : null);
  const dbOffline = outage?.database_reachable === false || health?.status === 'unavailable';
  const dbDegraded = Boolean(outage?.degraded) && !dbOffline;

  const dbVer =
    (health && health.mariadb_version) ||
    (activeDash && activeDash.mariadb_version) ||
    '13.0.2-MariaDB';
  const dbText = getEl('mariadb-version-text');
  const dbBadge = getEl('badge-mariadb');
  const dbDot = getEl('mariadb-pulse-dot');

  if (dbBadge) {
    dbBadge.classList.toggle('is-offline', dbOffline);
    dbBadge.classList.toggle('is-degraded', dbDegraded);
  }
  if (dbDot) {
    dbDot.className = dbOffline
      ? 'pulse-dot rose'
      : dbDegraded
      ? 'pulse-dot amber'
      : 'pulse-dot cyan';
  }
  if (dbText) {
    dbText.textContent = dbOffline
      ? 'MariaDB · OFFLINE'
      : dbDegraded
      ? `${dbVer} · DEGRADED`
      : `${dbVer} · Online`;
  }

  const prioWatch = (activeDash && activeDash.v_priority_watchlist) || [];
  const heldAssets = (activeDash && activeDash.v_assets_held) || [];
  const heldIds = new Set(heldAssets.map((heldItem) => Number(heldItem.asset_id)));
  const heldInWatch = prioWatch.filter((p) => {
    const aid = Number(p.asset_id);
    const st = String(p.position_status || '').toUpperCase();
    return heldIds.has(aid) || st === 'OPEN';
  }).length;

  const prioEl = getEl('prio-watch-count');
  if (prioEl) {
    prioEl.textContent =
      heldInWatch > 0
        ? `${prioWatch.length} (${heldInWatch} Held)`
        : String(prioWatch.length);
  }

  // R14-H3: PriorityMonitor surveillance health pill
  const pm = activeDash?.priority_monitor_health || {};
  const pmStatus = String(pm.status || 'IDLE').toUpperCase();
  const prioBadge = getEl('badge-prio-pulse');
  const prioDot = getEl('prio-pulse-dot');
  const prioLabel = getEl('prio-watch-label');
  const isCritPrio =
    pmStatus === 'CRITICAL' ||
    pmStatus === 'UNMONITORED' ||
    Boolean(pm.unmonitored_alert_active) ||
    (Array.isArray(pm.unfilled_stop_breach_symbols) &&
      pm.unfilled_stop_breach_symbols.length > 0);
  const isDegPrio = !isCritPrio && (pmStatus === 'DEGRADED' || Boolean(pm.is_stale));

  if (prioBadge) {
    prioBadge.classList.toggle('is-critical', isCritPrio);
    prioBadge.classList.toggle('is-degraded', isDegPrio);
  }
  if (prioDot) {
    prioDot.className = isCritPrio
      ? 'pulse-dot rose'
      : isDegPrio
      ? 'pulse-dot amber'
      : 'pulse-dot emerald';
  }
  if (prioLabel) {
    const countNode = prioEl || h('strong', { id: 'prio-watch-count', text: String(prioWatch.length) });
    if (isCritPrio) {
      prioLabel.replaceChildren(document.createTextNode('SURVEILLANCE STALLED · '), countNode);
    } else if (isDegPrio) {
      const errCnt = Number(pm.consecutive_error_ticks || pm.last_tick_error_count || 0);
      prioLabel.replaceChildren(
        document.createTextNode(`DEGRADED (${errCnt} err) · `),
        countNode
      );
    } else {
      prioLabel.replaceChildren(document.createTextNode('1s PRIORITY WATCH: '), countNode);
    }
  }

  const exAssets = (activeDash && activeDash.exchange_assets) || [];
  const scanEl = getEl('scanner-asset-count');
  if (scanEl) scanEl.textContent = `${exAssets.length} PERPS`;

  const mode =
    (activeDash && activeDash.execution_mode) ||
    (health && health.execution_mode) ||
    'TESTNET';
  const modeBadge = getEl('badge-exec-mode');
  const modeText = getEl('exec-mode-text');
  if (modeText) modeText.textContent = mode.toUpperCase();
  if (modeBadge) {
    modeBadge.classList.toggle('mode-live', mode.toUpperCase() === 'LIVE');
    modeBadge.classList.toggle('mode-testnet', mode.toUpperCase() !== 'LIVE');
  }

  const tier =
    (activeDash && activeDash.risk_tier) ||
    (activeDash && activeDash.snapshot && activeDash.snapshot.risk_tier) ||
    (health && health.risk_tier) ||
    'extreme';
  updateRiskTierButtons(tier);
}

function renderPnlHorizonRibbon(dash, force = false) {
  const summary = (dash && dash.pnl_summary) || {};
  const fallbackUpnl = ((dash && dash.v_assets_held) || []).reduce(
    (acc, heldItem) => acc + (toFiniteNumber(heldItem.unrealized_pnl_usdc) ?? 0),
    0
  );
  const totalEq =
    toFiniteNumber(dash?.equity_basis?.effective_equity_usdc) ??
    toFiniteNumber(dash?.snapshot?.total_equity_usdc) ??
    0;
  const capEpochMs = Number(dash?.risk_capital_cap_updated_at_ms ?? 0);

  const fp = computeFingerprint([summary, fallbackUpnl.toFixed(2), totalEq.toFixed(2), capEpochMs]);
  if (!force && !isSectionDirty('pnl-ribbon', fp)) return;

  const paintHorizon = (key, defaultObj) => {
    const hz = summary[key] || defaultObj;
    const netPnl = toFiniteNumber(hz.net_pnl_usdc) ?? fallbackUpnl;
    const retPct =
      toFiniteNumber(hz.return_pct) ?? (totalEq > 0 ? (netPnl / totalEq) * 100.0 : 0.0);
    const rpnl = toFiniteNumber(hz.realized_pnl_usdc) ?? 0;
    const upnl = toFiniteNumber(hz.unrealized_pnl_usdc) ?? fallbackUpnl;
    const trades = Number(hz.trade_count ?? 0);
    const wins = Number(hz.win_count ?? 0);
    const winStartMs = Number(hz.window_start_ms ?? 0);
    const isClamped =
      Boolean(hz.is_cap_clamped) ||
      (capEpochMs > 0 && winStartMs > 0 && capEpochMs > winStartMs);
    const clampTs = capEpochMs > 0 ? capEpochMs : winStartMs;

    const tone = pnlToneClass(netPnl).slice(4);

    const cardEl = getEl(`pnl-card-${key}`);
    const pctEl = getEl(`pnl-${key}-pct`);
    const usdcEl = getEl(`pnl-${key}-usdc`);
    const subEl = getEl(`pnl-${key}-sub`);
    const clampEl = getEl(`pnl-${key}-clamp`);
    const sourceEl = getEl(`pnl-${key}-source`);

    if (cardEl) {
      cardEl.classList.remove('pos', 'neg', 'flat');
      cardEl.classList.add(tone);
    }
    if (sourceEl) {
      if (String(hz.pnl_source || '').toUpperCase() === 'HL_PORTFOLIO') {
        sourceEl.textContent = 'HL Perps';
        sourceEl.setAttribute('aria-label', 'Hyperliquid Perps exchange-reconciled PnL');
        sourceEl.hidden = false;
      } else {
        sourceEl.textContent = '';
        sourceEl.removeAttribute('aria-label');
        sourceEl.hidden = true;
      }
    }
    if (clampEl) {
      if (isClamped && clampTs > 0) {
        const dtLabel = fmtLocalTime(clampTs, '');
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
      pctEl.textContent = fmtSignedPct(retPct, 2) ?? '0.00%';
    }
    if (usdcEl) {
      usdcEl.classList.remove('pos', 'neg', 'flat');
      usdcEl.classList.add(tone);
      usdcEl.textContent = fmtSignedUsd(netPnl);
    }
    if (subEl) {
      const tradeStr =
        trades > 0 ? `${trades} Trades (${wins}W)` : '0 Closed · Live Mark';
      subEl.replaceChildren(
        h(
          'span',
          null,
          'Realized: ',
          h('span', {
            className: `pnl-inline-val ${pnlToneClass(rpnl)}`,
            text: fmtSignedUsd(rpnl),
          }),
          ' · Unrealized: ',
          h('span', {
            className: `pnl-inline-val ${pnlToneClass(upnl)}`,
            text: fmtSignedUsd(upnl),
          })
        ),
        h('span', { text: tradeStr })
      );
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

function renderKpiCards(dash, force = false) {
  const snap = (dash && dash.snapshot) || {};
  const eb = (dash && dash.equity_basis) || {};
  const dd = (dash && dash.drawdown_breaker) || {};
  const maxGrossCap = getConfigFloatFromDash(dash, 'max_gross_leverage_x', 2.5);

  const fp = computeFingerprint([
    snap,
    eb,
    dd,
    dash?.execution_mode,
    dash?.risk_tier,
    dash?.target_leverage_multiplier,
    dash?.vault_info?.total_vault_equity_usdc,
    dash?.attached_tpsl_order_count,
    dash?.exchange_open_orders_count,
    maxGrossCap,
  ]);
  if (!force && !isSectionDirty('kpi-cards', fp)) return;

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

  const setTxt = (id, text) => {
    const el = getEl(id);
    if (el) el.textContent = text;
  };
  const setProgress = (id, pct) => {
    const el = getEl(id);
    if (el) el.value = Math.max(0, Math.min(100, Number.isFinite(pct) ? pct : 0));
  };

  const isCapBinding = Boolean(eb.is_cap_binding);
  const isUnified = Boolean(eb.is_unified_account);
  const perpMarginEq = toFiniteNumber(eb.perp_margin_equity_usdc) ?? 0;
  setTxt(
    'kpi-equity-title',
    isCapBinding
      ? 'Risk-Sizing Cap vs Live Wallet'
      : isUnified
      ? 'Trading Equity (Unified)'
      : isLiveOrTestnet
      ? 'Trading Equity (Perp)'
      : 'Total Portfolio Equity'
  );
  setTxt('kpi-total-equity', fmtUsd(totalEq));

  const walletEl = getEl('kpi-wallet-summary');
  const syncEl = getEl('kpi-equity-sync-sub');
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
      const items = [];
      if (isCapBinding) {
        items.push(
          createWalletStripItem(
            'Uncapped Live Wallet:',
            fmtUsd(perpWalletEq),
            'kpi-wallet-val-uncapped'
          ),
          createWalletStripItem(
            'Risk-Sizing Cap:',
            fmtUsd(capUsdc),
            'kpi-wallet-val-cap'
          )
        );
      } else {
        items.push(
          createWalletStripItem(
            isUnified ? 'Unified Wallet:' : 'Perp Wallet:',
            fmtUsd(perpWalletEq)
          )
        );
      }
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
      const capSpan = h('span', {
        text: capUsdc >= 10 ? `Cap: ${fmtUsd(capUsdc)}` : 'Uncapped Perp',
      });
      const syncLabel = fmtLocalTime(syncedAtMs, '');
      const syncTimeStr = syncLabel ? `Synced ${syncLabel}` : 'Awaiting Sync';
      const syncSpan = h(
        'span',
        { className: 'kpi-sync-status' },
        syncTimeStr,
        isStale ? h('span', { className: 'sync-stale-pill', text: 'STALE' }) : null
      );
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
  setProgress(
    'kpi-equity-meter',
    isCapBinding && perpWalletEq > 0 ? (totalEq / perpWalletEq) * 100.0 : 100
  );

  const cashPct = totalEq > 0 ? (availCash / totalEq) * 100.0 : 0.0;
  setTxt('kpi-avail-cash', fmtUsd(availCash));
  setTxt('kpi-cash-pct', `${cashPct.toFixed(1)}% of Equity`);
  setProgress('kpi-cash-meter', cashPct);

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
  setProgress('kpi-reserved-meter', Math.min(100, resPct * 2));

  const commPct = totalEq > 0 ? (committedNotional / totalEq) * 100.0 : 0.0;
  setTxt('kpi-committed-notional', fmtUsd(committedNotional));
  setTxt(
    'kpi-open-counts',
    `${Number(snap.open_long_count ?? 0)} Long · ${Number(
      snap.open_short_count ?? 0
    )} Short`
  );
  setTxt('kpi-net-notional', `Net: ${fmtUsd(netNotional)}`);
  setProgress('kpi-committed-meter', Math.min(100, commPct));

  setTxt('kpi-gross-leverage', `${grossLev.toFixed(2)}x`);
  setTxt('kpi-net-leverage', `${netLev >= 0 ? '+' : ''}${netLev.toFixed(2)}x`);
  setTxt('kpi-leverage-target', `Target: ${mult.toFixed(2)}x ${tier}`);
  setTxt('kpi-leverage-cap', `Cap: ${maxGrossCap.toFixed(2)}x`);
  setProgress('kpi-leverage-meter', (grossLev / Math.max(0.2, maxGrossCap)) * 100.0);

  // Forensic fix: ALWAYS render live 24h drawdown pill (amber >= 60%, pulsing rose when tripped) + RUIN HALT
  const ddPill = getEl('kpi-drawdown-pill');
  if (ddPill) {
    const ddPctNum = Math.abs(Number(dd.drawdown_pct ?? 0));
    const ddCapNum = Math.max(1.0, Number(dd.max_daily_drawdown_pct ?? 15.0));
    const basisTag = dd.basis === 'PAPER_EQUITY' ? 'Paper' : 'Uncapped';
    const warnThreshold = ddCapNum * 0.6;
    ddPill.hidden = false;
    if (dd.tripped) {
      ddPill.className = 'dd-halt-pill is-tripped';
      ddPill.textContent = `DD HALT -${ddPctNum.toFixed(1)}% / ${ddCapNum.toFixed(1)}% · ${basisTag}`;
    } else if (ddPctNum >= warnThreshold) {
      ddPill.className = 'dd-halt-pill is-warn';
      ddPill.textContent = `24h DD: -${ddPctNum.toFixed(1)}% / ${ddCapNum.toFixed(1)}% · ${basisTag}`;
    } else {
      ddPill.className = 'dd-halt-pill is-ok';
      ddPill.textContent = `24h DD: -${ddPctNum.toFixed(1)}% / ${ddCapNum.toFixed(1)}% · ${basisTag}`;
    }
    ddPill.title = `24h Peak: ${fmtUsd(dd.peak_24h_equity_usdc ?? 0)} -> Latest: ${fmtUsd(dd.latest_equity_usdc ?? 0)}`;
  }

  const ruinPill = getEl('kpi-ruin-halt-pill');
  if (ruinPill) {
    const ruinTripped = Boolean(dd.ruin_halt_tripped);
    ruinPill.hidden = !ruinTripped;
    if (ruinTripped) {
      const floorUsd = toFiniteNumber(dd.min_equity_halt_usdc) ?? 50;
      ruinPill.textContent = `RUIN HALT (< ${fmtUsd(floorUsd, 0)})`;
    }
  }

  setTxt('kpi-regime-label', regime);
  setTxt('kpi-breadth-pct', `Bull Breadth: ${(breadth * 100.0).toFixed(1)}%`);
  setTxt('kpi-beta-exp', `Beta: ${beta >= 0 ? '+' : ''}${beta.toFixed(2)}`);
  setProgress('kpi-breadth-meter', breadth * 100.0);

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

function applyDashboardPayload(dash, forceConfig = false) {
  if (!dash) return;

  // R14-M24: Monotonic snapshot_seq + server_boot_id freshness guard
  const bootId = Number(dash.server_boot_id || 0);
  if (bootId > 0 && state.serverBootId > 0 && bootId !== state.serverBootId) {
    state.lastAppliedSeq = 0;
    state.lastAppliedAtMs = 0;
    resetAllFingerprints();
  }
  if (bootId > 0) {
    state.serverBootId = bootId;
  }

  const seq = Number(dash.snapshot_seq || 0);
  const genMs = Number(dash.generated_at_ms || dash.timestamp_ms || 0);
  if (seq > 0) {
    if (seq < state.lastAppliedSeq) return;
    if (seq === state.lastAppliedSeq && !forceConfig) return;
    state.lastAppliedSeq = seq;
  } else if (genMs > 0 && genMs < state.lastAppliedAtMs) {
    return;
  }
  if (genMs > state.lastAppliedAtMs) {
    state.lastAppliedAtMs = genMs;
  }

  // R14-M22: Degraded stub event (e.g. SSE DB outage) omits snapshot/v_assets_held so rows aren't wiped
  const isStubOutage =
    Boolean(dash.degraded) && !dash.snapshot && !Array.isArray(dash.v_assets_held);
  if (isStubOutage) {
    state.degradedError = dash;
    renderHealthAndTopBar(state.health, dash);
    renderOutageAndSurveillanceBanners(dash, state.health);
    renderPriorityAndWorkingTable(dash, true);
    renderOpenHoldingsTable(dash, true);
    return;
  }

  state.degradedError = dash.degraded ? dash : null;
  state.dashboard = dash;

  renderHealthAndTopBar(state.health, dash);
  renderOutageAndSurveillanceBanners(dash, state.health);
  renderPnlHorizonRibbon(dash, forceConfig);
  renderKpiCards(dash, forceConfig);
  renderVaultPanel(dash, forceConfig);
  renderPriorityAndWorkingTable(dash, forceConfig);
  renderOpenHoldingsTable(dash, forceConfig);
  renderClosedPositionsTable(dash, forceConfig);
  renderCooldownsPanel(dash, forceConfig);
  renderExplorerTabs(dash, forceConfig);
  if (state.activeTab === 'config' || getEl('config-settings-modal')?.classList.contains('open')) {
    renderSystemConfigTables(forceConfig);
  }
  drawEquityCurveCanvas(dash, forceConfig);
  drawSleeveAttributionCanvas(dash, forceConfig);

  const vaultDialog = getEl('vault-manager-modal');
  if (vaultDialog && vaultDialog.open) {
    refreshOpenVaultModalFromDashboard(dash);
  }
}

function rerenderTableById(tableId) {
  const dash = state.dashboard;
  if (!dash) return;
  switch (tableId) {
    case 'table-vaults':
      renderVaultPanel(dash, true);
      break;
    case 'table-priority-ordered':
      renderPriorityAndWorkingTable(dash, true);
      break;
    case 'table-assets-held':
      renderOpenHoldingsTable(dash, true);
      break;
    case 'table-closed-positions':
      renderClosedPositionsTable(dash, true);
      break;
    case 'table-cooldowns':
      renderCooldownsPanel(dash, true);
      break;
    case 'table-system-config':
    case 'table-modal-system-config':
      renderSystemConfigTables(true);
      break;
    default:
      renderExplorerTabs(dash, true);
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

  const statusEl = getEl('table-sort-status');
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

function switchExplorerTab(tabName) {
  state.activeTab = tabName;
  document.querySelectorAll('.tab-btn').forEach((btn) => {
    btn.classList.toggle('active', btn.getAttribute('data-tab') === tabName);
  });
  document.querySelectorAll('.tab-pane').forEach((pane) => {
    pane.classList.toggle('active', pane.id === `tab-pane-${tabName}`);
  });
  if (state.dashboard) {
    if (tabName === 'config') {
      invalidateSection('config-tab');
      renderSystemConfigTables(true);
    } else {
      invalidateSection(`tab-${tabName}`);
      if (tabName === 'console') {
        invalidateSection('tab-console-audit');
        invalidateSection('tab-console-logs');
      }
      renderExplorerTabs(state.dashboard, true);
    }
  }
}

configureApiHooks({
  onAuthRequired: (msg) => openAuthModal(msg || ''),
  onApplyDashboard: applyDashboardPayload,
  onToast: showToast,
  onHealthUpdate: (health) => {
    renderHealthAndTopBar(health, state.dashboard);
    renderOutageAndSurveillanceBanners(state.degradedError || state.dashboard, health);
  },
  onOutageState: (outagePayload) => {
    state.degradedError = outagePayload;
    renderHealthAndTopBar(state.health, outagePayload);
    renderOutageAndSurveillanceBanners(outagePayload, state.health);
    renderPriorityAndWorkingTable(outagePayload, true);
    renderOpenHoldingsTable(outagePayload, true);
  },
  onRiskTierButtons: updateRiskTierButtons,
});

window.addEventListener('resize', () => {
  if (state.dashboard) {
    drawEquityCurveCanvas(state.dashboard, true);
    drawSleeveAttributionCanvas(state.dashboard, true);
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
      const dlg = getEl('vault-manager-modal');
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
    state.showAllPriorityWatch = !state.showAllPriorityWatch;
    if (state.dashboard) {
      renderPriorityAndWorkingTable(state.dashboard, true);
    }
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
  initDomRegistry();
  initSortableTables();
  initVaultModalDialogAndForm();

  const scannerSearch = getEl('scanner-search-input');
  if (scannerSearch) {
    scannerSearch.addEventListener('input', () => {
      if (!state.dashboard) return;
      if (state.activeTab === 'config') {
        renderSystemConfigTables(true);
      } else {
        renderExplorerTabs(state.dashboard, true);
      }
    });
  }
  const closedSearch = getEl('closed-search-input');
  if (closedSearch) {
    closedSearch.addEventListener('input', () => {
      if (state.dashboard) renderClosedPositionsTable(state.dashboard, true);
    });
  }
  const modalSearch = getEl('modal-cfg-search');
  if (modalSearch) {
    modalSearch.addEventListener('input', () => renderSystemConfigTables(true));
  }
  const authForm = getEl('auth-login-form');
  if (authForm) {
    authForm.addEventListener('submit', submitAuthLogin);
  }

  const eqCanvas = getEl('equity-curve-canvas');
  if (eqCanvas) {
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
      if (state.equityHoverIdx !== nearestIdx) {
        state.equityHoverIdx = nearestIdx;
        if (state.dashboard) drawEquityCurveCanvas(state.dashboard);
      }
    });
    eqCanvas.addEventListener('mouseleave', () => {
      if (state.equityHoverIdx !== null) {
        state.equityHoverIdx = null;
        if (state.dashboard) drawEquityCurveCanvas(state.dashboard);
      }
    });
  }

  await refreshAll(true);
  connectSSE();

  setInterval(() => {
    if (state.authRequired) return;
    if (state.sse && state.sse.readyState === EventSource.OPEN) return;
    refreshAll(false);
  }, 2500);
});
