/* ============================================================================
   AEGIS QUANT v2.0 — RETINA HTML5 CANVAS CHARTS & CANVAS-DRAWN HOVER TOOLTIP
   ============================================================================ */

import {
  state,
  getEl,
  computeFingerprint,
  isSectionDirty,
} from '/js/state.js';
import {
  toFiniteNumber,
  fmtUsd,
  fmtLocalTime,
  fmtCompactDuration,
} from '/js/format.js';

function prepareCanvas(canvasId) {
  const canvas = getEl(canvasId);
  if (!canvas || !canvas.parentElement) return null;
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
  return { ctx, w, h, canvas };
}

function drawRoundedRectPath(ctx, x, y, w, h, r) {
  const rad = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + rad, y);
  ctx.arcTo(x + w, y, x + w, y + h, rad);
  ctx.arcTo(x + w, y + h, x, y + h, rad);
  ctx.arcTo(x, y + h, x, y, rad);
  ctx.arcTo(x, y, x + w, y, rad);
  ctx.closePath();
}

export function drawEquityCurveCanvas(dash, force = false) {
  const prep = prepareCanvas('equity-curve-canvas');
  if (!prep) return;
  const { ctx, w, h, canvas } = prep;

  const rawHistory = (dash && dash.snapshots_history) || [];
  let history = rawHistory.length > 0 ? rawHistory : dash && dash.snapshot ? [dash.snapshot] : [];
  const fallbackNow = toFiniteNumber(dash?.timestamp_ms) ?? Date.now();
  const isCapBinding = Boolean(dash?.equity_basis?.is_cap_binding);

  const firstPt = history[0] || {};
  const lastPt = history[history.length - 1] || {};
  const canvasFp = computeFingerprint([
    history.length,
    firstPt.created_at_ms,
    lastPt.created_at_ms,
    lastPt.total_equity_usdc,
    lastPt.uncapped_equity_usdc,
    lastPt.committed_notional_usdc,
    lastPt.reserved_order_cash_usdc,
    dash?.equity_basis?.effective_equity_usdc,
    dash?.equity_basis?.perp_wallet_equity_usdc,
    isCapBinding,
    state.equityHoverIdx,
    Math.round(w),
    Math.round(h),
  ]);
  if (!force && !isSectionDirty('chart-equity', canvasFp)) {
    return;
  }

  ctx.clearRect(0, 0, w, h);

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

  const latestSrc =
    points.length > 0 ? points[points.length - 1].pt : dash?.snapshot || {};
  const liveEqVal =
    toFiniteNumber(dash?.equity_basis?.effective_equity_usdc) ??
    toFiniteNumber(dash?.snapshot?.total_equity_usdc) ??
    toFiniteNumber(latestSrc?.total_equity_usdc) ??
    0;
  const liveUncappedVal =
    toFiniteNumber(dash?.equity_basis?.perp_wallet_equity_usdc) ??
    toFiniteNumber(latestSrc?.uncapped_equity_usdc) ??
    liveEqVal;
  const liveCommVal =
    toFiniteNumber(dash?.snapshot?.committed_notional_usdc) ??
    toFiniteNumber(latestSrc?.committed_notional_usdc) ??
    0;
  const liveResVal =
    toFiniteNumber(dash?.snapshot?.reserved_order_cash_usdc) ??
    toFiniteNumber(latestSrc?.reserved_order_cash_usdc) ??
    0;

  const setLegend = (id, v) => {
    const el = getEl(id);
    if (el) el.textContent = fmtUsd(v);
  };
  setLegend('legend-eq-val', liveEqVal);
  setLegend('legend-comm-val', liveCommVal);
  setLegend('legend-res-val', liveResVal);

  const showUncappedSeries =
    isCapBinding ||
    points.some(({ pt }) => {
      const u = toFiniteNumber(pt.uncapped_equity_usdc);
      const e = toFiniteNumber(pt.total_equity_usdc) ?? 0;
      return u !== null && u > e + 1.0;
    });

  const uncappedLegendItem = getEl('legend-uncapped-item');
  if (uncappedLegendItem) {
    uncappedLegendItem.hidden = !showUncappedSeries;
  }
  if (showUncappedSeries) {
    setLegend('legend-uncapped-val', liveUncappedVal);
  }

  const padL = 64;
  const padR = 18;
  const padT = 16;
  const padB = 26;
  const plotW = Math.max(10, w - padL - padR);
  const plotH = Math.max(10, h - padT - padB);

  const vals = [];
  points.forEach(({ pt }) => {
    const eq = Number(pt.total_equity_usdc ?? 0);
    const uncapped = toFiniteNumber(pt.uncapped_equity_usdc);
    vals.push(eq);
    if (showUncappedSeries && uncapped !== null && uncapped > 0) {
      vals.push(uncapped);
    }
    vals.push(Number(pt.committed_notional_usdc ?? 0));
    vals.push(Number(pt.reserved_order_cash_usdc ?? 0));
  });
  const minVal = 0;
  const rawMax = Math.max(0, ...vals);
  const maxVal = rawMax > 0 ? rawMax * 1.08 : 100;

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
    const uncappedRaw = toFiniteNumber(pt.uncapped_equity_usdc);
    const uncapped = uncappedRaw !== null && uncappedRaw > 0 ? uncappedRaw : eq;
    const comm = Number(pt.committed_notional_usdc ?? 0);
    const res = Number(pt.reserved_order_cash_usdc ?? 0);
    return {
      x: xForTime(item.ts, idx),
      y: yFor(eq),
      yUncapped: yFor(uncapped),
      uncappedVal: uncapped,
      yComm: yFor(comm),
      yRes: yFor(res),
      ts: item.ts,
      pt,
    };
  });

  state.equityChartPoints = coords;

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

  if (coords.length > 0) {
    ctx.font = '10px "JetBrains Mono", monospace';
    ctx.fillStyle = '#94a3b8';

    const startLabel = fmtLocalTime(tMin, '');
    const midLabel = spanMs > 0 ? fmtLocalTime(tMin + spanMs * 0.5, '') : '';
    const endLabel = fmtLocalTime(tMax, '');

    const startW = startLabel ? ctx.measureText(startLabel).width : 0;
    const midW = midLabel ? ctx.measureText(midLabel).width : 0;
    const endW = endLabel ? ctx.measureText(endLabel).width : 0;
    const minGapPx = 14;

    const tickPositions = [];
    if (startLabel) {
      tickPositions.push({ label: startLabel, x: padL, align: 'left' });
    }
    const canShowEnd =
      Boolean(endLabel) && (startW === 0 || startW + endW + minGapPx <= plotW);
    const canShowMid =
      spanMs > 0 &&
      canShowEnd &&
      Boolean(midLabel) &&
      startW + minGapPx <= plotW * 0.5 - midW / 2 &&
      plotW * 0.5 + midW / 2 + minGapPx <= plotW - endW;

    if (canShowMid) {
      tickPositions.push({ label: midLabel, x: padL + plotW * 0.5, align: 'center' });
    }
    if (canShowEnd) {
      tickPositions.push({ label: endLabel, x: padL + plotW, align: 'right' });
    }

    tickPositions.forEach((tk) => {
      ctx.textAlign = tk.align;
      ctx.fillText(tk.label, tk.x, padT + plotH + 17);
    });
    ctx.textAlign = 'left';
  }

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
  if (showUncappedSeries) {
    drawGapBridge('yUncapped', 'rgba(167, 139, 250, 0.5)');
  }

  const drawSeriesStroke = (yKey, strokeColor, lineWidth, dashPattern = null) => {
    ctx.save();
    ctx.strokeStyle = strokeColor;
    ctx.fillStyle = strokeColor;
    ctx.lineWidth = lineWidth;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    if (dashPattern) ctx.setLineDash(dashPattern);
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
    ctx.restore();
  };

  drawSeriesStroke('yComm', '#10b981', 2.0);
  drawSeriesStroke('yRes', '#f59e0b', 2.0);
  if (showUncappedSeries) {
    drawSeriesStroke('yUncapped', '#a78bfa', 1.8, [6, 4]);
  }
  drawSeriesStroke('y', '#00f2fe', 2.4);

  // Layer 5: Hover crosshair, markers, and canvas-rendered glass tooltip (zero inline DOM styles)
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
    if (showUncappedSeries) {
      drawHoverDot(hp.x, hp.yUncapped, '#a78bfa', 3.5);
    }
    drawHoverDot(hp.x, hp.y, '#00f2fe', 4.5);

    const pt = hp.pt;
    const tipLines = [
      { text: fmtLocalTime(hp.ts ?? pt.created_at_ms ?? pt.updated_at_ms), color: '#00f2fe', bold: true },
      { text: `Equity: ${fmtUsd(pt.total_equity_usdc ?? 0)}`, color: '#f8fafc', bold: true },
    ];
    if (showUncappedSeries) {
      tipLines.push({
        text: `Uncapped: ${fmtUsd(hp.uncappedVal)}`,
        color: '#c4b5fd',
        bold: false,
      });
    }
    tipLines.push(
      { text: `Committed: ${fmtUsd(pt.committed_notional_usdc ?? 0)}`, color: '#34d399', bold: false },
      { text: `Reserved: ${fmtUsd(pt.reserved_order_cash_usdc ?? 0)}`, color: '#fbbf24', bold: false }
    );

    ctx.save();
    ctx.font = '600 10.5px "JetBrains Mono", monospace';
    let maxTextW = 0;
    for (const ln of tipLines) {
      maxTextW = Math.max(maxTextW, ctx.measureText(ln.text).width);
    }
    const tipW = Math.ceil(maxTextW) + 20;
    const lineH = 15;
    const tipH = tipLines.length * lineH + 14;
    const preferRight = hp.x + 12 + tipW <= w - 8;
    const tipX = preferRight
      ? Math.min(w - tipW - 6, hp.x + 12)
      : Math.max(padL + 4, hp.x - tipW - 12);
    const tipY = Math.max(padT + 2, Math.min(h - tipH - 6, hp.y - tipH / 2));

    drawRoundedRectPath(ctx, tipX, tipY, tipW, tipH, 7);
    ctx.fillStyle = 'rgba(10, 15, 29, 0.95)';
    ctx.fill();
    ctx.lineWidth = 1;
    ctx.strokeStyle = 'rgba(0, 242, 254, 0.55)';
    ctx.stroke();

    ctx.textAlign = 'left';
    tipLines.forEach((ln, i) => {
      ctx.font = ln.bold
        ? '700 10.5px "JetBrains Mono", monospace'
        : '500 10.5px "JetBrains Mono", monospace';
      ctx.fillStyle = ln.color;
      ctx.fillText(ln.text, tipX + 10, tipY + 18 + i * lineH);
    });
    ctx.restore();
  }

  const eqCaptionEl = getEl('equity-curve-caption');
  const ariaSummary = `Portfolio equity curve from ${fmtLocalTime(tMin)} to ${fmtLocalTime(tMax)}: current equity ${fmtUsd(liveEqVal)}, committed notional ${fmtUsd(liveCommVal)}, reserved margin ${fmtUsd(liveResVal)}${gaps.length > 0 ? `, with ${gaps.length} snapshot gap(s)` : ''}.`;
  canvas.setAttribute('aria-label', ariaSummary);
  if (eqCaptionEl) eqCaptionEl.textContent = ariaSummary;
}

export function drawSleeveAttributionCanvas(dash, force = false) {
  const prep = prepareCanvas('sleeve-attr-canvas');
  if (!prep) return;
  const { ctx, w, h, canvas } = prep;

  const snap = (dash && dash.snapshot) || {};
  const rawS1 = Number(snap.s1_gross_weight ?? 0);
  const rawS2 = Number(snap.s2_gross_weight ?? 0);
  const rawS3 = Number(snap.s3_gross_weight ?? 0);
  const s1 = Number.isFinite(rawS1) ? Math.abs(rawS1) : 0;
  const s2 = Number.isFinite(rawS2) ? Math.abs(rawS2) : 0;
  const s3 = Number.isFinite(rawS3) ? Math.abs(rawS3) : 0;

  const fp = computeFingerprint([
    s1.toFixed(4),
    s2.toFixed(4),
    s3.toFixed(4),
    Math.round(w),
    Math.round(h),
  ]);
  if (!force && !isSectionDirty('chart-sleeve', fp)) {
    return;
  }

  ctx.clearRect(0, 0, w, h);

  const bars = [
    { label: 'S1 CARRY-MOM', val: s1, color: '#00f2fe' },
    { label: 'S2 TREND-BRK', val: s2, color: '#10b981' },
    { label: 'S3 EVENT-REV', val: s3, color: '#f59e0b' },
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

    ctx.fillStyle = 'rgba(255, 255, 255, 0.06)';
    ctx.fillRect(padL, barY, trackW, barH);

    const fillW = b.val > 0 ? Math.min(trackW, (b.val / maxW) * trackW) : 0;
    if (fillW > 0) {
      ctx.fillStyle = b.color;
      ctx.fillRect(padL, barY, Math.max(3, fillW), barH);
    }

    ctx.fillStyle = '#ffffff';
    ctx.fillText(`${(b.val * 100).toFixed(1)}%`, padL + trackW + 8, yCenter + 4);
  });

  const slCaptionEl = getEl('sleeve-attr-caption');
  const totalPct = ((s1 + s2 + s3) * 100).toFixed(1);
  const slSummary = `Gross target sleeve exposure total ${totalPct}% of equity: S1 Carry-Momentum ${(s1 * 100).toFixed(1)}%, S2 Trend-Breakout ${(s2 * 100).toFixed(1)}%, S3 Event-Reversion ${(s3 * 100).toFixed(1)}%.`;
  canvas.setAttribute('aria-label', slSummary);
  if (slCaptionEl) slCaptionEl.textContent = slSummary;
}
