#!/usr/bin/env python3
"""
2025 Frozen-Calibration Out-of-Sample Holdout Replay Runner for Aegis Quant (v0.2.0-rc2).
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.backtest import HistoricalBacktester


def main() -> None:
    print("=================================================================")
    print("  AEGIS QUANT v0.2.0-rc2: 2025 Frozen-Calibration Holdout Replay ")
    print("=================================================================")

    backtester = HistoricalBacktester(
        data_path="data/real_4h_backfill.json",
        initial_equity=10000.0,
    )
    try:
        tear_sheet = backtester.run(start_date="2025-01-01", end_date="2025-12-31")
        tear_sheet.print_summary()

        # Holdout Acceptance Gate Verification
        if tear_sheet.sharpe < 1.20 or tear_sheet.max_drawdown > 0.22:
            print("\n❌ HOLDOUT VALIDATION FAILED: Performance below 2025 threshold limits.")
            sys.exit(1)

        print("\n✅ HOLDOUT VALIDATION PASSED: 2025 performance bounds verified.")
        sys.exit(0)
    finally:
        backtester.mem_db.close()


if __name__ == "__main__":
    main()
