# Aegis Quant (`v0.1.0-dev`)

Autonomous, self-custodial Web3 DEX quantitative crypto trading platform targeting **Hyperliquid USDC Perpetuals (`USDC_PERP`)** on **Fedora 45 Minimal** (`/home/fedora/aegis`).

## Key Capabilities (`v0.1.0-dev`)
- **Hyperliquid L1 Native Execution (`ccxt.async_support.hyperliquid`)**: Non-custodial routing via a Main Arbitrum Wallet (`wallet_address`) and Trade-Only Agent Wallet (`private_key`), modeling 1.5 bps maker / 4.5 bps taker fees and 1-hour funding settlement.
- **Synthetic `ALT/BTC` Cross-Pair Engine (`BTC_SYNTH`)**: Computes timestamp-aligned `ALT/BTC:SYNTH` OHLCV candles from `ALT/USDC:USDC` and `BTC/USDC:USDC` perpetuals ($P_{\text{Alt/BTC}}(t) = P_{\text{Alt/USDC}}(t) / P_{\text{BTC/USDC}}(t)$).
- **3-State Gaussian HMM Regime Gate (540-Bar 4H Window)**: Decodes posterior regime probabilities $\gamma_t(\text{Bull})$, $\gamma_t(\text{Bear})$, and $\gamma_t(\text{Chop})$ over a rolling 90-day window.
- **MACE Non-Linear Factor Filter**: Blends a regularized Random Forest + Ridge Regression MACE ensemble ($35\%$) with multi-horizon SNR continuous momentum ($65\%$).
- **Long Leaders, Bear Laggard Shorts & Idle Basis Harvest**: Longs top 2 relative-strength leaders in Bull regimes, shorts bottom 2 relative-weakness laggards in Bear regimes with inverted Chandelier trailing stops and 1H funding carry protection, and harvests delta-neutral basis/HLP yield on idle USDC.
- **Conviction-Scaled Risk Engine**: Scales per-trade risk from $50 ($0.5\%$) to $150 ($1.5\%$) within a $400 ($4.0\%$) total portfolio heat cap and $1.5\times$ gross leverage limit on a $10,000 USDC baseline.

## Target Directory Structure (`/home/fedora/aegis`)
- `deploy/bootstrap_vm.sh` & `deploy/aegis-quant.service` — Fedora 45 Minimal provisioning & systemd service (`User=fedora`)
- `sql/schema_and_seed.sql` — SQLite WAL schema, 3 views, 60 seed config variables, and 31 universe rows
- `engine/` — `database.py`, `ingestion.py`, `signals.py`, `risk.py`, `execution.py`, `calibration.py`, `main.py` (`__version__ = "0.1.0-dev"`)
- `web/` — `server.py` & `static/index.html` FastAPI REST + SSE live telemetry dashboard (`:8000`)
