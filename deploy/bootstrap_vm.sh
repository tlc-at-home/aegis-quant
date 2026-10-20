#!/usr/bin/env bash
# =============================================================================
# Aegis Quant (v0.1.0-dev) — Automated Fedora 45 Minimal Bootstrap Script
# Target Root: /home/fedora/aegis
# =============================================================================
set -euo pipefail

AEGIS_ROOT="/home/fedora/aegis"
DATA_DIR="${AEGIS_ROOT}/data"
LOG_DIR="${AEGIS_ROOT}/logs"
DB_PATH="${DATA_DIR}/aegis_quant.db"
SCHEMA_SQL="${AEGIS_ROOT}/sql/schema_and_seed.sql"
VENV_DIR="${AEGIS_ROOT}/venv"
SERVICE_SRC="${AEGIS_ROOT}/deploy/aegis-quant.service"
SERVICE_DST="/etc/systemd/system/aegis-quant.service"

echo "[1/6] Verifying system dependencies & chronyd time synchronization on Fedora..."
sudo dnf install -y --skip-unavailable \
    python3 python3-pip python3-virtualenv python3-devel \
    python3-cffi python3-pycares python3-aiodns \
    gcc make libffi-devel c-ares-devel \
    sqlite chrony curl

sudo systemctl enable --now chronyd.service
chronyc tracking || true

echo "[2/6] Preparing Aegis Quant directories (${DATA_DIR}, ${LOG_DIR})..."
mkdir -p "${DATA_DIR}" "${LOG_DIR}"
chmod 700 "${DATA_DIR}"
chmod 755 "${LOG_DIR}"

echo "[3/6] Setting up Python virtual environment & installing requirements.txt..."
if [ ! -x "${VENV_DIR}/bin/uvicorn" ]; then
    rm -rf "${VENV_DIR}"
    python3 -m venv --system-site-packages "${VENV_DIR}"
fi
"${VENV_DIR}/bin/pip" install --upgrade pip
"${VENV_DIR}/bin/pip" install -r "${AEGIS_ROOT}/requirements.txt"
chcon -R -t bin_t "${VENV_DIR}/bin" 2>/dev/null || true

echo "[4/6] Initializing SQLite WAL database (${DB_PATH})..."
sqlite3 "${DB_PATH}" < "${SCHEMA_SQL}"
chmod 600 "${DB_PATH}"

JOURNAL_MODE=$(sqlite3 "${DB_PATH}" "PRAGMA journal_mode;")
CFG_COUNT=$(sqlite3 "${DB_PATH}" "SELECT COUNT(*) FROM system_config;")
USDC_COUNT=$(sqlite3 "${DB_PATH}" "SELECT COUNT(*) FROM v_universe_usdc_perp;")
SYNTH_COUNT=$(sqlite3 "${DB_PATH}" "SELECT COUNT(*) FROM v_universe_btc_synth;")
PAIRS_COUNT=$(sqlite3 "${DB_PATH}" "SELECT COUNT(*) FROM v_universe_alt_pairs;")

echo "  -> journal_mode=${JOURNAL_MODE} | system_config=${CFG_COUNT} | usdc_perp=${USDC_COUNT} | btc_synth=${SYNTH_COUNT} | alt_pairs=${PAIRS_COUNT}"

echo "[5/6] Installing and starting systemd service (aegis-quant.service)..."
sudo cp "${SERVICE_SRC}" "${SERVICE_DST}"
sudo chmod 644 "${SERVICE_DST}"
sudo restorecon -v "${SERVICE_DST}" 2>/dev/null || true
sudo systemctl daemon-reload
sudo systemctl enable aegis-quant.service
sudo systemctl restart aegis-quant.service

echo "[6/6] Waiting for Aegis Quant FastAPI server on http://127.0.0.1:8000..."
for _ in $(seq 1 30); do
    if curl -fsS http://127.0.0.1:8000/api/v1/status >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

echo "=== Bootstrap Complete ==="
systemctl is-active aegis-quant.service
chronyc tracking
curl -fsS http://127.0.0.1:8000/api/v1/status
echo ""
curl -fsS http://127.0.0.1:8000/api/v1/portfolio
echo ""

