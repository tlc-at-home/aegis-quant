#!/usr/bin/env bash
# =============================================================================
# Aegis Quant (v0.2.0-rc2 Rust Native) — Automated Fedora 45 Minimal Bootstrap
# Target Root: /home/fedora/aegis
# =============================================================================
set -euo pipefail

AEGIS_ROOT="/home/fedora/aegis"
DATA_DIR="${AEGIS_ROOT}/data"
LOG_DIR="${AEGIS_ROOT}/logs"
BIN_DIR="${AEGIS_ROOT}/bin"
DB_PATH="${DATA_DIR}/aegis_quant.db"
SERVICE_SRC="${AEGIS_ROOT}/deploy/aegis-quant.service"
SERVICE_DST="/etc/systemd/system/aegis-quant.service"

echo "[1/5] Verifying system dependencies & chronyd time synchronization on Fedora..."
sudo dnf install -y --skip-unavailable sqlite chrony curl
sudo systemctl enable --now chronyd.service
chronyc tracking || true

echo "[2/5] Preparing Aegis Quant directories (${DATA_DIR}, ${LOG_DIR}, ${BIN_DIR})..."
mkdir -p "${DATA_DIR}" "${LOG_DIR}" "${BIN_DIR}"
chmod 700 "${DATA_DIR}"
chmod 755 "${LOG_DIR}" "${BIN_DIR}"

echo "[3/5] Verifying Aegis Quant Rust binary (${BIN_DIR}/aegis-quant)..."
if [ ! -x "${BIN_DIR}/aegis-quant" ]; then
    echo "Building release binary via cargo..."
    cargo build --release --manifest-path "${AEGIS_ROOT}/Cargo.toml"
    cp /tmp/aegis-cargo-target/release/aegis-quant "${BIN_DIR}/aegis-quant"
fi
chmod 755 "${BIN_DIR}/aegis-quant"
chcon -t bin_t "${BIN_DIR}/aegis-quant" 2>/dev/null || true

echo "[4/5] Initializing/verifying SQLite WAL database (${DB_PATH})..."
"${BIN_DIR}/aegis-quant" --db "${DB_PATH}" db --init
chmod 600 "${DB_PATH}"

echo "[5/5] Installing and starting systemd service (aegis-quant.service)..."
sudo cp "${SERVICE_SRC}" "${SERVICE_DST}"
sudo chmod 644 "${SERVICE_DST}"
sudo restorecon -v "${SERVICE_DST}" 2>/dev/null || true
sudo systemctl daemon-reload
sudo systemctl enable aegis-quant.service
sudo systemctl restart aegis-quant.service

echo "Waiting for Aegis Quant Axum WebUI server on http://127.0.0.1:8000..."
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
