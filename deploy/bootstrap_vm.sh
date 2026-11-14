#!/usr/bin/env bash
# =============================================================================
# Aegis Quant (v0.3.0-rc1 Rust Native) — Automated Fedora 45 Minimal Bootstrap
# Target Root: /home/fedora/aegis
# =============================================================================
set -euo pipefail

AEGIS_ROOT="/home/fedora/aegis"
DATA_DIR="${AEGIS_ROOT}/data"
LOG_DIR="${AEGIS_ROOT}/logs"
BIN_DIR="${AEGIS_ROOT}/bin"
DB_PATH="${DATA_DIR}/aegis_quant.db"
KEY_PATH="${DATA_DIR}/.aegis_master_key"
DEPLOY_DIR="${AEGIS_ROOT}/deploy"
SYSTEMD_DIR="/etc/systemd/system"
SYSTEMD_UNITS=(
    "aegis-quant.service"
    "aegis-pit.service"
    "aegis-pond.service"
    "aegis-settle.service"
    "aegis-settle.timer"
)

echo "[1/6] Verifying system dependencies & chronyd time synchronization on Fedora..."
sudo dnf install -y --skip-unavailable sqlite chrony curl firewalld
sudo systemctl enable --now chronyd.service
chronyc tracking || true

echo "[2/6] Configuring firewalld to block external access to port 8000 (loopback/SSH tunnel only)..."
if sudo systemctl is-active --quiet firewalld || sudo systemctl enable --now firewalld 2>/dev/null; then
    sudo firewall-cmd --permanent --remove-port=8000/tcp 2>/dev/null || true
    sudo firewall-cmd --reload 2>/dev/null || true
fi

echo "[3/6] Preparing Aegis Quant directories (${DATA_DIR}, ${LOG_DIR}, ${BIN_DIR})..."
mkdir -p "${DATA_DIR}" "${LOG_DIR}" "${BIN_DIR}"
chmod 700 "${DATA_DIR}"
chmod 750 "${LOG_DIR}"
chmod 755 "${BIN_DIR}"

echo "[4/6] Verifying Aegis Quant Rust binary (${BIN_DIR}/aegis-quant)..."
if [ ! -x "${BIN_DIR}/aegis-quant" ]; then
    echo "Building release binary via cargo..."
    cargo build --release --manifest-path "${AEGIS_ROOT}/Cargo.toml"
    if [ -f "${AEGIS_ROOT}/target/release/aegis-quant" ]; then
        cp "${AEGIS_ROOT}/target/release/aegis-quant" "${BIN_DIR}/aegis-quant"
    elif [ -f "/tmp/aegis-cargo-target/release/aegis-quant" ]; then
        cp "/tmp/aegis-cargo-target/release/aegis-quant" "${BIN_DIR}/aegis-quant"
    fi
fi
chmod 755 "${BIN_DIR}/aegis-quant"
chcon -t bin_t "${BIN_DIR}/aegis-quant" 2>/dev/null || true

echo "[5/6] Provisioning CSPRNG Fernet master key & SQLite WAL database (${DB_PATH})..."
if [ ! -s "${KEY_PATH}" ]; then
    (umask 0077; "${BIN_DIR}/aegis-quant" --db "${DB_PATH}" db --generate-key > "${KEY_PATH}")
fi
chmod 600 "${KEY_PATH}"
"${BIN_DIR}/aegis-quant" --db "${DB_PATH}" db --init --migrate
chmod 600 "${DB_PATH}"

echo "[6/6] Installing and starting hardened systemd units (${SYSTEMD_UNITS[*]}) (R19-L4)..."
for unit in "${SYSTEMD_UNITS[@]}"; do
    src="${DEPLOY_DIR}/${unit}"
    dst="${SYSTEMD_DIR}/${unit}"
    if [ ! -s "${src}" ]; then
        echo "ERROR: Missing required systemd unit ${src}" >&2
        exit 1
    fi
    sudo cp "${src}" "${dst}"
    sudo chmod 644 "${dst}"
    sudo restorecon -v "${dst}" 2>/dev/null || true
done
sudo systemctl daemon-reload
sudo systemctl enable --now aegis-settle.timer
sudo systemctl enable aegis-quant.service
sudo systemctl restart aegis-quant.service

for unit in "${SYSTEMD_UNITS[@]}"; do
    if [ ! -s "${SYSTEMD_DIR}/${unit}" ]; then
        echo "ERROR: Installed systemd unit missing at ${SYSTEMD_DIR}/${unit}" >&2
        exit 1
    fi
done

echo "Waiting for Aegis Quant Axum WebUI server on http://127.0.0.1:8000/api/v1/health..."
for _ in $(seq 1 30); do
    if curl -fsS http://127.0.0.1:8000/api/v1/health >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

echo "=== Bootstrap Complete ==="
systemctl is-active aegis-quant.service
systemctl is-active aegis-settle.timer
chronyc tracking
curl -fsS http://127.0.0.1:8000/api/v1/health
echo ""
