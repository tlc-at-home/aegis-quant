#!/usr/bin/env bash
# =============================================================================
# Aegis Quant v2.0 — Automated Fedora Minimal Bootstrap (MariaDB 13.0 Rolling)
# Target Root: /home/fedora/aegis
# =============================================================================
set -euo pipefail

AEGIS_ROOT="/home/fedora/aegis"
DATA_DIR="${AEGIS_ROOT}/data"
LOG_DIR="${AEGIS_ROOT}/logs"
BIN_DIR="${AEGIS_ROOT}/bin"
KEY_PATH="${DATA_DIR}/.aegis_master_key"
DEPLOY_DIR="${AEGIS_ROOT}/deploy"
SQL_SCHEMA="${AEGIS_ROOT}/sql/schema_v2_mariadb.sql"
SYSTEMD_DIR="/etc/systemd/system"
MARIADB_VERSION="${MARIADB_VERSION:-13.0}"

echo "[1/7] Installing system dependencies & chronyd time synchronization..."
sudo dnf install -y --skip-unavailable chrony curl firewalld
sudo systemctl enable --now chronyd.service
chronyc tracking || true

echo "[2/7] Installing & tuning official MariaDB (${MARIADB_VERSION} Rolling)..."
curl -LsS https://r.mariadb.com/downloads/mariadb_repo_setup | sudo bash -s -- --mariadb-server-version="mariadb-${MARIADB_VERSION}" --os-type=rhel --os-version=9
sudo dnf install -y --allowerasing MariaDB-server MariaDB-client
sudo cp "${DEPLOY_DIR}/99-aegis-mariadb.cnf" /etc/my.cnf.d/99-aegis-mariadb.cnf
sudo chmod 644 /etc/my.cnf.d/99-aegis-mariadb.cnf
sudo systemctl enable --now mariadb.service
sudo systemctl restart mariadb.service

echo "[3/7] Provisioning MariaDB database user & applying schema_v2_mariadb.sql..."
sudo mariadb -e "CREATE DATABASE IF NOT EXISTS aegis_quant CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'aegis'@'localhost' IDENTIFIED BY 'aegis_v2_local';
CREATE USER IF NOT EXISTS 'aegis'@'127.0.0.1' IDENTIFIED BY 'aegis_v2_local';
GRANT ALL PRIVILEGES ON aegis_quant.* TO 'aegis'@'localhost';
GRANT ALL PRIVILEGES ON aegis_quant.* TO 'aegis'@'127.0.0.1';
GRANT ALL PRIVILEGES ON \`aegis_quant_test%\`.* TO 'aegis'@'localhost';
GRANT ALL PRIVILEGES ON \`aegis_quant_test%\`.* TO 'aegis'@'127.0.0.1';
FLUSH PRIVILEGES;"
sudo mariadb aegis_quant < "${SQL_SCHEMA}"

echo "[4/7] Preparing Aegis Quant directories (${DATA_DIR}, ${LOG_DIR}, ${BIN_DIR})..."
mkdir -p "${DATA_DIR}" "${LOG_DIR}" "${BIN_DIR}"
chmod 700 "${DATA_DIR}"
chmod 750 "${LOG_DIR}"
chmod 755 "${BIN_DIR}"

echo "[5/7] Building or verifying Aegis Quant v2.0 Rust binary (${BIN_DIR}/aegis-quant)..."
if [ ! -x "${BIN_DIR}/aegis-quant" ]; then
    cargo build --release --manifest-path "${AEGIS_ROOT}/Cargo.toml"
    cp "${AEGIS_ROOT}/target/release/aegis-quant" "${BIN_DIR}/aegis-quant"
fi
chmod 755 "${BIN_DIR}/aegis-quant"
chcon -t bin_t "${BIN_DIR}/aegis-quant" 2>/dev/null || true

echo "[6/7] Verifying CSPRNG Fernet master key & initializing MariaDB seed..."
if [ ! -s "${KEY_PATH}" ]; then
    (umask 0077; "${BIN_DIR}/aegis-quant" db --generate-key > "${KEY_PATH}")
fi
chmod 600 "${KEY_PATH}"
"${BIN_DIR}/aegis-quant" db --init --migrate

echo "[7/7] Installing and starting hardened aegis-quant.service..."
sudo systemctl disable --now aegis-pit.service aegis-pond.service aegis-settle.service aegis-settle.timer 2>/dev/null || true
sudo rm -f "${SYSTEMD_DIR}/aegis-pit.service" "${SYSTEMD_DIR}/aegis-pond.service" "${SYSTEMD_DIR}/aegis-settle.service" "${SYSTEMD_DIR}/aegis-settle.timer"
sudo cp "${DEPLOY_DIR}/aegis-quant.service" "${SYSTEMD_DIR}/aegis-quant.service"
sudo chmod 644 "${SYSTEMD_DIR}/aegis-quant.service"
sudo restorecon -v "${SYSTEMD_DIR}/aegis-quant.service" 2>/dev/null || true
sudo systemctl daemon-reload
sudo systemctl enable aegis-quant.service
sudo systemctl restart aegis-quant.service

echo "Waiting for Aegis Quant v2.0 WebUI server on http://127.0.0.1:8000/api/v1/health..."
for _ in $(seq 1 30); do
    if curl -fsS http://127.0.0.1:8000/api/v1/health >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

echo "=== Bootstrap Complete ==="
systemctl is-active mariadb.service
systemctl is-active aegis-quant.service
curl -fsS http://127.0.0.1:8000/api/v1/health
echo ""
