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
BACKUP_DIR="${DATA_DIR}/mariadb_dumps"
KEY_PATH="${DATA_DIR}/.aegis_master_key"
ENV_FILE="/etc/aegis-quant.env"
DEPLOY_DIR="${AEGIS_ROOT}/deploy"
SQL_SCHEMA="${AEGIS_ROOT}/sql/schema_v2_mariadb.sql"
SYSTEMD_DIR="/etc/systemd/system"
MARIADB_VERSION="${MARIADB_VERSION:-13.0}"
AEGIS_DB_URL_DEFAULT="mysql://aegis:aegis_v2_local@127.0.0.1:3306/aegis_quant"

echo "[1/8] Installing system dependencies, chronyd time sync, and firewalld..."
sudo dnf install -y --skip-unavailable chrony curl firewalld gzip policycoreutils-python-utils
sudo systemctl enable --now chronyd.service
chronyc tracking || true

sudo systemctl enable --now firewalld.service
sudo firewall-cmd --permanent --add-service=ssh
# Allow WebUI port 8000 only from libvirt host bridge subnet; MariaDB 3306 stays bound to 127.0.0.1
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="192.168.122.0/24" port port="8000" protocol="tcp" accept'
sudo firewall-cmd --reload

echo "[2/8] Preparing Btrfs NOCOW /var/lib/mysql and installing MariaDB (${MARIADB_VERSION} Rolling)..."
sudo mkdir -p /var/lib/mysql
# Set NOCOW before MariaDB initializes InnoDB tablespaces (safe no-op on non-Btrfs filesystems)
sudo chattr +C /var/lib/mysql 2>/dev/null || true

if [ -f /etc/fedora-release ]; then
    sudo dnf install -y mariadb-server mariadb || {
        curl -LsS https://r.mariadb.com/downloads/mariadb_repo_setup | sudo bash -s -- --mariadb-server-version="mariadb-${MARIADB_VERSION}"
        sudo dnf install -y --allowerasing MariaDB-server MariaDB-client
    }
else
    curl -LsS https://r.mariadb.com/downloads/mariadb_repo_setup | sudo bash -s -- --mariadb-server-version="mariadb-${MARIADB_VERSION}" --os-type=rhel --os-version=9
    sudo dnf install -y --allowerasing MariaDB-server MariaDB-client
fi

sudo cp "${DEPLOY_DIR}/99-aegis-mariadb.cnf" /etc/my.cnf.d/99-aegis-mariadb.cnf
sudo chmod 644 /etc/my.cnf.d/99-aegis-mariadb.cnf

# Raise MariaDB LimitNOFILE to 65536
sudo mkdir -p "${SYSTEMD_DIR}/mariadb.service.d"
sudo tee "${SYSTEMD_DIR}/mariadb.service.d/limits.conf" >/dev/null <<'EOF'
[Service]
LimitNOFILE=65536
EOF
sudo chmod 644 "${SYSTEMD_DIR}/mariadb.service.d/limits.conf"
sudo systemctl daemon-reload
sudo systemctl enable --now mariadb.service
sudo systemctl restart mariadb.service

echo "[3/8] Provisioning MariaDB database user & applying schema_v2_mariadb.sql..."
sudo mariadb <<'SQL'
CREATE DATABASE IF NOT EXISTS aegis_quant CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'aegis'@'localhost' IDENTIFIED BY 'aegis_v2_local';
CREATE USER IF NOT EXISTS 'aegis'@'127.0.0.1' IDENTIFIED BY 'aegis_v2_local';
GRANT ALL PRIVILEGES ON aegis_quant.* TO 'aegis'@'localhost';
GRANT ALL PRIVILEGES ON aegis_quant.* TO 'aegis'@'127.0.0.1';
GRANT ALL PRIVILEGES ON `aegis\_quant\_test%`.* TO 'aegis'@'localhost';
GRANT ALL PRIVILEGES ON `aegis\_quant\_test%`.* TO 'aegis'@'127.0.0.1';
FLUSH PRIVILEGES;
SQL
sudo mariadb aegis_quant < "${SQL_SCHEMA}"

echo "[4/8] Preparing Aegis Quant directories & 0600 environment secret file (${ENV_FILE})..."
mkdir -p "${DATA_DIR}" "${LOG_DIR}" "${BIN_DIR}" "${BACKUP_DIR}"
chmod 700 "${DATA_DIR}" "${BACKUP_DIR}"
chmod 750 "${LOG_DIR}"
chmod 755 "${BIN_DIR}"

if [ ! -f "${ENV_FILE}" ]; then
    sudo install -m 0600 -o root -g root /dev/null "${ENV_FILE}"
    sudo tee "${ENV_FILE}" >/dev/null <<EOF
RUST_LOG=info
AEGIS_DB_URL=${AEGIS_DB_URL:-${AEGIS_DB_URL_DEFAULT}}
EOF
fi
sudo chmod 0600 "${ENV_FILE}"

echo "[5/8] Building or updating Aegis Quant v2.0 Rust binary (${BIN_DIR}/aegis-quant)..."
if [ ! -x "${BIN_DIR}/aegis-quant" ] || [ "${AEGIS_REBUILD:-0}" = "1" ]; then
    cargo build --release --manifest-path "${AEGIS_ROOT}/Cargo.toml"
    cp "${AEGIS_ROOT}/target/release/aegis-quant" "${BIN_DIR}/aegis-quant"
fi
chmod 755 "${BIN_DIR}/aegis-quant"
sudo semanage fcontext -a -t bin_t "${BIN_DIR}/aegis-quant" 2>/dev/null || true
chcon -t bin_t "${BIN_DIR}/aegis-quant" 2>/dev/null || true

echo "[6/8] Verifying CSPRNG Fernet master key & initializing MariaDB seed..."
cd "${AEGIS_ROOT}"
if [ ! -s "${KEY_PATH}" ]; then
    (umask 0077; "${BIN_DIR}/aegis-quant" db --generate-key > "${KEY_PATH}")
fi
chmod 600 "${KEY_PATH}"
AEGIS_DB_URL="${AEGIS_DB_URL:-${AEGIS_DB_URL_DEFAULT}}" "${BIN_DIR}/aegis-quant" --db "${DATA_DIR}/aegis_quant.db" db --init --migrate

echo "[7/8] Installing daily mariadb-dump backup script & systemd timer..."
sudo tee /usr/local/bin/aegis-mariadb-backup.sh >/dev/null <<EOF
#!/usr/bin/env bash
set -euo pipefail
mkdir -p "${BACKUP_DIR}"
chmod 700 "${BACKUP_DIR}"
STAMP="\$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${BACKUP_DIR}/aegis_quant_\${STAMP}.sql.gz"
TMP="\${OUT}.tmp"
umask 0077
mariadb-dump --single-transaction --routines --events aegis_quant | gzip -c > "\${TMP}"
mv -f "\${TMP}" "\${OUT}"
chmod 0600 "\${OUT}"
find "${BACKUP_DIR}" -maxdepth 1 -type f -name 'aegis_quant_*.sql.gz' -mtime +14 -delete
EOF
sudo chmod 750 /usr/local/bin/aegis-mariadb-backup.sh

sudo tee "${SYSTEMD_DIR}/aegis-mariadb-backup.service" >/dev/null <<'EOF'
[Unit]
Description=Daily compressed mariadb-dump backup for Aegis Quant v2.0
After=mariadb.service

[Service]
Type=oneshot
ExecStart=/usr/local/bin/aegis-mariadb-backup.sh
EOF
sudo chmod 644 "${SYSTEMD_DIR}/aegis-mariadb-backup.service"

sudo tee "${SYSTEMD_DIR}/aegis-mariadb-backup.timer" >/dev/null <<'EOF'
[Unit]
Description=Daily timer for Aegis Quant v2.0 MariaDB logical backups

[Timer]
OnCalendar=daily
RandomizedDelaySec=300
Persistent=true

[Install]
WantedBy=timers.target
EOF
sudo chmod 644 "${SYSTEMD_DIR}/aegis-mariadb-backup.timer"

echo "[8/8] Installing and starting hardened aegis-quant.service..."
sudo systemctl disable --now aegis-pit.service aegis-pond.service aegis-settle.service aegis-settle.timer 2>/dev/null || true
sudo rm -f "${SYSTEMD_DIR}/aegis-pit.service" "${SYSTEMD_DIR}/aegis-pond.service" "${SYSTEMD_DIR}/aegis-settle.service" "${SYSTEMD_DIR}/aegis-settle.timer"
sudo cp "${DEPLOY_DIR}/aegis-quant.service" "${SYSTEMD_DIR}/aegis-quant.service"
sudo chmod 644 "${SYSTEMD_DIR}/aegis-quant.service"
sudo restorecon -v "${SYSTEMD_DIR}/aegis-quant.service" "${SYSTEMD_DIR}/aegis-mariadb-backup.service" "${SYSTEMD_DIR}/aegis-mariadb-backup.timer" 2>/dev/null || true
sudo systemctl daemon-reload
sudo systemctl enable --now aegis-mariadb-backup.timer
sudo systemctl enable aegis-quant.service
sudo systemctl restart aegis-quant.service

echo "Waiting for Aegis Quant v2.0 WebUI server on http://127.0.0.1:8000/api/v1/health..."
HEALTHY=0
for _ in $(seq 1 30); do
    if curl -fsS http://127.0.0.1:8000/api/v1/health >/dev/null 2>&1; then
        HEALTHY=1
        break
    fi
    sleep 1
done

if [ "${HEALTHY}" -ne 1 ]; then
    echo "ERROR: Aegis Quant v2.0 health endpoint failed to respond within 30s" >&2
    sudo journalctl -u aegis-quant.service -n 40 --no-pager || true
    exit 1
fi

echo "=== Bootstrap Complete ==="
systemctl is-active mariadb.service
systemctl is-active aegis-quant.service
systemctl is-active aegis-mariadb-backup.timer
curl -fsS http://127.0.0.1:8000/api/v1/health
echo ""
