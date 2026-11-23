#!/usr/bin/env bash
# =============================================================================
# Aegis Quant v2.0 — Automated Fedora Minimal Bootstrap (MariaDB 13.0 Rolling)
# Target Root: /home/fedora/aegis
# =============================================================================
set -euo pipefail

ENABLE_DEV_TEST_GRANTS=0
for arg in "$@"; do
    case "${arg}" in
        --enable-dev-test-grants)
            ENABLE_DEV_TEST_GRANTS=1
            ;;
        -h|--help)
            echo "Usage: $0 [--enable-dev-test-grants]"
            exit 0
            ;;
        *)
            echo "ERROR: Unknown argument: ${arg}" >&2
            exit 2
            ;;
    esac
done

AEGIS_ROOT="/home/fedora/aegis"
DATA_DIR="${AEGIS_ROOT}/data"
LOG_DIR="${AEGIS_ROOT}/logs"
BIN_DIR="${AEGIS_ROOT}/bin"
BACKUP_DIR="/var/backups/aegis-mariadb"
KEY_PATH="${DATA_DIR}/.aegis_master_key"
ENV_FILE="/etc/aegis-quant.env"
DEPLOY_DIR="${AEGIS_ROOT}/deploy"
SQL_SCHEMA="${AEGIS_ROOT}/sql/schema_v2_mariadb.sql"
SYSTEMD_DIR="/etc/systemd/system"
MARIADB_VERSION="${MARIADB_VERSION:-13.0}"
MARIADB_RPM_GPG_URL="https://supplychain.mariadb.com/MariaDB-Server-GPG-KEY"
MARIADB_RPM_GPG_PATH="/etc/pki/rpm-gpg/MariaDB-Server-GPG-KEY"
MARIADB_RPM_GPG_FPR="177F4010FE56CA3336300305F1656F24C74CD1D8"

if ! [[ "${MARIADB_VERSION}" =~ ^[0-9]+\.[0-9]+$ ]]; then
    echo "ERROR: Invalid MARIADB_VERSION '${MARIADB_VERSION}' (expected major.minor)" >&2
    exit 1
fi

install_mariadb_gpg_verified_repo() {
    local arch
    arch="$(uname -m)"
    case "${arch}" in
        x86_64|aarch64) ;;
        *)
            echo "ERROR: Unsupported architecture '${arch}' for official MariaDB RPM repo" >&2
            exit 1
            ;;
    esac

    local tmp_key=""
    if [ ! -f "${MARIADB_RPM_GPG_PATH}" ]; then
        tmp_key="$(mktemp)"
        trap 'rm -f "${tmp_key:-}"' RETURN
        curl -fsSL "${MARIADB_RPM_GPG_URL}" -o "${tmp_key}"
        local actual_fpr
        actual_fpr="$(gpg --batch --show-keys --with-colons "${tmp_key}" | awk -F: '$1 == "fpr" {print $10; exit}')"
        if [ "${actual_fpr}" != "${MARIADB_RPM_GPG_FPR}" ]; then
            echo "ERROR: MariaDB GPG key fingerprint mismatch: got '${actual_fpr}', expected '${MARIADB_RPM_GPG_FPR}'" >&2
            exit 1
        fi
        sudo install -m 0644 -o root -g root "${tmp_key}" "${MARIADB_RPM_GPG_PATH}"
    else
        local existing_fpr
        existing_fpr="$(gpg --batch --show-keys --with-colons "${MARIADB_RPM_GPG_PATH}" | awk -F: '$1 == "fpr" {print $10; exit}')"
        if [ "${existing_fpr}" != "${MARIADB_RPM_GPG_FPR}" ]; then
            echo "ERROR: Existing ${MARIADB_RPM_GPG_PATH} fingerprint mismatch: '${existing_fpr}'" >&2
            exit 1
        fi
    fi

    sudo rpm --import "${MARIADB_RPM_GPG_PATH}"
    sudo tee /etc/yum.repos.d/mariadb.repo >/dev/null <<EOF
[mariadb-main]
name = MariaDB Server
baseurl = https://dlm.mariadb.com/repo/mariadb-server/${MARIADB_VERSION}/yum/rhel/9/${arch}
gpgkey = file://${MARIADB_RPM_GPG_PATH}
gpgcheck = 1
repo_gpgcheck = 1
enabled = 1
module_hotfixes = 1
EOF
    sudo chmod 0644 /etc/yum.repos.d/mariadb.repo
    sudo dnf install -y --allowerasing MariaDB-server MariaDB-client
}

echo "[1/8] Installing system dependencies, chronyd time sync, and firewalld..."
sudo dnf install -y --skip-unavailable chrony curl firewalld gzip openssl gnupg2 policycoreutils-python-utils
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

if [ -f /etc/fedora-release ] && [ "${MARIADB_USE_DISTRO_PKG:-0}" = "1" ]; then
    sudo dnf install -y mariadb-server mariadb
else
    install_mariadb_gpg_verified_repo
fi

sudo cp "${DEPLOY_DIR}/99-aegis-mariadb.cnf" /etc/my.cnf.d/99-aegis-mariadb.cnf
sudo chmod 644 /etc/my.cnf.d/99-aegis-mariadb.cnf

if mariadbd --help --verbose 2>/dev/null | grep -q -- '--validate-config'; then
    sudo mariadbd --defaults-file=/etc/my.cnf.d/99-aegis-mariadb.cnf --validate-config
else
    sudo mariadbd --defaults-file=/etc/my.cnf.d/99-aegis-mariadb.cnf --verbose --help >/dev/null
fi

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

echo "[3/8] Preparing Aegis Quant directories & 0600 CSPRNG environment secret file (${ENV_FILE})..."
mkdir -p "${DATA_DIR}" "${LOG_DIR}" "${BIN_DIR}"
chmod 700 "${DATA_DIR}"
chmod 750 "${LOG_DIR}"
chmod 755 "${BIN_DIR}"
sudo install -d -m 0700 -o root -g root "${BACKUP_DIR}"

NEW_DB_PASS=""
TARGET_DB_URL="${AEGIS_DB_URL:-}"
if [ -z "${TARGET_DB_URL}" ] && sudo test -f "${ENV_FILE}"; then
    TARGET_DB_URL="$(sudo awk -F= '/^AEGIS_DB_URL=/{sub(/^[^=]*=/,""); print; exit}' "${ENV_FILE}" || true)"
fi

if [ -z "${TARGET_DB_URL}" ]; then
    NEW_DB_PASS="$(
        umask 0077
        openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | head -c 36
    )"
    if [ "${#NEW_DB_PASS}" -ne 36 ]; then
        echo "ERROR: Failed to generate 36-character CSPRNG MariaDB password" >&2
        exit 1
    fi
    TARGET_DB_URL="mysql://aegis:${NEW_DB_PASS}@127.0.0.1:3306/aegis_quant"
fi

if [ ! -f "${ENV_FILE}" ] || [ -n "${NEW_DB_PASS}" ]; then
    (
        umask 0077
        sudo install -m 0600 -o root -g root /dev/null "${ENV_FILE}"
        sudo tee "${ENV_FILE}" >/dev/null <<EOF
RUST_LOG=info
AEGIS_DB_URL=${TARGET_DB_URL}
EOF
    )
fi
sudo chown root:root "${ENV_FILE}"
sudo chmod 0600 "${ENV_FILE}"

echo "[4/8] Provisioning least-privilege MariaDB database user & applying schema_v2_mariadb.sql..."
if [ -n "${NEW_DB_PASS}" ]; then
    sudo mariadb <<SQL
CREATE DATABASE IF NOT EXISTS aegis_quant CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'aegis'@'localhost' IDENTIFIED BY '${NEW_DB_PASS}';
CREATE USER IF NOT EXISTS 'aegis'@'127.0.0.1' IDENTIFIED BY '${NEW_DB_PASS}';
ALTER USER 'aegis'@'localhost' IDENTIFIED BY '${NEW_DB_PASS}';
ALTER USER 'aegis'@'127.0.0.1' IDENTIFIED BY '${NEW_DB_PASS}';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, REFERENCES, CREATE VIEW, SHOW VIEW, LOCK TABLES ON aegis_quant.* TO 'aegis'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, REFERENCES, CREATE VIEW, SHOW VIEW, LOCK TABLES ON aegis_quant.* TO 'aegis'@'127.0.0.1';
FLUSH PRIVILEGES;
SQL
    unset NEW_DB_PASS
else
    sudo mariadb <<'SQL'
CREATE DATABASE IF NOT EXISTS aegis_quant CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, REFERENCES, CREATE VIEW, SHOW VIEW, LOCK TABLES ON aegis_quant.* TO 'aegis'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, REFERENCES, CREATE VIEW, SHOW VIEW, LOCK TABLES ON aegis_quant.* TO 'aegis'@'127.0.0.1';
FLUSH PRIVILEGES;
SQL
fi

if [ "${ENABLE_DEV_TEST_GRANTS}" -eq 1 ]; then
    echo "Granting dev/test isolated database privileges on aegis_quant_test%* (--enable-dev-test-grants)..."
    sudo mariadb <<'SQL'
GRANT ALL PRIVILEGES ON `aegis\_quant\_test%`.* TO 'aegis'@'localhost';
GRANT ALL PRIVILEGES ON `aegis\_quant\_test%`.* TO 'aegis'@'127.0.0.1';
FLUSH PRIVILEGES;
SQL
fi

sudo mariadb aegis_quant < "${SQL_SCHEMA}"

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
AEGIS_DB_URL="${TARGET_DB_URL}" "${BIN_DIR}/aegis-quant" --db "${DATA_DIR}/aegis_quant.db" db --init --migrate
unset TARGET_DB_URL

echo "[7/8] Installing daily mariadb-dump backup script & systemd timer..."
sudo tee /usr/local/bin/aegis-mariadb-backup.sh >/dev/null <<EOF
#!/usr/bin/env bash
set -euo pipefail
umask 0077
[ ! -L "${BACKUP_DIR}" ] || {
    echo "ERROR: Refusing symlink backup directory: ${BACKUP_DIR}" >&2
    exit 1
}
install -d -m 0700 -o root -g root "${BACKUP_DIR}"
STAMP="\$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${BACKUP_DIR}/aegis_quant_\${STAMP}.sql.gz"
TMP="\$(mktemp "${BACKUP_DIR}/.aegis_quant_\${STAMP}.XXXXXX.tmp")"
trap 'rm -f "\${TMP:-}"' EXIT
mariadb-dump --single-transaction --routines --events aegis_quant | gzip -c >| "\${TMP}"
chmod 0600 "\${TMP}"
mv -n "\${TMP}" "\${OUT}"
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
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true
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
