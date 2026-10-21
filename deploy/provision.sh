#!/usr/bin/env bash
# Provision a fresh Ubuntu 24.04 VM (ARM or x86, Minimal image is fine) for WikiPulse.
# Run ON the VM as the default user (ubuntu), after copying .env and secrets/ (see deploy/README.md):
#
#     bash deploy/provision.sh
#
# Idempotent: safe to re-run. Installs Docker Engine + Compose + git, firewalls everything but
# SSH (the UIs stay on 127.0.0.1; reach them through an SSH tunnel), and starts the stack.
set -euo pipefail

REPO_DIR="${REPO_DIR:-$HOME/wikipulse}"
cd "$REPO_DIR"

[ -f .env ] || { echo "missing $REPO_DIR/.env: copy it from your machine first (deploy/README.md)"; exit 1; }
[ -f secrets/deploy_key ] || echo "note: secrets/deploy_key missing; dashboard publishing will stay local"

echo "==> packages"
sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates curl git make python3-venv ufw >/dev/null

echo "==> Docker Engine + Compose plugin (Docker's own apt repository)"
if ! command -v docker >/dev/null; then
  sudo install -m 0755 -d /etc/apt/keyrings
  sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  sudo chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
  sudo apt-get update -qq
  sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin >/dev/null
  sudo usermod -aG docker "$USER"
fi

echo "==> firewall: SSH only"
sudo ufw default deny incoming >/dev/null
sudo ufw default allow outgoing >/dev/null
sudo ufw allow OpenSSH >/dev/null
sudo ufw --force enable >/dev/null
# Oracle's Ubuntu images also ship iptables REJECT rules; ufw manages its own chains, and
# nothing but SSH needs to be reachable, so those rules can stay.

echo "==> permissions"
chmod 600 .env secrets/deploy_key 2>/dev/null || true
grep -q '^AIRFLOW_UID=' .env && sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
mkdir -p warehouse dashboard/data

echo "==> start the stack (first build downloads Spark jars; ~10-15 minutes)"
# `sg docker` runs the commands with the docker group the usermod above just granted.
sg docker -c "docker compose up -d --build"
sg docker -c "docker compose ps --format '{{.Service}}: {{.Status}}'"

cat <<'EOF'

Done. From your own machine, open the UIs through an SSH tunnel:
    ssh -N -L 8080:127.0.0.1:8080 -L 3000:127.0.0.1:3000 ubuntu@<VM_IP>
then browse http://127.0.0.1:8080 (Airflow) and http://127.0.0.1:3000 (Grafana).
EOF
