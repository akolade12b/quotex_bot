#!/usr/bin/env bash
#
# One-shot deploy: sync this project to a remote host over rsync/ssh, install
# dependencies, and install + start the bot as a systemd service. Idempotent —
# re-run any time to push code changes. The server's own .env / session are
# preserved across re-deploys (see FORCE_STATE below).
#
# Usage:
#   ./deploy/deploy.sh <user@host> [remote-dir]
#
# Examples:
#   ./deploy/deploy.sh ubuntu@203.0.113.10
#   ./deploy/deploy.sh ubuntu@203.0.113.10 my-bot
#
# Env overrides:
#   SSH_PORT=2222     non-default ssh port
#   FORCE_STATE=1     overwrite the server's .env AND data/ (incl. bot.db and
#                     the live Telegram session) with local copies.
#                     DANGEROUS — only for a freshly-created server.
#
# Local requirements: rsync + ssh, passwordless login to the host
# (ssh-copy-id <user>@<host> once, or an entry in ~/.ssh/config).

set -euo pipefail

cd "$(dirname "$0")/.."                     # repo root

HOST="${1:?usage: deploy.sh <user@host> [remote-dir]}"
REMOTE_DIR="${2:-quotex_signal_bot}"
PORT="${SSH_PORT:-22}"

echo "==> syncing project to ${HOST}:${REMOTE_DIR}/"
rsync -az --delete -e "ssh -p ${PORT}" \
  --exclude '.venv' \
  --exclude '.claude' \
  --exclude '.pytest_cache' \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  --exclude 'debug' \
  ./ "${HOST}:${REMOTE_DIR}/"

if [ "${FORCE_STATE:-0}" = "1" ]; then
  echo "==> FORCE_STATE=1 — overwriting server .env / data/ with local copies"
  rsync -az -e "ssh -p ${PORT}" .env "${HOST}:${REMOTE_DIR}/"
  rsync -az -e "ssh -p ${PORT}" data/ "${HOST}:${REMOTE_DIR}/data/"
else
  # Default: ship .env + the Telethon .session only when the server has none.
  # The local bot.db (SQLite WAL sidecars could be mid-write, and the server
  # should start a clean trade history) is NOT shipped.
  echo "==> syncing state files (server keeps its own .env / session unless new)"
  rsync -az -e "ssh -p ${PORT}" --ignore-existing .env "${HOST}:${REMOTE_DIR}/"
  rsync -az -e "ssh -p ${PORT}" --ignore-existing \
    --include '*/' --include '*.session*' --exclude '*' \
    data/ "${HOST}:${REMOTE_DIR}/data/"
fi

echo "==> remote setup (python, venv, systemd, restart)"
ssh -p "${PORT}" "${HOST}" bash -s -- "${REMOTE_DIR}" <<'REMOTE'
set -euo pipefail

REMOTE_DIR="$1"
DIR="$HOME/$REMOTE_DIR"
cd "$DIR"

# Ensure python3 + venv exist (idempotent; apt only runs when something's missing).
need=0
command -v python3 >/dev/null 2>&1 || need=1
python3 -m venv --help >/dev/null 2>&1 || need=1
if [ "$need" -eq 1 ]; then
  echo "==> installing python3 + python3-venv"
  sudo apt-get update
  sudo apt-get install -y python3 python3-venv
fi

echo "==> python environment"
[ -d .venv ] || python3 -m venv .venv
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements.txt

echo "==> protecting secrets"
chmod 600 .env 2>/dev/null || true
chmod 600 data/*.session 2>/dev/null || true

echo "==> installing systemd unit"
sudo sed "s|__USER__|$(whoami)|g; s|__DIR__|$DIR|g" \
  deploy/quotex-bot.service \
  | sudo tee /etc/systemd/system/quotex-bot.service >/dev/null

sudo systemctl daemon-reload
sudo systemctl enable quotex-bot
sudo systemctl restart quotex-bot

echo "==> service status"
systemctl --no-pager --lines=15 status quotex-bot || true

if ! ls data/*.session >/dev/null 2>&1; then
  echo
  echo "WARNING: no Telethon session on the server — the userbot monitor will"
  echo "not start until you create one. See deploy/README.md 'First run'."
fi
REMOTE

echo
echo "==> deploy complete"
echo "    live logs: ssh -p ${PORT} ${HOST} 'journalctl -u quotex-bot -f'"
echo "    status   : ssh -p ${PORT} ${HOST} 'systemctl status quotex-bot'"
