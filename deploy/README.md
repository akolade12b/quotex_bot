# Deploy the bot to a 24/7 VPS (Oracle Cloud Free Tier)

Run the bot continuously — starts on boot, auto-restarts on crash, logs to
journald — on an **Oracle Cloud "Always Free"** instance for $0/month.

- `deploy.sh` — one-line deploy: sync code + install deps + install/start systemd service.
- `quotex-bot.service` — the systemd unit (template; `deploy.sh` fills in user/paths).

The bot is **outbound-only** (Telegram MTProto, Quotex WebSocket, HTTPS polling),
so the VPS needs no inbound ports beyond SSH.

---

## 0. One-time: create the VPS

1. Sign up at **cloud.oracle.com** (Always Free tier — needs a card to verify, never charged).
2. **Compute → Instances → Create instance**:
   - Image: **Ubuntu 24.04** (Python 3.12 — the bot requires ≥ 3.10).
   - Shape: **VM.Standard.A1.Flex (Ampere/ARM, 4 OCPU/24 GB free)** if available in your region,
     otherwise **E2.1.Micro** (1 GB RAM — still plenty).
   - SSH keys: add a public key. **Download the matching private key** (that's your login credential).
3. Launch, wait for **Running**, copy the **public IP**.

## 1. One-time: SSH access from your machine

```bash
ssh-copy-id -i ~/.ssh/<your-key>.pub ubuntu@<PUBLIC_IP>
ssh ubuntu@<PUBLIC_IP>     # confirm it logs in without a password
```

(If you generated the key inside Oracle's console, point ssh at the downloaded key with `-i`, or add a `Host` block in `~/.ssh/config`.)

## 2. Deploy

From **your local machine**, at the repo root. **Stop the local bot first**
(`Ctrl+C` on the running process) so the Telegram session file is copied cleanly
and it isn't fighting the server for the same session:

```bash
./deploy/deploy.sh ubuntu@<PUBLIC_IP>
```

That single command:
1. **rsyncs** the project to `~/quotex_signal_bot` on the server (skips `.venv`,
   `.claude/`, caches, `debug/`).
2. **Copies `.env` and `data/userbot.session`** (credentials + the Telegram login,
   so the server auto-authenticates with no prompt) — **only when the server
   doesn't have them yet**. The local `data/bot.db` is **not** copied: the server
   starts a clean trade history (safer than shipping a mid-write SQLite file).
3. On the server: installs `python3`/`python3-venv` if missing, creates `.venv`,
   `pip install -r requirements.txt`.
4. Installs `deploy/quotex-bot.service` as `/etc/systemd/system/quotex-bot.service`
   (substituting the real user + path), then `enable --now` (starts now, auto-starts on boot).
5. Prints `systemctl status`.

> To force the server to take your local `.env`/`data/` wholesale (fresh server,
> or you want your trade history pushed), run with `FORCE_STATE=1` — it
> **overwrites** the server's files, including the live session and `bot.db`.

## 3. Verify it's running

```bash
ssh ubuntu@<PUBLIC_IP> 'systemctl status quotex-bot'
ssh ubuntu@<PUBLIC_IP> 'journalctl -u quotex-bot -f'
```

You should see `Active: active (running)`, `broker connected: LiveQuotexClient`,
`group monitor configured for 2 link(s)`, and `group monitor listening on ...`.

## 4. First run / Telegram login

The bot's Telegram *bot* needs no login, but the **userbot group monitor** does.

- **Normal case:** you already logged in locally (the `data/userbot.session` file exists).
  The deploy script copies it on the first deploy, so the server auto-authenticates — **no prompt**.
- **If there's no session file:** the monitor can't start. Create one once, interactively:

  ```bash
  ssh ubuntu@<PUBLIC_IP>
  sudo apt install -y tmux
  tmux new -s login
  cd ~/quotex_signal_bot
  .venv/bin/python -m app.main     # enter the Telegram code when prompted
  ```
  When you see the bot running, `Ctrl+B` then `D` to detach, then `Ctrl+C` won't work —
  stop it with `sudo systemctl restart quotex-bot` after detaching (the session file now exists).

> ⚠️ A `.session` can only be **actively used in one place at a time**. Don't also run the bot
> locally while it's live on the VPS, or one of them gets disconnected.

## 5. Day-to-day ops

| Task | Command (on your local machine) |
|---|---|
| Push new code | `./deploy/deploy.sh ubuntu@<PUBLIC_IP>` (restarts the service) |
| Watch live logs | `ssh ubuntu@<PUBLIC_IP> 'journalctl -u quotex-bot -f'` |
| Status | `ssh ubuntu@<PUBLIC_IP> 'systemctl status quotex-bot'` |
| Restart | `ssh ubuntu@<PUBLIC_IP> 'sudo systemctl restart quotex-bot'` |
| Stop | `ssh ubuntu@<PUBLIC_IP> 'sudo systemctl stop quotex-bot'` |
| Reboot test | `ssh ubuntu@<PUBLIC_IP> 'sudo reboot'` — service must come back on its own |
| Edit `.env` | `ssh ubuntu@<PUBLIC_IP> 'nano ~/quotex_signal_bot/.env'` then restart |
| Backup state | `scp ubuntu@<PUBLIC_IP>:~/quotex_signal_bot/data/bot.db data/` |

The server is the source of truth for `.env`, `data/bot.db`, and the live
`.session` — **edit those on the server**, not locally. A re-deploy pushes code
only and leaves server state alone, except `FORCE_STATE=1` which overwrites it.

## 6. Security checklist

- `.env` and `data/*.session` are `chmod 600` by the deploy script (only your user can read).
- Keep the Oracle SSH private key private — anyone with it controls the box and its credentials.
- The VPS holds real Telegram credentials (`api_id`/`api_hash`/phone) and Quotex tokens —
  treat the instance as sensitive, don't share it.
- The userbot keeps your personal Telegram account connected 24/7; a ban risk exists.
  Prefer a spare account.

## 7. Troubleshooting

- **`rsync: command not found`** (local): `sudo apt install -y rsync`.
- **`Connection refused` on SSH:** instance still booting, or wrong IP/security-list — confirm
  port 22 is open (default Oracle VCN allows it) and the IP matches the console.
- **Service exits immediately:** read the journal — `journalctl -u quotex-bot -n 100`.
  Common cause on a fresh server: **no `.session`** (see step 4), or a stale `QSB_DEMO_SSID`.
- **Quotex `HTTP 429`**: rate-limiting on the first WebSocket connect — the client retries and
  usually succeeds; if it persists, wait a minute between restarts.
- **Session invalidated:** if you ever run locally again with the same `.session`, delete
  `data/userbot.session` on the server and re-run step 4 to re-login.
