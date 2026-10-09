# Quotex Signal Bot

A Telegram bot that ingests **MagicTrader-style gale signals**, parses them,
and executes a **martingale (gale) ladder** on Quotex — against a deterministic
**simulated broker** first, with a gated live WebSocket client behind the same
`Broker` interface. Signals, trades, balances, and risk state persist to SQLite.

> ⚠️ **Simulation-first.** The default and recommended mode is `broker_mode=simulated`
> (paper trading with no real money). The live client speaks the modern Quotex
> Socket.IO protocol but is **not yet validated end-to-end against a real
> account**. **Run the simulated broker until you have validated the ladder and
> risk rules, then validate the live client on the demo account before any real
> money.**

## What it does

1. A signal channel forwards MagicTrader messages to the bot (you control the
   channel; the bot is a normal Telegram bot).
2. Each message is parsed into a `Signal`: asset, direction, expiry, entry time,
   and the gale ladder length.
3. The execution engine places level 0 at `base_amount`. On a **loss** it
   auto-places the next level at `amount × martingale_multiplier` (default 2×),
   up to `min(signal.gale_count, config.max_gales)`. A win ends the ladder.
4. Every placement passes the risk manager first: asset allowlist, per-trade
   and per-ladder stake caps, per-asset cooldown, max concurrent positions,
   balance check, and a daily loss limit.
5. All signals, trades, and risk state land in SQLite (`data/bot.db`).

## Signal format

The parser is tailored to the MagicTrader gale format:

```
💰5-minute expiration
USD/IDR; 08:55; PUT 🟥
🕐 TIME TO 09:00
1st GALE —> TIME TO 09:05
2nd GALE —> TIME TO 09:10
```

| Field | Parsed as |
|---|---|
| `5-minute` | expiry = 300 s |
| `USD/IDR` | asset |
| `PUT` (or SELL/DOWN/🟥/🔴) | direction → **put**; CALL/BUY/UP/🟩/🟢 → **call** |
| `08:55` | entry time (wall-clock); `entry_time` is **None** ⇒ execute immediately |
| `1st GALE` / `2nd GALE` | gale_count = 2 (max ordinal; default 0) |

`entry_time is None` (no time, or signals fed via CLI/tests) executes immediately.

## Install

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env        # then edit
```

## Configure

All configuration is via `QSB_*` env vars (loaded from `.env`). Key ones:

| Variable | Default | Meaning |
|---|---|---|
| `QSB_BROKER_MODE` | `simulated` | `simulated` \| `quotex` (live) |
| `QSB_TELEGRAM_TOKEN` | *(empty)* | Bot token; empty = no Telegram polling |
| `QSB_ALLOWED_CHAT_IDS` | *(empty)* | Comma list of chat ids allowed to feed signals |
| `QSB_BASE_AMOUNT` | `10` | Level-0 stake |
| `QSB_MARTINGALE_MULTIPLIER` | `2.0` | Gale stake multiplier |
| `QSB_MAX_GALES` | `2` | Hard cap on ladder length |
| `QSB_MAX_STAKE` | `100` | Per-trade stake cap |
| `QSB_MAX_LADDER_STAKE` | `0` | 0 = unlimited cumulative ladder stake |
| `QSB_MAX_DAILY_LOSS` | `200` | Stop trading once realized losses hit this |
| `QSB_MAX_CONCURRENT_POSITIONS` | `3` | Open-position cap |
| `QSB_ASSET_ALLOWLIST` | *(empty)* | Comma list; empty = all assets |
| `QSB_COOLDOWN_SECONDS` | `5` | Min gap between new signals on one asset |
| `QSB_TIME_SCALE` | `1.0` | Accelerate simulated expiries (`>1` for tests) |
| `QSB_SEED` | `0` | Simulated price-path seed (determinism) |
| `QSB_QUOTEX_MODE` | `demo` | Account type to connect as when live: `demo` \| `live` |
| `QSB_SSID` | *(empty)* | Quotex session token (browser `window.settings.token`) |
| `QSB_DEMO_SSID` | *(empty)* | Demo-account token (used by `/mode demo`) |
| `QSB_LIVE_SSID` | *(empty)* | Live-account token (used by `/mode live`) |
| `QSB_EMAIL` / `QSB_PASSWORD` | *(empty)* | Optional auto-login fallback (cloudscraper, Playwright) |

## Run

**Bot mode** (parses forwarded Telegram messages forever):

```bash
.venv/bin/python -m app.main
```

In bot mode you can also query the running connection from Telegram:

| Command | Effect |
|---|---|
| `/balance` | Report current mode and account balance |
| `/mode` | Report current mode (no argument) |
| `/mode demo` / `/mode live` | Reconnect the live client to the demo/live account |

`/mode` refuses to switch while trades are open. Only chat ids in
`QSB_ALLOWED_CHAT_IDS` may use these commands.

**Offline feed** (feed one signal, watch the ladder, print the trade table, exit):

```bash
.venv/bin/python -m app.main --signal "💰5-minute expiration
USD/IDR; 08:55; PUT 🟥
🕐 TIME TO 09:00
1st GALE —> TIME TO 09:05
2nd GALE —> TIME TO 09:10"
```

**Benchmark** (replay a synthetic day through the full pipeline):

```bash
.venv/bin/python -m benchmarks.simulate_day --signals 30
```

## Tests

```bash
.venv/bin/python -m pytest tests/ -q
```

91 tests cover the parser (including the exact sample above), simulated broker
determinism, repository roundtrips, every risk rule, the martingale ladder
(win/loss/gale-cap/daily-loss-limit paths), signal dedup, the Socket.IO wire
framing, the live client's offline logic, the `/mode`/`/balance` command
handlers, and the full handler→queue→engine→broker pipeline.

## Layout

```
app/               config (QSB_* env) + entrypoint (bot / offline feed)
broker/base.py     Broker ABC + domain types — everything is broker-agnostic
broker/simulated.py  deterministic GBM paper broker (seed, time_scale, test hooks)
broker/protocol.py   ONLY file that knows the Quotex Socket.IO wire format
broker/login.py      cloudscraper/Playwright login (email+password fallback)
broker/live_client.py gated live client (modern protocol; demo-first)
execution/         RiskManager (authorize/cooldown/daily-loss) + ExecutionEngine (ladder)
persistence/       SQLite schema, models, async repository
telegram_signals/  Signal model, MagicTrader parser, Telegram handler + bot
benchmarks/        simulate_day.py — session replay + stats
```

## Live Quotex — demo first, then live

The live client speaks the modern Quotex Socket.IO protocol (EIO=3,
`orders/open`, binary `s_balance`/`s_orders` events) verified against a current
reference implementation. It has **not** been validated against a real account
in this project, so treat it as untested until proven otherwise:

1. Set `QSB_BROKER_MODE=quotex`.
2. Provide credentials — either paste a session token from a logged-in browser
   (`window.settings.token` on the trade page) as `QSB_SSID`, or a per-account
   token (`QSB_DEMO_SSID` / `QSB_LIVE_SSID`), or set `QSB_EMAIL`/`QSB_PASSWORD`
   for automated login.
3. Start with `QSB_QUOTEX_MODE=demo` and `QSB_DEMO_SSID` set. From Telegram,
   send `/balance` to confirm the demo connection and balance, then `/mode live`
   only when you are deliberately switching to real money.
4. The bot starts in whatever mode `QSB_QUOTEX_MODE` names, so **never set
   `QSB_QUOTEX_MODE=live` for the initial validation** — use demo and switch
   manually. If a connection cannot be established, the bot exits with a clear
   error rather than silently trading on the wrong account.

## Risk warning

Martingale-style ladders can lose the full ladder stake quickly. The 85%
payout against fair 50/50 odds is a negative-expectancy game; the sim shows
this plainly. Use the risk limits (`max_stake`, `max_ladder_stake`,
`max_daily_loss`, `max_gales`) and understand that **no configuration makes a
martingale profitable** — it only changes the loss profile.
