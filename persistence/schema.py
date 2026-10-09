"""SQLite schema. One DDL string, executed idempotently on connect."""

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('call', 'put')),
    expiry_s INTEGER NOT NULL,
    entry_time TEXT,
    gale_count INTEGER NOT NULL DEFAULT 0,
    raw_text TEXT NOT NULL,
    source TEXT,
    received_at TEXT NOT NULL,
    parse_status TEXT NOT NULL DEFAULT 'parsed',
    reject_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_signals_received_at ON signals(received_at);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_trade_id TEXT NOT NULL UNIQUE,
    signal_id INTEGER REFERENCES signals(id),
    asset TEXT NOT NULL,
    direction TEXT NOT NULL,
    amount REAL NOT NULL,
    entry_price REAL NOT NULL,
    expiry_s INTEGER NOT NULL,
    level INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'open',
    opened_at TEXT NOT NULL,
    expires_at TEXT,
    closed_at TEXT,
    exit_price REAL,
    payout_pct REAL NOT NULL DEFAULT 0.85,
    pnl REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_asset ON trades(asset);
CREATE INDEX IF NOT EXISTS idx_trades_opened_at ON trades(opened_at);

CREATE TABLE IF NOT EXISTS balance_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    balance REAL NOT NULL,
    equity REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    day TEXT NOT NULL,
    realized_loss REAL NOT NULL DEFAULT 0,
    trade_count INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO risk_state (id, day, realized_loss, trade_count) VALUES (1, '', 0, 0);
"""
