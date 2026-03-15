import os
import sqlite3
from pathlib import Path
from utils.logger import logger

DB_PATH = os.environ.get("ALGOSOFT_DB_PATH", "config/algosoft.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT DEFAULT 'client',
    is_active INTEGER DEFAULT 0,
    subscription_tier TEXT DEFAULT 'FREE',
    max_brokers INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),
    activated_at TEXT,
    activated_by INTEGER
);

CREATE TABLE IF NOT EXISTS data_providers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL UNIQUE,
    api_key_encrypted TEXT,
    access_token_encrypted TEXT,
    status TEXT DEFAULT 'not_configured',
    updated_at TEXT DEFAULT (datetime('now')),
    updated_by INTEGER
);

CREATE TABLE IF NOT EXISTS client_broker_instances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL REFERENCES users(id),
    broker TEXT NOT NULL,
    instance_label TEXT,
    api_key_encrypted TEXT,
    api_secret_encrypted TEXT,
    access_token_encrypted TEXT,
    token_updated_at TEXT,
    trading_mode TEXT DEFAULT 'paper',
    instrument TEXT DEFAULT 'NIFTY',
    quantity INTEGER DEFAULT 25,
    strategy_version TEXT DEFAULT 'V3',
    status TEXT DEFAULT 'idle',
    bot_pid INTEGER,
    last_heartbeat TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(client_id, broker)
);

CREATE TABLE IF NOT EXISTS trade_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id INTEGER NOT NULL REFERENCES client_broker_instances(id),
    client_id INTEGER NOT NULL REFERENCES users(id),
    trade_type TEXT,
    direction TEXT,
    strike INTEGER,
    entry_price REAL,
    exit_price REAL,
    pnl_pts REAL,
    pnl_rs REAL,
    quantity INTEGER,
    broker TEXT,
    exit_reason TEXT,
    instrument TEXT,
    trading_mode TEXT,
    opened_at TEXT,
    closed_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS order_failures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id INTEGER NOT NULL REFERENCES client_broker_instances(id),
    client_id INTEGER NOT NULL REFERENCES users(id),
    order_side TEXT,
    broker_error TEXT,
    failure_reason TEXT,
    retry_attempt INTEGER DEFAULT 0,
    paired_leg_closed INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id INTEGER,
    actor_role TEXT,
    action TEXT,
    target_type TEXT,
    target_id INTEGER,
    details TEXT,
    ip_address TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS broker_change_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL REFERENCES users(id),
    current_broker TEXT NOT NULL,
    requested_broker TEXT NOT NULL,
    reason TEXT DEFAULT '',
    status TEXT DEFAULT 'pending',
    created_at TEXT DEFAULT (datetime('now')),
    resolved_at TEXT,
    resolved_by_id INTEGER
);
"""

_conn = None


def get_db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA foreign_keys=ON")
        _migrate(_conn)
        _seed(_conn)
        logger.info(f"[DB] SQLite connected: {DB_PATH}")
    return _conn


def _migrate(conn: sqlite3.Connection):
    conn.executescript(SCHEMA)
    existing = [row[1] for row in conn.execute("PRAGMA table_info(client_broker_instances)").fetchall()]
    if "api_secret_encrypted" not in existing:
        conn.execute("ALTER TABLE client_broker_instances ADD COLUMN api_secret_encrypted TEXT")
    if "token_updated_at" not in existing:
        conn.execute("ALTER TABLE client_broker_instances ADD COLUMN token_updated_at TEXT")
    conn.commit()


def _seed(conn: sqlite3.Connection):
    from web.auth import hash_password
    existing = conn.execute("SELECT id FROM users WHERE role='admin'").fetchone()
    if not existing:
        ph = hash_password("Admin@123")
        conn.execute(
            "INSERT INTO users (username, email, password_hash, role, is_active) VALUES (?,?,?,?,?)",
            ("admin", "admin@algosoft.com", ph, "admin", 1)
        )
        conn.commit()
        logger.info("[DB] Default admin created (admin / Admin@123)")

    conn.execute(
        "INSERT OR IGNORE INTO data_providers (provider, status) VALUES ('upstox', 'not_configured')"
    )
    conn.commit()


def db_fetchone(sql: str, params=()):
    row = get_db().execute(sql, params).fetchone()
    return dict(row) if row else None


def db_fetchall(sql: str, params=()):
    rows = get_db().execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def db_execute(sql: str, params=()):
    conn = get_db()
    cur = conn.execute(sql, params)
    conn.commit()
    return cur.lastrowid
