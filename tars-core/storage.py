"""TARS Core v5.0.0 — Storage layer

SQLite-backed append-only event store for post-mortem debugging,
pattern learning, and historical analytics.

Four databases, each in /data/:
  - events.sqlite    — raw HA state changes (high volume, 30-day retention)
  - decisions.sqlite — TARS decision log (low volume, retained forever)
  - anomalies.sqlite — detected anomalies with dedup + resolution tracking
  - modes.sqlite     — mode state transitions

Public API:
  init_storage()                         — create schemas on boot
  record_event(entity_id, old, new, ...) — append event
  record_decision(kind, actions, ...)    — append decision
  record_anomaly(type, entity, severity, msg, ...) — append anomaly (dedup)
  record_mode(old, new, reason)          — append mode transition
  query_events(since=, entity=, limit=)  — retrieve events
  query_decisions(hours=, kind=, limit=) — retrieve decisions
  query_anomalies(entity=, days=, ...)   — historical anomalies
  anomaly_rate(entity, hours=24)         — how often this entity anomalies
  prune_old(days=30)                     — cleanup helper (call from daily cron)
"""
import os
import sqlite3
import time
import json
import threading
from datetime import datetime, timedelta
from contextlib import contextmanager

DATA_DIR = os.environ.get('TARS_DATA_DIR', '/data')

# Per-database lock: SQLite handles concurrent reads but we serialize writes
_locks = {
    'events': threading.Lock(),
    'decisions': threading.Lock(),
    'anomalies': threading.Lock(),
    'modes': threading.Lock(),
}


@contextmanager
def _conn(db_name):
    """Get a connection to named db with 5s timeout."""
    path = os.path.join(DATA_DIR, f'{db_name}.sqlite')
    conn = sqlite3.connect(path, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_storage():
    """Initialize all databases and create schemas if missing."""
    os.makedirs(DATA_DIR, exist_ok=True)

    with _conn('events') as c:
        c.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,             -- unix ms
                entity_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                old_state TEXT,
                new_state TEXT,
                classification TEXT,             -- significant|noise|unknown
                attributes TEXT                  -- JSON blob of changed attributes
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_events_entity ON events(entity_id, ts DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_events_domain ON events(domain, ts DESC)')

    with _conn('decisions') as c:
        c.execute('''
            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                kind TEXT NOT NULL,              -- mode_change|focus_mode|sonos_follow|...
                source TEXT NOT NULL,            -- motion_event|calendar|mode_machine|...
                why TEXT,                        -- human-readable explanation
                actions TEXT,                    -- JSON list of {action, reason}
                outcome TEXT                     -- success|failed|deferred
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_decisions_ts ON decisions(ts DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_decisions_kind ON decisions(kind, ts DESC)')

    with _conn('anomalies') as c:
        c.execute('''
            CREATE TABLE IF NOT EXISTS anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                count INTEGER DEFAULT 1,
                dedup_key TEXT UNIQUE NOT NULL,  -- entity|type|severity
                entity_id TEXT NOT NULL,
                type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT,
                resolved_at INTEGER,
                resolved_reason TEXT
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_last_seen ON anomalies(last_seen DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_entity ON anomalies(entity_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_unresolved ON anomalies(resolved_at) WHERE resolved_at IS NULL')

    with _conn('modes') as c:
        c.execute('''
            CREATE TABLE IF NOT EXISTS modes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                from_mode TEXT NOT NULL,
                to_mode TEXT NOT NULL,
                reason TEXT,                     -- why the transition fired
                duration_sec INTEGER              -- time spent in from_mode
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_modes_ts ON modes(ts DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_modes_to ON modes(to_mode, ts DESC)')


# ─────────────────── Events ───────────────────

def record_event(entity_id: str, old_state: str, new_state: str,
                 classification: str = 'unknown', attributes: dict = None):
    """Append a state change to the event store."""
    if not entity_id: return
    ts = int(time.time() * 1000)
    domain = entity_id.split('.')[0] if '.' in entity_id else entity_id
    attrs_json = json.dumps(attributes) if attributes else None
    with _locks['events']:
        with _conn('events') as c:
            c.execute('''INSERT INTO events
                (ts, entity_id, domain, old_state, new_state, classification, attributes)
                VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (ts, entity_id, domain, str(old_state) if old_state is not None else None,
                 str(new_state) if new_state is not None else None,
                 classification, attrs_json))


def query_events(since_ms: int = None, entity: str = None, domain: str = None,
                 limit: int = 100):
    """Retrieve events. Times in unix ms."""
    q = 'SELECT * FROM events WHERE 1=1'
    args = []
    if since_ms is not None:
        q += ' AND ts >= ?'; args.append(since_ms)
    if entity:
        q += ' AND entity_id = ?'; args.append(entity)
    if domain:
        q += ' AND domain = ?'; args.append(domain)
    q += ' ORDER BY ts DESC LIMIT ?'; args.append(limit)
    with _conn('events') as c:
        rows = c.execute(q, args).fetchall()
    return [dict(r) for r in rows]


# ─────────────────── Decisions ───────────────────

def record_decision(kind: str, source: str, actions: list, why: str = None,
                    outcome: str = 'success'):
    """Append a decision to the log."""
    ts = int(time.time() * 1000)
    actions_json = json.dumps(actions or [])
    with _locks['decisions']:
        with _conn('decisions') as c:
            c.execute('''INSERT INTO decisions
                (ts, kind, source, why, actions, outcome)
                VALUES (?, ?, ?, ?, ?, ?)''',
                (ts, kind, source, why, actions_json, outcome))


def query_decisions(hours: int = 24, kind: str = None, source: str = None,
                    limit: int = 100):
    """Retrieve recent decisions."""
    since_ms = int((time.time() - hours * 3600) * 1000)
    q = 'SELECT * FROM decisions WHERE ts >= ?'
    args = [since_ms]
    if kind:
        q += ' AND kind = ?'; args.append(kind)
    if source:
        q += ' AND source = ?'; args.append(source)
    q += ' ORDER BY ts DESC LIMIT ?'; args.append(limit)
    with _conn('decisions') as c:
        rows = c.execute(q, args).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try: d['actions'] = json.loads(d['actions']) if d['actions'] else []
        except: pass
        out.append(d)
    return out


# ─────────────────── Anomalies ───────────────────

def record_anomaly(entity_id: str, atype: str, severity: str, message: str,
                   dedup_window_sec: int = 3600) -> dict:
    """Record anomaly with deduplication.
    If same entity+type seen within dedup_window, increment count instead of insert.
    Returns the final anomaly record as dict."""
    ts = int(time.time() * 1000)
    dedup_key = f'{entity_id}|{atype}|{severity}'
    with _locks['anomalies']:
        with _conn('anomalies') as c:
            existing = c.execute(
                'SELECT id, count, last_seen FROM anomalies WHERE dedup_key = ? AND resolved_at IS NULL',
                (dedup_key,)
            ).fetchone()
            if existing:
                # Update existing
                c.execute('''UPDATE anomalies SET count = count + 1, last_seen = ?,
                              message = ? WHERE id = ?''',
                    (ts, message, existing['id']))
                aid = existing['id']
            else:
                cur = c.execute('''INSERT INTO anomalies
                    (first_seen, last_seen, dedup_key, entity_id, type, severity, message)
                    VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (ts, ts, dedup_key, entity_id, atype, severity, message))
                aid = cur.lastrowid
            row = c.execute('SELECT * FROM anomalies WHERE id = ?', (aid,)).fetchone()
    return dict(row)


def resolve_anomaly(anomaly_id: int, reason: str = 'auto'):
    """Mark an anomaly resolved."""
    ts = int(time.time() * 1000)
    with _locks['anomalies']:
        with _conn('anomalies') as c:
            c.execute('UPDATE anomalies SET resolved_at = ?, resolved_reason = ? WHERE id = ?',
                      (ts, reason, anomaly_id))


def query_anomalies(entity: str = None, days: int = 7, unresolved_only: bool = False,
                    limit: int = 100):
    """Retrieve anomaly history."""
    since_ms = int((time.time() - days * 86400) * 1000)
    q = 'SELECT * FROM anomalies WHERE last_seen >= ?'
    args = [since_ms]
    if entity:
        q += ' AND entity_id = ?'; args.append(entity)
    if unresolved_only:
        q += ' AND resolved_at IS NULL'
    q += ' ORDER BY last_seen DESC LIMIT ?'; args.append(limit)
    with _conn('anomalies') as c:
        rows = c.execute(q, args).fetchall()
    return [dict(r) for r in rows]


def anomaly_rate(entity: str, hours: int = 24) -> dict:
    """How many times this entity anomalied in the last N hours."""
    since_ms = int((time.time() - hours * 3600) * 1000)
    with _conn('anomalies') as c:
        row = c.execute('''SELECT COUNT(*) as events, SUM(count) as total_occurrences,
                            MAX(last_seen) as most_recent
                           FROM anomalies WHERE entity_id = ? AND last_seen >= ?''',
                        (entity, since_ms)).fetchone()
    return dict(row) if row else {'events': 0, 'total_occurrences': 0, 'most_recent': None}


# ─────────────────── Modes ───────────────────

def record_mode(old_mode: str, new_mode: str, reason: str = None,
                prev_duration_sec: int = None):
    """Append a mode transition."""
    ts = int(time.time() * 1000)
    with _locks['modes']:
        with _conn('modes') as c:
            c.execute('''INSERT INTO modes
                (ts, from_mode, to_mode, reason, duration_sec)
                VALUES (?, ?, ?, ?, ?)''',
                (ts, old_mode, new_mode, reason, prev_duration_sec))


def query_modes(days: int = 7, limit: int = 100):
    since_ms = int((time.time() - days * 86400) * 1000)
    with _conn('modes') as c:
        rows = c.execute('''SELECT * FROM modes WHERE ts >= ?
                            ORDER BY ts DESC LIMIT ?''',
                         (since_ms, limit)).fetchall()
    return [dict(r) for r in rows]


# ─────────────────── Maintenance ───────────────────

def prune_old(events_days: int = 30, decisions_days: int = 365,
              anomalies_days: int = 90, modes_days: int = 180):
    """Delete old records. Call from daily cron."""
    stats = {'events_deleted': 0, 'decisions_deleted': 0, 'anomalies_deleted': 0, 'modes_deleted': 0}
    now_ms = int(time.time() * 1000)

    if events_days is not None:
        cutoff = now_ms - events_days * 86400 * 1000
        with _locks['events']:
            with _conn('events') as c:
                r = c.execute('DELETE FROM events WHERE ts < ?', (cutoff,))
                stats['events_deleted'] = r.rowcount

    if decisions_days is not None:
        cutoff = now_ms - decisions_days * 86400 * 1000
        with _locks['decisions']:
            with _conn('decisions') as c:
                r = c.execute('DELETE FROM decisions WHERE ts < ?', (cutoff,))
                stats['decisions_deleted'] = r.rowcount

    if anomalies_days is not None:
        cutoff = now_ms - anomalies_days * 86400 * 1000
        with _locks['anomalies']:
            with _conn('anomalies') as c:
                r = c.execute('DELETE FROM anomalies WHERE last_seen < ?', (cutoff,))
                stats['anomalies_deleted'] = r.rowcount

    if modes_days is not None:
        cutoff = now_ms - modes_days * 86400 * 1000
        with _locks['modes']:
            with _conn('modes') as c:
                r = c.execute('DELETE FROM modes WHERE ts < ?', (cutoff,))
                stats['modes_deleted'] = r.rowcount

    # Vacuum all dbs after prune
    for db in ('events', 'decisions', 'anomalies', 'modes'):
        with _conn(db) as c:
            c.execute('VACUUM')

    return stats


def storage_stats() -> dict:
    """Return row counts + db file sizes for debugging."""
    out = {}
    for db in ('events', 'decisions', 'anomalies', 'modes'):
        path = os.path.join(DATA_DIR, f'{db}.sqlite')
        try:
            size = os.path.getsize(path) if os.path.exists(path) else 0
        except: size = 0
        try:
            with _conn(db) as c:
                # Get table name dynamically: db name matches table name
                table = db
                count = c.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        except: count = 0
        out[db] = {'rows': count, 'size_bytes': size, 'size_mb': round(size / 1048576, 2)}
    return out
