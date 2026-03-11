"""
E-Fence Edge Agent — SQLite Buffer
=====================================
Persists Wi-Fi and Bluetooth observations locally so records survive
reboots and network outages. Records accumulate here until the uploader
flushes them to Databricks.

Schema:
  observations(id, protocol, payload JSON, created_at, uploaded_at)

WAL mode is enabled so the scanner and uploader threads can read/write
concurrently without blocking each other.
"""

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

log = logging.getLogger("efence.buffer")


class Buffer:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db_path = db_path
        self._lock    = threading.Lock()
        self._init_db()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self):
        with self._lock:
            conn = self._connect()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS observations (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    protocol    TEXT    NOT NULL,       -- 'wifi' or 'bt'
                    payload     TEXT    NOT NULL,       -- JSON string
                    created_at  TEXT    NOT NULL,
                    uploaded_at TEXT    DEFAULT NULL    -- NULL = pending
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pending ON observations(protocol, uploaded_at)")
            conn.commit()
            conn.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def put_wifi(self, records: List[dict]) -> int:
        """Insert Wi-Fi records into the buffer. Returns count inserted."""
        return self._insert("wifi", records)

    def put_bt(self, records: List[dict]) -> int:
        """Insert Bluetooth records into the buffer. Returns count inserted."""
        return self._insert("bt", records)

    def _insert(self, protocol: str, records: List[dict]) -> int:
        if not records:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        rows = [(protocol, json.dumps(r, separators=(",", ":")), now)
                for r in records]
        with self._lock:
            conn = self._connect()
            conn.executemany(
                "INSERT INTO observations (protocol, payload, created_at) VALUES (?,?,?)",
                rows,
            )
            conn.commit()
            conn.close()
        log.debug("Buffered %d %s records", len(records), protocol)
        return len(records)

    def get_pending(self, protocol: str, limit: int = 500) -> List[Tuple[int, dict]]:
        """
        Return up to `limit` pending (un-uploaded) records for a protocol.
        Returns list of (row_id, record_dict) tuples.
        """
        with self._lock:
            conn = self._connect()
            rows = conn.execute(
                "SELECT id, payload FROM observations "
                "WHERE protocol=? AND uploaded_at IS NULL "
                "ORDER BY id ASC LIMIT ?",
                (protocol, limit),
            ).fetchall()
            conn.close()
        return [(row_id, json.loads(payload)) for row_id, payload in rows]

    def mark_uploaded(self, row_ids: List[int]):
        """Mark records as successfully uploaded."""
        if not row_ids:
            return
        now = datetime.now(timezone.utc).isoformat()
        placeholders = ",".join("?" * len(row_ids))
        with self._lock:
            conn = self._connect()
            conn.execute(
                f"UPDATE observations SET uploaded_at=? WHERE id IN ({placeholders})",
                [now] + row_ids,
            )
            conn.commit()
            conn.close()
        log.debug("Marked %d records as uploaded", len(row_ids))

    def purge_old(self, retain_days: int = 3):
        """
        Delete uploaded records older than retain_days.
        Keeps the database from growing unboundedly on a Pi SD card.
        """
        with self._lock:
            conn = self._connect()
            result = conn.execute(
                "DELETE FROM observations "
                "WHERE uploaded_at IS NOT NULL "
                "AND created_at < datetime('now', ?)",
                (f"-{retain_days} days",),
            )
            deleted = result.rowcount
            conn.commit()
            conn.close()
        if deleted:
            log.info("Purged %d old uploaded records", deleted)

    def stats(self) -> dict:
        """Return pending/uploaded counts per protocol for monitoring."""
        with self._lock:
            conn = self._connect()
            rows = conn.execute("""
                SELECT protocol,
                       SUM(CASE WHEN uploaded_at IS NULL THEN 1 ELSE 0 END) as pending,
                       SUM(CASE WHEN uploaded_at IS NOT NULL THEN 1 ELSE 0 END) as uploaded
                FROM observations
                GROUP BY protocol
            """).fetchall()
            conn.close()
        return {r[0]: {"pending": r[1], "uploaded": r[2]} for r in rows}
