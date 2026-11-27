"""
Persistent Log Storage Engine with SQLite & In-Memory WAL.
Guarantees durable state transitions (current_term, voted_for, log entries).
"""

import sqlite3
import json
import threading
from typing import List, Optional, Tuple
from .protocol import LogEntry

class Storage:
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._init_db()

    def _init_db(self):
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    val TEXT
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS log_entries (
                    idx INTEGER PRIMARY KEY,
                    term INTEGER,
                    command TEXT
                )
            """)
            self._conn.commit()

    def get_metadata(self) -> Tuple[int, Optional[str]]:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT val FROM metadata WHERE key = 'current_term'")
            row_term = cursor.fetchone()
            current_term = int(row_term[0]) if row_term else 0

            cursor.execute("SELECT val FROM metadata WHERE key = 'voted_for'")
            row_voted = cursor.fetchone()
            voted_for = row_voted[0] if (row_voted and row_voted[0]) else None

            return current_term, voted_for

    def set_metadata(self, current_term: int, voted_for: Optional[str]):
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO metadata (key, val) VALUES ('current_term', ?)", (str(current_term),))
            self._conn.execute("INSERT OR REPLACE INTO metadata (key, val) VALUES ('voted_for', ?)", (voted_for or "",))
            self._conn.commit()

    def append_entries(self, entries: List[LogEntry]):
        if not entries:
            return
        with self._lock:
            for e in entries:
                self._conn.execute(
                    "INSERT OR REPLACE INTO log_entries (idx, term, command) VALUES (?, ?, ?)",
                    (e.index, e.term, json.dumps(e.command))
                )
            self._conn.commit()

    def truncate_from(self, from_index: int):
        with self._lock:
            self._conn.execute("DELETE FROM log_entries WHERE idx >= ?", (from_index,))
            self._conn.commit()

    def get_log_entries(self) -> List[LogEntry]:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT idx, term, command FROM log_entries ORDER BY idx ASC")
            entries = []
            for row in cursor.fetchall():
                entries.append(LogEntry(index=row[0], term=row[1], command=json.loads(row[2])))
            return entries

    def get_last_log_info(self) -> Tuple[int, int]:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT idx, term FROM log_entries ORDER BY idx DESC LIMIT 1")
            row = cursor.fetchone()
            if row:
                return row[0], row[1]
            return 0, 0

    def close(self):
        with self._lock:
            self._conn.close()
