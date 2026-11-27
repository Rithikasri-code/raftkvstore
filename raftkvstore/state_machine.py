"""
Deterministic Key-Value State Machine with Atomic Snapshotting.
Executes committed Raft log commands.
"""

from typing import Dict, Optional, Any
import json

class KeyValueStateMachine:
    def __init__(self):
        self._store: Dict[str, str] = {}
        self.last_applied_index: int = 0

    def apply(self, index: int, command: Dict[str, Any]) -> Optional[str]:
        op = command.get("op", "").upper()
        key = command.get("key", "")
        value = command.get("value")

        self.last_applied_index = index

        if op == "SET":
            self._store[key] = str(value)
            return self._store[key]
        elif op == "GET":
            return self._store.get(key)
        elif op == "DELETE":
            return self._store.pop(key, None)
        return None

    def get(self, key: str) -> Optional[str]:
        return self._store.get(key)

    def create_snapshot(self) -> bytes:
        return json.dumps({
            "store": self._store,
            "last_applied_index": self.last_applied_index
        }).encode("utf-8")

    def restore_snapshot(self, snapshot_bytes: bytes):
        data = json.loads(snapshot_bytes.decode("utf-8"))
        self._store = data.get("store", {})
        self.last_applied_index = data.get("last_applied_index", 0)
