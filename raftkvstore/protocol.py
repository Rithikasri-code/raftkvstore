"""
Raft RPC Protocol Definitions & Serialization
Implements standard Raft consensus messages and client KV operations.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Any, Dict
import json
import enum

class Role(str, enum.Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {"term": self.term, "index": self.index, "command": self.command}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEntry":
        return cls(term=data["term"], index=data["index"], command=data["command"])

@dataclass
class RequestVoteArgs:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "RequestVoteArgs",
            "term": self.term,
            "candidate_id": self.candidate_id,
            "last_log_index": self.last_log_index,
            "last_log_term": self.last_log_term,
        }

@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "RequestVoteReply",
            "term": self.term,
            "vote_granted": self.vote_granted,
        }

@dataclass
class AppendEntriesArgs:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "AppendEntriesArgs",
            "term": self.term,
            "leader_id": self.leader_id,
            "prev_log_index": self.prev_log_index,
            "prev_log_term": self.prev_log_term,
            "entries": [e.to_dict() for e in self.entries],
            "leader_commit": self.leader_commit,
        }

@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "AppendEntriesReply",
            "term": self.term,
            "success": self.success,
            "match_index": self.match_index,
        }

@dataclass
class ClientRequest:
    op: str  # GET, SET, DELETE
    key: str
    value: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"type": "ClientRequest", "op": self.op, "key": self.key, "value": self.value}

@dataclass
class ClientResponse:
    success: bool
    value: Optional[str] = None
    leader_id: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ClientResponse",
            "success": self.success,
            "value": self.value,
            "leader_id": self.leader_id,
            "error": self.error,
        }
