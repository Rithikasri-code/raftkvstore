import pytest
import asyncio
from raftkvstore.protocol import (
    Role, LogEntry, RequestVoteArgs, AppendEntriesArgs, ClientRequest
)
from raftkvstore.storage import Storage
from raftkvstore.state_machine import KeyValueStateMachine
from raftkvstore.node import RaftNode

@pytest.mark.asyncio
async def test_state_machine_operations():
    sm = KeyValueStateMachine()
    assert sm.apply(1, {"op": "SET", "key": "k1", "value": "v1"}) == "v1"
    assert sm.get("k1") == "v1"
    assert sm.apply(2, {"op": "GET", "key": "k1"}) == "v1"
    assert sm.apply(3, {"op": "DELETE", "key": "k1"}) == "v1"
    assert sm.get("k1") is None

@pytest.mark.asyncio
async def test_storage_log_persistence():
    storage = Storage()
    storage.set_metadata(current_term=3, voted_for="node_1")
    term, voted = storage.get_metadata()
    assert term == 3
    assert voted == "node_1"

    entries = [
        LogEntry(term=1, index=1, command={"op": "SET", "key": "a", "value": "1"}),
        LogEntry(term=1, index=2, command={"op": "SET", "key": "b", "value": "2"}),
        LogEntry(term=2, index=3, command={"op": "SET", "key": "c", "value": "3"}),
    ]
    storage.append_entries(entries)
    retrieved = storage.get_log_entries()
    assert len(retrieved) == 3
    assert retrieved[2].command["key"] == "c"

    last_idx, last_term = storage.get_last_log_info()
    assert last_idx == 3
    assert last_term == 2

    storage.truncate_from(2)
    truncated = storage.get_log_entries()
    assert len(truncated) == 1
    assert truncated[0].index == 1

@pytest.mark.asyncio
async def test_raft_node_vote_and_replication():
    node = RaftNode("node_1", peers=["node_2", "node_3"])
    
    # 1. Vote Request Handling
    vote_args = RequestVoteArgs(term=1, candidate_id="node_2", last_log_index=0, last_log_term=0)
    vote_reply = await node.handle_request_vote(vote_args)
    assert vote_reply.vote_granted is True
    assert vote_reply.term == 1

    # Second vote in same term rejected
    vote_args2 = RequestVoteArgs(term=1, candidate_id="node_3", last_log_index=0, last_log_term=0)
    vote_reply2 = await node.handle_request_vote(vote_args2)
    assert vote_reply2.vote_granted is False

    # 2. AppendEntries Handling
    append_args = AppendEntriesArgs(
        term=1,
        leader_id="node_2",
        prev_log_index=0,
        prev_log_term=0,
        entries=[LogEntry(term=1, index=1, command={"op": "SET", "key": "cluster", "value": "online"})],
        leader_commit=1
    )
    append_reply = await node.handle_append_entries(append_args)
    assert append_reply.success is True
    assert node.state_machine.get("cluster") == "online"

    await node.stop()
