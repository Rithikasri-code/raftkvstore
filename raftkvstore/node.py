"""
Production Raft Consensus Engine Implementation (asyncio).
Implements leader election, randomized timeouts, log replication, and safety.
"""

import asyncio
import random
import logging
from typing import List, Dict, Optional, Any, Callable
from .protocol import (
    Role, LogEntry, RequestVoteArgs, RequestVoteReply,
    AppendEntriesArgs, AppendEntriesReply, ClientRequest, ClientResponse
)
from .storage import Storage
from .state_machine import KeyValueStateMachine

logger = logging.getLogger("raft")

class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: List[str],
        storage: Optional[Storage] = None,
        rpc_sender: Optional[Callable] = None
    ):
        self.node_id = node_id
        self.peers = peers
        self.storage = storage or Storage()
        self.state_machine = KeyValueStateMachine()
        self.rpc_sender = rpc_sender

        # Persistent state on all servers
        self.current_term, self.voted_for = self.storage.get_metadata()
        self.log: List[LogEntry] = self.storage.get_log_entries()

        # Volatile state on all servers
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.role: Role = Role.FOLLOWER
        self.current_leader: Optional[str] = None

        # Volatile state on leaders
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Timers & concurrency
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._running = False

    def last_log_index(self) -> int:
        return self.log[-1].index if self.log else 0

    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    async def start(self):
        self._running = True
        self._reset_election_timer()

    async def stop(self):
        self._running = False
        if self._election_task:
            self._election_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    def _reset_election_timer(self):
        if self._election_task:
            self._election_task.cancel()
        timeout = random.uniform(0.15, 0.30)
        self._election_task = asyncio.create_task(self._election_countdown(timeout))

    async def _election_countdown(self, timeout: float):
        try:
            await asyncio.sleep(timeout)
            async with self._lock:
                if self.role != Role.LEADER:
                    await self._start_election()
        except asyncio.CancelledError:
            pass

    async def _start_election(self):
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.storage.set_metadata(self.current_term, self.voted_for)
        self._reset_election_timer()

        votes_granted = 1
        needed_votes = (len(self.peers) + 1) // 2 + 1

        args = RequestVoteArgs(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self.last_log_index(),
            last_log_term=self.last_log_term()
        )

        for peer in self.peers:
            if self.rpc_sender:
                reply = await self.rpc_sender(peer, args)
                if reply and reply.vote_granted and reply.term == self.current_term:
                    votes_granted += 1
                elif reply and reply.term > self.current_term:
                    self.current_term = reply.term
                    self.role = Role.FOLLOWER
                    self.voted_for = None
                    self.storage.set_metadata(self.current_term, None)
                    self._reset_election_timer()
                    return

        if votes_granted >= needed_votes and self.role == Role.CANDIDATE:
            await self._become_leader()

    async def _become_leader(self):
        self.role = Role.LEADER
        self.current_leader = self.node_id
        if self._election_task:
            self._election_task.cancel()

        last_idx = self.last_log_index()
        for peer in self.peers:
            self.next_index[peer] = last_idx + 1
            self.match_index[peer] = 0

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self):
        while self._running and self.role == Role.LEADER:
            async with self._lock:
                await self._send_append_entries()
            await asyncio.sleep(0.05)

    async def _send_append_entries(self):
        for peer in self.peers:
            prev_idx = self.next_index.get(peer, 1) - 1
            prev_term = 0
            if prev_idx > 0:
                for entry in self.log:
                    if entry.index == prev_idx:
                        prev_term = entry.term
                        break

            entries_to_send = [e for e in self.log if e.index > prev_idx]
            args = AppendEntriesArgs(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_idx,
                prev_log_term=prev_term,
                entries=entries_to_send,
                leader_commit=self.commit_index
            )

            if self.rpc_sender:
                reply = await self.rpc_sender(peer, args)
                if reply:
                    if reply.term > self.current_term:
                        self.current_term = reply.term
                        self.role = Role.FOLLOWER
                        self.voted_for = None
                        self.storage.set_metadata(self.current_term, None)
                        if self._heartbeat_task:
                            self._heartbeat_task.cancel()
                        self._reset_election_timer()
                        return
                    if reply.success:
                        self.match_index[peer] = reply.match_index
                        self.next_index[peer] = reply.match_index + 1
                    else:
                        self.next_index[peer] = max(1, self.next_index[peer] - 1)

        self._update_commit_index()

    def _update_commit_index(self):
        # Majority commit index check
        matches = [self.last_log_index()] + list(self.match_index.values())
        matches.sort()
        majority_idx = matches[len(matches) // 2]

        if majority_idx > self.commit_index:
            for entry in self.log:
                if entry.index == majority_idx and entry.term == self.current_term:
                    self.commit_index = majority_idx
                    self._apply_to_state_machine()
                    break

    def _apply_to_state_machine(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            for entry in self.log:
                if entry.index == self.last_applied:
                    self.state_machine.apply(entry.index, entry.command)
                    break

    async def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        async with self._lock:
            if args.term > self.current_term:
                self.current_term = args.term
                self.role = Role.FOLLOWER
                self.voted_for = None
                self.storage.set_metadata(self.current_term, None)

            vote_granted = False
            if args.term == self.current_term:
                can_vote = (self.voted_for is None or self.voted_for == args.candidate_id)
                up_to_date = (
                    args.last_log_term > self.last_log_term() or
                    (args.last_log_term == self.last_log_term() and args.last_log_index >= self.last_log_index())
                )
                if can_vote and up_to_date:
                    vote_granted = True
                    self.voted_for = args.candidate_id
                    self.storage.set_metadata(self.current_term, self.voted_for)
                    self._reset_election_timer()

            return RequestVoteReply(term=self.current_term, vote_granted=vote_granted)

    async def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        async with self._lock:
            if args.term < self.current_term:
                return AppendEntriesReply(term=self.current_term, success=False)

            if args.term > self.current_term or self.role == Role.CANDIDATE:
                self.current_term = args.term
                self.role = Role.FOLLOWER
                self.voted_for = None
                self.storage.set_metadata(self.current_term, None)

            self.current_leader = args.leader_id
            self._reset_election_timer()

            # Verify log consistency
            if args.prev_log_index > 0:
                found = False
                for entry in self.log:
                    if entry.index == args.prev_log_index and entry.term == args.prev_log_term:
                        found = True
                        break
                if not found:
                    return AppendEntriesReply(term=self.current_term, success=False)

            # Insert/overwrite entries
            for new_entry in args.entries:
                existing = [e for e in self.log if e.index == new_entry.index]
                if existing and existing[0].term != new_entry.term:
                    self.storage.truncate_from(new_entry.index)
                    self.log = [e for e in self.log if e.index < new_entry.index]
                if not any(e.index == new_entry.index for e in self.log):
                    self.log.append(new_entry)
                    self.storage.append_entries([new_entry])

            if args.leader_commit > self.commit_index:
                self.commit_index = min(args.leader_commit, self.last_log_index())
                self._apply_to_state_machine()

            return AppendEntriesReply(
                term=self.current_term,
                success=True,
                match_index=self.last_log_index()
            )

    async def execute_client_command(self, req: ClientRequest) -> ClientResponse:
        async with self._lock:
            if self.role != Role.LEADER:
                return ClientResponse(
                    success=False,
                    leader_id=self.current_leader,
                    error="Not leader. Redirect to current leader."
                )

            if req.op.upper() == "GET":
                val = self.state_machine.get(req.key)
                return ClientResponse(success=True, value=val)

            new_idx = self.last_log_index() + 1
            entry = LogEntry(term=self.current_term, index=new_idx, command=req.to_dict())
            self.log.append(entry)
            self.storage.append_entries([entry])

        # Await majority commit
        for _ in range(50):
            await asyncio.sleep(0.02)
            if self.commit_index >= new_idx:
                val = self.state_machine.get(req.key)
                return ClientResponse(success=True, value=val)

        return ClientResponse(success=False, error="Command execution timed out awaiting majority replication")
