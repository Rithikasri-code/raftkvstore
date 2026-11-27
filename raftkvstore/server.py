"""
Asyncio TCP Server & RPC Framing for Raft Node.
Listens for Raft peer RPCs and client GET/SET/DELETE queries.
"""

import asyncio
import json
import logging
from typing import Optional, List
from .node import RaftNode
from .protocol import (
    RequestVoteArgs, RequestVoteReply, AppendEntriesArgs, AppendEntriesReply,
    ClientRequest, ClientResponse, LogEntry
)

logger = logging.getLogger("raft.server")

class RaftServer:
    def __init__(self, node: RaftNode, host: str = "127.0.0.1", port: int = 5001):
        self.node = node
        self.host = host
        self.port = port
        self._server: Optional[asyncio.Server] = None

    async def start(self):
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        await self.node.start()
        logger.info(f"Raft node {self.node.node_id} listening on {self.host}:{self.port}")

    async def stop(self):
        await self.node.stop()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            line = await reader.readline()
            if not line:
                return

            req_data = json.loads(line.decode("utf-8").strip())
            req_type = req_data.get("type")

            if req_type == "RequestVoteArgs":
                args = RequestVoteArgs(
                    term=req_data["term"],
                    candidate_id=req_data["candidate_id"],
                    last_log_index=req_data["last_log_index"],
                    last_log_term=req_data["last_log_term"]
                )
                reply = await self.node.handle_request_vote(args)
                resp_bytes = (json.dumps(reply.to_dict()) + "\n").encode("utf-8")
                writer.write(resp_bytes)
                await writer.drain()

            elif req_type == "AppendEntriesArgs":
                entries = [LogEntry.from_dict(e) for e in req_data.get("entries", [])]
                args = AppendEntriesArgs(
                    term=req_data["term"],
                    leader_id=req_data["leader_id"],
                    prev_log_index=req_data["prev_log_index"],
                    prev_log_term=req_data["prev_log_term"],
                    entries=entries,
                    leader_commit=req_data["leader_commit"]
                )
                reply = await self.node.handle_append_entries(args)
                resp_bytes = (json.dumps(reply.to_dict()) + "\n").encode("utf-8")
                writer.write(resp_bytes)
                await writer.drain()

            elif req_type == "ClientRequest":
                req = ClientRequest(
                    op=req_data["op"],
                    key=req_data["key"],
                    value=req_data.get("value")
                )
                resp = await self.node.execute_client_command(req)
                resp_bytes = (json.dumps(resp.to_dict()) + "\n").encode("utf-8")
                writer.write(resp_bytes)
                await writer.drain()

        except Exception as e:
            logger.error(f"Error handling connection: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

async def send_rpc_async(peer_addr: str, message: Any) -> Optional[Any]:
    try:
        host, port_str = peer_addr.split(":")
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, int(port_str)), timeout=0.1)
        writer.write((json.dumps(message.to_dict()) + "\n").encode("utf-8"))
        await writer.drain()

        line = await asyncio.wait_for(reader.readline(), timeout=0.2)
        writer.close()
        await writer.wait_closed()

        if not line:
            return None
        data = json.loads(line.decode("utf-8").strip())
        t = data.get("type")
        if t == "RequestVoteReply":
            return RequestVoteReply(term=data["term"], vote_granted=data["vote_granted"])
        elif t == "AppendEntriesReply":
            return AppendEntriesReply(term=data["term"], success=data["success"], match_index=data.get("match_index", 0))
        elif t == "ClientResponse":
            return ClientResponse(success=data["success"], value=data.get("value"), leader_id=data.get("leader_id"), error=data.get("error"))
        return None
    except Exception:
        return None
