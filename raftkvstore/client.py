"""
RaftKVStore Asynchronous CLI Client.
Discovers active leader, dispatches strongly-consistent GET/SET/DELETE commands.
"""

import asyncio
import sys
import argparse
from typing import List, Optional
from .protocol import ClientRequest, ClientResponse
from .server import send_rpc_async

class RaftClient:
    def __init__(self, cluster_addrs: List[str]):
        self.cluster_addrs = cluster_addrs
        self.leader_addr: Optional[str] = cluster_addrs[0] if cluster_addrs else None

    async def execute(self, op: str, key: str, value: Optional[str] = None) -> ClientResponse:
        req = ClientRequest(op=op, key=key, value=value)
        
        # Try leader first, then fall back across peers
        targets = [self.leader_addr] + [a for a in self.cluster_addrs if a != self.leader_addr]
        for target in targets:
            if not target:
                continue
            resp = await send_rpc_async(target, req)
            if resp and resp.success:
                return resp
            elif resp and resp.leader_id:
                # Redirect
                self.leader_addr = resp.leader_id
                resp2 = await send_rpc_async(self.leader_addr, req)
                if resp2:
                    return resp2

        return ClientResponse(success=False, error="Cluster unreachable or election in progress")

async def main():
    parser = argparse.ArgumentParser(description="RaftKVStore CLI Client")
    parser.add_argument("--cluster", default="127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003", help="Comma-separated peer addresses")
    parser.add_argument("op", choices=["GET", "SET", "DELETE"], help="Operation to execute")
    parser.add_argument("key", help="Key name")
    parser.add_argument("value", nargs="?", default=None, help="Value (for SET)")
    args = parser.parse_args()

    client = RaftClient(args.cluster.split(","))
    resp = await client.execute(args.op, args.key, args.value)
    if resp.success:
        print(f"SUCCESS: {resp.value if resp.value is not None else 'OK'}")
    else:
        print(f"ERROR: {resp.error}")

if __name__ == "__main__":
    asyncio.run(main())
