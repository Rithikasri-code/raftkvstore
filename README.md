# RaftKVStore

<p align="center">
  <img src="https://img.shields.io/badge/Language-Python%203.11%2B-3776AB?style=flat-square&logo=python&logoColor=white&labelColor=080c14" alt="Python 3.11+" />
  <img src="https://img.shields.io/badge/Consensus-Raft%20Protocol-10b981?style=flat-square&labelColor=080c14" alt="Raft Protocol" />
  <img src="https://img.shields.io/badge/Consistency-Linearizable%20Strict-38bdf8?style=flat-square&labelColor=080c14" alt="Linearizable Consistency" />
  <img src="https://img.shields.io/badge/Concurrency-AsyncIO%20Streams-3b82f6?style=flat-square&labelColor=080c14" alt="AsyncIO" />
  <img src="https://img.shields.io/badge/Storage-SQLite%20Durable%20WAL-8b5cf6?style=flat-square&labelColor=080c14" alt="SQLite WAL" />
  <img src="https://img.shields.io/badge/License-MIT-blue?style=flat-square&labelColor=080c14" alt="MIT License" />
</p>

---

## ⚡ Overview

**RaftKVStore** is an institutional-grade, fault-tolerant distributed replicated state machine and key-value storage engine engineered on the **Raft Consensus Algorithm** (Ongaro & Ousterhout). Built with non-blocking `asyncio` streams and durable SQLite WAL persistence, RaftKVStore guarantees **strict linearizable consistency**, automatic leader election with randomized jitter timeouts, and cluster safety across network partitions and hardware failures.

---

## 🏗️ Architecture

```mermaid
flowchart TD
    subgraph ClientLayer["💻 Client Ingestion"]
        Client["RaftClient CLI / SDK\n(Auto Leader Discovery & Failover)"]
    end

    subgraph Cluster["⚡ 3-Node Raft Consensus Cluster"]
        Node1["Raft Node 1 (LEADER)\nTerm: 4 | CommitIndex: 142"]
        Node2["Raft Node 2 (FOLLOWER)\nMatchIndex: 142"]
        Node3["Raft Node 3 (FOLLOWER)\nMatchIndex: 142"]
    end

    subgraph StateMachine["⚙️ Deterministic Replicated State Machine"]
        WAL["Durable SQLite WAL\n(term, voted_for, log_entries)"]
        KVS["KeyValueStateMachine\n(GET / SET / DELETE)"]
    end

    Client -->|TCP RPC| Node1
    Node1 -->|AppendEntries RPC| Node2
    Node1 -->|AppendEntries RPC| Node3
    Node1 --> WAL
    Node1 -->|Committed Entries| KVS
```

---

## 🚀 Key Architectural Features

- **Formal Leader Election:** Randomized election timeouts (150ms–300ms) prevent split-vote deadlocks; candidates request votes and transition to leader upon receiving majority quorum.
- **Log Matching & Replication:** Heartbeat-driven `AppendEntries` RPC verifies log continuity (`prev_log_index` / `prev_log_term`) and forces followers to replicate leader logs.
- **Durable Write-Ahead Logging:** State transitions (`current_term`, `voted_for`, and log sequence) persist durably to SQLite with ACID transaction semantics.
- **Deterministic State Machine:** Key-value engine executes committed log entries strictly in sequence, guaranteeing zero state divergence across surviving nodes.
- **Network Partition Tolerance:** Minority partitions cannot commit new log entries; majority partitions continue serving reads and writes without interruption.

---

## 💻 Quickstart

### Prerequisites
- Python 3.10+
- `pytest` & `pytest-asyncio`

### 1. Installation
```bash
git clone https://github.com/Rithikasri-code/raftkvstore.git
cd raftkvstore
pip install -r requirements.txt
```

### 2. Run Test Suite
```bash
pytest tests/ -v
```

### 3. Spin Up a 3-Node Cluster
```bash
# Terminal 1 (Node 1)
python -m raftkvstore.server --id node_1 --port 5001 --peers 127.0.0.1:5002,127.0.0.1:5003

# Terminal 2 (Node 2)
python -m raftkvstore.server --id node_2 --port 5002 --peers 127.0.0.1:5001,127.0.0.1:5003

# Terminal 3 (Node 3)
python -m raftkvstore.server --id node_3 --port 5003 --peers 127.0.0.1:5001,127.0.0.1:5002
```

### 4. Execute Client Queries
```bash
# Write a key
python -m raftkvstore.client SET alpha "production_cluster"

# Read the key
python -m raftkvstore.client GET alpha
```

---

## 📄 License

Distributed under the **MIT License**.
