# DistriKV: Mini Distributed Key-Value Store & Design Issue Analyzer

**Course:** Distributed Computing  
**Project Topic:** *Design a mini distributed application and analyze its major design issues and challenges, including scalability, communication, and fault tolerance.*  
**Author:** Kishoraa T R (Department of Computer Science and Engineering, SRM TRP Engineering College / SRM Institute of Science and Technology)

## 1. Project Overview

**DistriKV** is a lightweight, zero-dependency, multi-node distributed key-value storage system implemented in Python (`distrikv_cluster.py`). It models real-world distributed storage engines (such as [Amazon DynamoDB and Apache Cassandra](https://www.geeksforgeeks.org/system-design/what-is-replication-in-consistent-hashing/)) using separate multi-threaded TCP socket servers, a [Consistent Hashing ring with virtual nodes](https://www.hellointerview.com/learn/system-design/core-concepts/consistent-hashing), Lamport logical clocks for causal ordering, tunable quorum replication (`N, W, R`), and active heartbeat fault detection with automatic read-repair.

## 2. Core Architectural Components

| Component | Implementation Mechanism | Distributed Computing Challenge Addressed |
| :--- | :--- | :--- |
| **Partitioning Layer** | `ConsistentHashRing` (SHA-256 + Virtual Nodes) | **Horizontal Scalability:** Avoids `O(K)` full-cluster rehashing; limits key migration to `O(K/N)` when nodes join or leave. |
| **Communication Layer** | `ThreadedTCPServer` with newline-delimited JSON-RPC | **Inter-Node Communication:** Handles concurrent TCP socket streams, request serialization, and network timeouts. |
| **Synchronization Layer** | `LamportClock` + Last-Write-Wins (LWW) tuples | **Causal Ordering & Concurrency:** Orders concurrent updates across nodes without relying on synchronized physical clocks. |
| **Replication & Fault Layer** | `ClusterCoordinator` (`N=3, W=2, R=2`) + Read-Repair | **Fault Tolerance & CAP Trade-off:** Continues serving reads and writes despite single-node crashes and heals stale replicas upon recovery. |

## 3. Repository Structure

* `distrikv_cluster.py` — Complete runnable multi-node TCP distributed key-value cluster, coordinator, Lamport clock implementation, and failover demonstration.
* `benchmark_suite.py` — Automated empirical evaluation suite measuring load balancing skew, key remapping overhead, communication latency under varying quorum policies, and fault-tolerant failover.
* `benchmark_results.json` — Raw empirical benchmark telemetry generated from live 5-node and 8-node cluster executions.
* `README.md` — Project documentation, architecture guide, and GitHub upload instructions.

## 4. How to Run Locally

Requires **Python 3.8+** (uses only the Python standard library; no external `pip` packages needed to run the cluster).

```bash
# Run the 5-node cluster demonstration (writes, node failure, quorum reads, and read-repair)
python3 distrikv_cluster.py --demo
```

## 5. Quick Steps to Upload to GitHub & Share on College Drive

1. Sign in to your GitHub account and click **New Repository**.
2. Name the repository `DistriKV-Distributed-Computing` and set visibility to **Public**.
3. Click **uploading an existing file** on the quick-setup screen (or run `git init`, `git add .`, `git commit -m "Initial commit: DistriKV mini distributed app"`, and `git push`).
4. Upload `distrikv_cluster.py` and `README.md` (or extract `DistriKV_GitHub_Repo.zip` and upload its contents), then click **Commit changes**.
5. Copy your repository URL (`https://github.com/<your-username>/DistriKV-Distributed-Computing`) and paste it into your college submission drive.
