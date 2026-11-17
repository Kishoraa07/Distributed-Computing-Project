#!/usr/bin/env python3
"""
DistriKV: A Mini Distributed Key-Value Store & Cluster Coordinator
Subject: Distributed Computing
Topic: Design a mini distributed application and analyze its major design issues
       and challenges, including scalability, communication, and fault tolerance.

Architecture Features:
1. Consistent Hashing Ring with Configurable Virtual Nodes (Scalability & Load Balancing)
2. Multi-Threaded TCP JSON-RPC Server per Node (Inter-Node Communication)
3. Lamport Logical Clocks for Causal Event Ordering (Synchronization)
4. Tunable Quorum Replication (N, W, R) with Early-Return Quorum Completion (Consistency vs. Tail Latency)
5. Heartbeat Gossiper & Automatic Read-Repair Failover (Fault Tolerance)
"""

import argparse
import bisect
import hashlib
import json
import queue
import socket
import socketserver
import threading
import time
from typing import Dict, List, Optional, Tuple


class LamportClock:
    """Thread-safe Lamport Logical Clock for causal ordering across distributed nodes."""

    def __init__(self) -> None:
        self._value = 0
        self._lock = threading.Lock()

    def increment(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_time: int) -> int:
        with self._lock:
            self._value = max(self._value, received_time) + 1
            return self._value

    @property
    def value(self) -> int:
        with self._lock:
            return self._value


class ConsistentHashRing:
    """
    Consistent Hashing Ring using SHA-256 and Virtual Nodes.
    Minimizes key remapping (O(K/N)) when nodes join or leave the cluster.
    """

    def __init__(self, virtual_nodes: int = 25) -> None:
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        self.nodes: set = set()

    @staticmethod
    def _hash(key: str) -> int:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return int(digest[:8], 16)

    def add_node(self, node_id: str) -> None:
        self.nodes.add(node_id)
        for v in range(self.virtual_nodes):
            vnode_key = f"{node_id}#vnode{v}"
            h = self._hash(vnode_key)
            self.ring[h] = node_id
            bisect.insort(self.sorted_keys, h)

    def remove_node(self, node_id: str) -> None:
        self.nodes.discard(node_id)
        for v in range(self.virtual_nodes):
            vnode_key = f"{node_id}#vnode{v}"
            h = self._hash(vnode_key)
            if h in self.ring:
                del self.ring[h]
                idx = bisect.bisect_left(self.sorted_keys, h)
                if idx < len(self.sorted_keys) and self.sorted_keys[idx] == h:
                    self.sorted_keys.pop(idx)

    def get_preference_list(self, key: str, replication_factor: int = 3) -> List[str]:
        """Returns up to N distinct physical nodes responsible for storing `key`."""
        if not self.ring:
            return []
        h = self._hash(key)
        idx = bisect.bisect_left(self.sorted_keys, h)
        chosen: List[str] = []
        visited = 0
        total_vnodes = len(self.sorted_keys)

        while len(chosen) < min(replication_factor, len(self.nodes)) and visited < total_vnodes:
            ring_pos = (idx + visited) % total_vnodes
            candidate = self.ring[self.sorted_keys[ring_pos]]
            if candidate not in chosen:
                chosen.append(candidate)
            visited += 1
        return chosen


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


class StorageNode:
    """
    Represents an independent Distributed Storage Node executing over TCP JSON-RPC.
    Maintains local key-value state, versioned timestamps (Last-Write-Wins via Lamport Clock),
    and communication telemetry.
    """

    def __init__(self, node_id: str, host: str = "127.0.0.1", port: int = 0) -> None:
        self.node_id = node_id
        self.host = host
        self.clock = LamportClock()
        self.store: Dict[str, Tuple[str, int, str]] = {}  # key -> (value, lamport_ts, writer_node)
        self.store_lock = threading.Lock()
        self.alive = True
        self.simulated_delay_ms: float = 0.0
        self.rpc_received_count = 0

        node_ref = self

        class RPCHandler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                try:
                    raw = self.rfile.readline().decode("utf-8").strip()
                    if not raw:
                        return
                    req = json.loads(raw)
                    resp = node_ref.handle_rpc(req)
                    self.wfile.write((json.dumps(resp) + "\n").encode("utf-8"))
                except Exception as exc:
                    err = {"status": "ERROR", "error": str(exc)}
                    self.wfile.write((json.dumps(err) + "\n").encode("utf-8"))

        self.server = ThreadedTCPServer((self.host, port), RPCHandler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def handle_rpc(self, req: dict) -> dict:
        if not self.alive:
            return {"status": "NODE_DOWN", "node_id": self.node_id}

        if self.simulated_delay_ms > 0:
            time.sleep(self.simulated_delay_ms / 1000.0)

        self.rpc_received_count += 1
        msg_ts = req.get("lamport_ts", 0)
        local_ts = self.clock.update(msg_ts)
        op = req.get("op")

        if op == "PING":
            return {"status": "ALIVE", "node_id": self.node_id, "lamport_ts": local_ts}

        if op == "REPLICA_PUT":
            key = req["key"]
            val = req["value"]
            write_ts = req["write_ts"]
            origin = req.get("origin", self.node_id)
            with self.store_lock:
                current = self.store.get(key)
                # Last-Write-Wins (LWW) conflict resolution using (lamport_ts, origin_id)
                if current is None or (write_ts, origin) >= (current[1], current[2]):
                    self.store[key] = (val, write_ts, origin)
            return {"status": "ACK", "node_id": self.node_id, "lamport_ts": local_ts}

        if op == "REPLICA_GET":
            key = req["key"]
            with self.store_lock:
                record = self.store.get(key)
            if record is None:
                return {"status": "NOT_FOUND", "node_id": self.node_id, "lamport_ts": local_ts}
            return {
                "status": "OK",
                "node_id": self.node_id,
                "value": record[0],
                "write_ts": record[1],
                "origin": record[2],
                "lamport_ts": local_ts,
            }

        return {"status": "UNKNOWN_OP", "node_id": self.node_id, "lamport_ts": local_ts}

    def shutdown(self) -> None:
        self.alive = False
        self.server.shutdown()
        self.server.server_close()


class ClusterCoordinator:
    """
    Coordinates client PUT/GET requests across the Consistent Hash Ring using
    tunable Quorum parameters (N, W, R) and monitors node health via heartbeats.
    """

    def __init__(self, virtual_nodes: int = 25, n_replicas: int = 3, w_quorum: int = 2, r_quorum: int = 2) -> None:
        self.ring = ConsistentHashRing(virtual_nodes=virtual_nodes)
        self.nodes: Dict[str, StorageNode] = {}
        self.health_status: Dict[str, bool] = {}
        self.n_replicas = n_replicas
        self.w_quorum = w_quorum
        self.r_quorum = r_quorum
        self.clock = LamportClock()
        self.total_rpc_calls = 0
        self.failed_rpc_calls = 0

    def register_node(self, node: StorageNode) -> None:
        self.nodes[node.node_id] = node
        self.health_status[node.node_id] = True
        self.ring.add_node(node.node_id)

    def send_rpc(self, node_id: str, payload: dict, timeout: float = 0.5) -> dict:
        node = self.nodes[node_id]
        payload["lamport_ts"] = self.clock.increment()
        self.total_rpc_calls += 1
        try:
            with socket.create_connection((node.host, node.port), timeout=timeout) as sock:
                sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))
                data = sock.makefile("r", encoding="utf-8").readline().strip()
                if not data:
                    self.failed_rpc_calls += 1
                    return {"status": "EMPTY_RESPONSE"}
                resp = json.loads(data)
                if "lamport_ts" in resp:
                    self.clock.update(resp["lamport_ts"])
                if resp.get("status") == "NODE_DOWN":
                    self.failed_rpc_calls += 1
                return resp
        except Exception as exc:
            self.failed_rpc_calls += 1
            return {"status": "RPC_ERROR", "error": str(exc)}

    def check_heartbeats(self) -> Dict[str, bool]:
        """Polls all nodes via PING RPC to update cluster liveness state."""
        for nid in list(self.nodes.keys()):
            resp = self.send_rpc(nid, {"op": "PING"})
            self.health_status[nid] = resp.get("status") == "ALIVE"
        return dict(self.health_status)

    def put(self, key: str, value: str) -> Tuple[bool, int, float]:
        """
        Executes a distributed PUT across N preference nodes.
        Returns immediately once W replicas return ACK (or all N finish if ACKs < W),
        allowing remaining replicas to complete asynchronously in background threads.
        """
        t0 = time.perf_counter()
        write_ts = self.clock.increment()
        targets = self.ring.get_preference_list(key, self.n_replicas)
        result_q: queue.Queue = queue.Queue()

        def _worker(nid: str) -> None:
            resp = self.send_rpc(
                nid,
                {
                    "op": "REPLICA_PUT",
                    "key": key,
                    "value": value,
                    "write_ts": write_ts,
                    "origin": "coordinator",
                },
            )
            result_q.put(resp.get("status") == "ACK")

        for nid in targets:
            t = threading.Thread(target=_worker, args=(nid,), daemon=True)
            t.start()

        acks = 0
        completed = 0
        total_targets = len(targets)
        while completed < total_targets:
            ok = result_q.get()
            completed += 1
            if ok:
                acks += 1
            if acks >= self.w_quorum:
                break

        latency_ms = (time.perf_counter() - t0) * 1000.0
        return (acks >= self.w_quorum, acks, latency_ms)

    def get(self, key: str) -> Tuple[Optional[str], int, float, bool]:
        """
        Executes a Quorum GET across N preference nodes.
        Returns (latest_value, responses_count, latency_ms, read_repair_triggered).
        """
        t0 = time.perf_counter()
        targets = self.ring.get_preference_list(key, self.n_replicas)
        responses: List[dict] = []
        lock = threading.Lock()

        def _worker(nid: str) -> None:
            resp = self.send_rpc(nid, {"op": "REPLICA_GET", "key": key})
            if resp.get("status") == "OK":
                with lock:
                    responses.append(resp)

        threads = [threading.Thread(target=_worker, args=(nid,)) for nid in targets]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        latency_ms = (time.perf_counter() - t0) * 1000.0
        if len(responses) < self.r_quorum:
            return (None, len(responses), latency_ms, False)

        # Select version with highest Lamport timestamp
        latest = max(responses, key=lambda r: (r["write_ts"], r["origin"]))
        versions = {r["write_ts"] for r in responses}
        read_repair_needed = len(versions) > 1

        # Trigger synchronous Read-Repair if stale replica is detected
        if read_repair_needed:
            for r in responses:
                if r["write_ts"] < latest["write_ts"]:
                    self.send_rpc(
                        r["node_id"],
                        {
                            "op": "REPLICA_PUT",
                            "key": key,
                            "value": latest["value"],
                            "write_ts": latest["write_ts"],
                            "origin": latest["origin"],
                        },
                    )

        return (latest["value"], len(responses), latency_ms, read_repair_needed)

    def shutdown_all(self) -> None:
        for node in self.nodes.values():
            node.shutdown()


def run_interactive_demo() -> None:
    print("=" * 72)
    print("  DistriKV: Mini Distributed Key-Value Store Demonstration")
    print("  Configuration: 5 Physical Nodes | 25 Virtual Nodes/Node | N=3, W=2, R=2")
    print("=" * 72)

    cluster = ClusterCoordinator(virtual_nodes=25, n_replicas=3, w_quorum=2, r_quorum=2)
    for i in range(1, 6):
        node = StorageNode(f"node-{i}")
        cluster.register_node(node)
        print(f"[+] Started {node.node_id} on {node.host}:{node.port}")

    sample_records = {
        "student:101": "Kishoraa_TR_CSE",
        "course:DC2026": "Distributed_Computing",
        "project:status": "Quorum_Active",
        "sensor:trichy_01": "31.4C_Humidity_62%",
        "job:batch_42": "Completed_Shard_3",
    }

    print("\n--- Phase 1: Distributed Quorum Writes (N=3, W=2) ---")
    for k, v in sample_records.items():
        replicas = cluster.ring.get_preference_list(k, 3)
        ok, acks, lat = cluster.put(k, v)
        print(f"PUT ({k} -> {v}) | Replicas: {replicas} | Quorum ACKs: {acks} | Success: {ok} | Latency: {lat:.2f} ms")
    time.sleep(0.05)

    print("\n--- Phase 2: Simulating Node Failure (Stopping node-2) ---")
    cluster.nodes["node-2"].alive = False
    liveness = cluster.check_heartbeats()
    print(f"Heartbeat Monitor Status: {liveness}")

    print("\n--- Phase 3: Fault-Tolerant Quorum Reads (N=3, R=2) Under Node Failure ---")
    for k in sample_records:
        val, resp_cnt, lat, repaired = cluster.get(k)
        print(f"GET ({k}) -> '{val}' | Quorum Replies: {resp_cnt}/3 | Read-Repair: {repaired} | Latency: {lat:.2f} ms")

    print("\n--- Phase 4: Write During Outage, Node Recovery & Active Read-Repair ---")
    # student:101 maps to ['node-5', 'node-2', 'node-3']; writing while node-2 is down leaves node-2 stale
    cluster.put("student:101", "Kishoraa_TR_CSE_Verified_v2")
    time.sleep(0.05)
    cluster.nodes["node-2"].alive = True
    val, resp_cnt, lat, repaired = cluster.get("student:101")
    print(
        f"GET (student:101) after node-2 recovery -> '{val}' | Replies: {resp_cnt}/3 | "
        f"Stale Replica Repaired: {repaired} | Final Lamport Clock: {cluster.clock.value}"
    )

    cluster.shutdown_all()
    print("\n[✓] Cluster shut down cleanly.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DistriKV Mini Distributed Application")
    parser.add_argument("--demo", action="store_true", help="Run interactive cluster lifecycle demo")
    args = parser.parse_args()
    run_interactive_demo()
