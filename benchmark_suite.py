#!/usr/bin/env python3
"""
Empirical Benchmark & Chart Generator for DistriKV Mini Distributed Application.
Runs actual multi-node cluster tests and generates high-DPI publication charts
plus packages the GitHub repository zip.
"""

import json
import os
import statistics
import sys
import time
import zipfile

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# Import our distributed application
sys.path.insert(0, "/working_dir/c_2da7d9d94f4d49cf/artifacts/file_generation/ttl=63d/output")
from distrikv_cluster import ClusterCoordinator, ConsistentHashRing, StorageNode

INTERMEDIATE_DIR = "/working_dir/c_2da7d9d94f4d49cf/artifacts/file_generation/ttl=63d/intermediate"
OUTPUT_DIR = "/working_dir/c_2da7d9d94f4d49cf/artifacts/file_generation/ttl=63d/output"
os.makedirs(INTERMEDIATE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_experiments() -> dict:
    results = {}

    # 1. Scalability Experiment A: Virtual Nodes vs Load Balance Standard Deviation (10,000 keys across 8 nodes)
    vnode_configs = [1, 5, 10, 25, 50, 100]
    num_keys = 10000
    num_nodes = 8
    cv_scores = []  # Coefficient of variation (std / mean * 100%)
    max_min_ratios = []

    for v in vnode_configs:
        ring = ConsistentHashRing(virtual_nodes=v)
        for n in range(1, num_nodes + 1):
            ring.add_node(f"node-{n}")
        counts = {f"node-{n}": 0 for n in range(1, num_nodes + 1)}
        for k in range(num_keys):
            primary = ring.get_preference_list(f"key-{k}", replication_factor=1)[0]
            counts[primary] += 1
        vals = list(counts.values())
        mean_val = statistics.mean(vals)
        stdev_val = statistics.stdev(vals)
        cv = (stdev_val / mean_val) * 100.0
        cv_scores.append(round(cv, 2))
        max_min_ratios.append(round(max(vals) / max(1, min(vals)), 2))

    results["vnode_scalability"] = {
        "vnodes": vnode_configs,
        "cv_percent": cv_scores,
        "max_min_ratio": max_min_ratios,
    }

    # 2. Scalability Experiment B: Key Migration Overhead (Consistent Hashing vs Modulo Hashing when scaling N -> N+1)
    cluster_sizes = [3, 4, 6, 8, 10, 12]
    ch_migration_pct = []
    mod_migration_pct = []

    for n in cluster_sizes:
        ring_before = ConsistentHashRing(virtual_nodes=50)
        ring_after = ConsistentHashRing(virtual_nodes=50)
        for i in range(1, n + 1):
            ring_before.add_node(f"node-{i}")
            ring_after.add_node(f"node-{i}")
        ring_after.add_node(f"node-{n + 1}")

        moved_ch = 0
        moved_mod = 0
        for k in range(5000):
            key_str = f"user_record_{k}"
            h_val = ConsistentHashRing._hash(key_str)
            if ring_before.get_preference_list(key_str, 1)[0] != ring_after.get_preference_list(key_str, 1)[0]:
                moved_ch += 1
            if (h_val % n) != (h_val % (n + 1)):
                moved_mod += 1

        ch_migration_pct.append(round((moved_ch / 5000.0) * 100.0, 2))
        mod_migration_pct.append(round((moved_mod / 5000.0) * 100.0, 2))

    results["migration_comparison"] = {
        "cluster_sizes": [f"{n}→{n+1}" for n in cluster_sizes],
        "consistent_hashing_pct": ch_migration_pct,
        "modulo_hashing_pct": mod_migration_pct,
    }

    # 3. Communication & Replication Policy Latency Experiment on Real TCP Cluster (3 Replicas with Heterogeneous Delays)
    # Tiered replica delays: Fast replica (0.5x base), Median replica (1.0x base), Straggler replica (2.2x base)
    delays_ms = [0, 2, 5, 10, 15]
    w1_latencies = []
    w2_latencies = []
    w3_latencies = []

    for d_ms in delays_ms:
        for w_mode, target_list in [(1, w1_latencies), (2, w2_latencies), (3, w3_latencies)]:
            coord = ClusterCoordinator(virtual_nodes=25, n_replicas=3, w_quorum=w_mode, r_quorum=2)
            delay_multipliers = {1: 0.5, 2: 1.0, 3: 2.2}
            for i in range(1, 4):
                node = StorageNode(f"node-{i}")
                node.simulated_delay_ms = d_ms * delay_multipliers[i]
                coord.register_node(node)

            sample_lats = []
            for k in range(25):
                ok, acks, lat = coord.put(f"bench_key_{k}", f"val_{k}")
                sample_lats.append(lat)
            time.sleep(0.05)
            target_list.append(round(statistics.mean(sample_lats), 2))
            coord.shutdown_all()

    results["communication_latency"] = {
        "network_delay_ms": delays_ms,
        "w1_ms": w1_latencies,
        "w2_quorum_ms": w2_latencies,
        "w3_strict_ms": w3_latencies,
    }

    # 4. Fault Tolerance & Availability Experiment (80 requests across healthy, 1-node crash, 2-node crash)
    scenarios = ["0 Nodes Failed\n(Healthy)", "1 Node Failed\n(Single Fault)", "2 Nodes Failed\n(Double Fault)"]
    quorum_avail = []
    strict_avail = []

    for failed_count in [0, 1, 2]:
        coord_q = ClusterCoordinator(virtual_nodes=25, n_replicas=3, w_quorum=2, r_quorum=2)
        coord_s = ClusterCoordinator(virtual_nodes=25, n_replicas=3, w_quorum=3, r_quorum=3)
        for i in range(1, 6):
            nq = StorageNode(f"node-{i}")
            ns = StorageNode(f"node-{i}")
            if i <= failed_count:
                nq.alive = False
                ns.alive = False
            coord_q.register_node(nq)
            coord_s.register_node(ns)

        q_ok = sum(1 for k in range(80) if coord_q.put(f"ft_key_{k}", "v")[0])
        s_ok = sum(1 for k in range(80) if coord_s.put(f"ft_key_{k}", "v")[0])
        time.sleep(0.05)
        quorum_avail.append(round((q_ok / 80.0) * 100.0, 1))
        strict_avail.append(round((s_ok / 80.0) * 100.0, 1))
        coord_q.shutdown_all()
        coord_s.shutdown_all()

    results["fault_availability"] = {
        "scenarios": scenarios,
        "quorum_w2_pct": quorum_avail,
        "strict_w3_pct": strict_avail,
    }

    return results


def generate_charts(results: dict) -> None:
    plt.rcParams["font.family"] = "sans-serif"
    plt.rcParams["font.size"] = 10

    # Chart 1: Scalability Analysis (2-panel: Virtual Node Load Imbalance + Key Migration %)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(6.8, 3.2), dpi=220, constrained_layout=True)

    vnodes = results["vnode_scalability"]["vnodes"]
    cvs = results["vnode_scalability"]["cv_percent"]
    ax1.plot(vnodes, cvs, marker="o", linewidth=2.2, color="#1e3a5f", label="Load Imbalance (CV %)")
    for x, y in zip(vnodes, cvs):
        ax1.annotate(
            f"{y:.1f}%",
            xy=(x, y),
            xytext=(0, 7),
            textcoords="offset points",
            ha="center",
            fontsize=8.5,
            fontweight="bold",
            color="#1e3a5f",
        )
    ax1.set_title("A. Virtual Nodes vs. Load Imbalance", fontsize=10.5, fontweight="bold", color="#1e3a5f")
    ax1.set_xlabel("Virtual Nodes per Physical Node (V)")
    ax1.set_ylabel("Coeff. of Variation (CV %)")
    ax1.set_ylim(0, max(cvs) * 1.25)
    ax1.grid(True, linestyle="--", alpha=0.4)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    transitions = results["migration_comparison"]["cluster_sizes"]
    ch_pct = results["migration_comparison"]["consistent_hashing_pct"]
    mod_pct = results["migration_comparison"]["modulo_hashing_pct"]
    x_idx = np.arange(len(transitions))
    width = 0.36
    ax2.bar(x_idx - width / 2, mod_pct, width, label="Modulo (hash % N)", color="#cbd5e1", edgecolor="#64748b")
    rects2 = ax2.bar(x_idx + width / 2, ch_pct, width, label="Consistent Hashing", color="#d97706")
    for r in rects2:
        h = r.get_height()
        ax2.annotate(
            f"{h:.0f}%",
            xy=(r.get_x() + r.get_width() / 2, h),
            xytext=(0, 3),
            textcoords="offset points",
            ha="center",
            fontsize=8,
            fontweight="bold",
            color="#b45309",
        )
    ax2.set_title("B. Keys Remapped on Node Join (N→N+1)", fontsize=10.5, fontweight="bold", color="#1e3a5f")
    ax2.set_xlabel("Cluster Scaling Transition")
    ax2.set_ylabel("Keys Migrated (%)")
    ax2.set_xticks(x_idx)
    ax2.set_xticklabels(transitions, fontsize=8.5)
    ax2.set_ylim(0, 105)
    ax2.legend(loc="upper right", frameon=True, fontsize=8)
    ax2.grid(True, axis="y", linestyle="--", alpha=0.4)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.savefig(os.path.join(INTERMEDIATE_DIR, "chart_scalability.png"), dpi=220, bbox_inches="tight")
    plt.close(fig)

    # Chart 2: Communication & Fault Tolerance Analysis (2-panel: RPC Latency vs Delay + Availability under Crashes)
    fig2, (bx1, bx2) = plt.subplots(1, 2, figsize=(6.8, 3.2), dpi=220, constrained_layout=True)

    delays = results["communication_latency"]["network_delay_ms"]
    w1 = results["communication_latency"]["w1_ms"]
    w2 = results["communication_latency"]["w2_quorum_ms"]
    w3 = results["communication_latency"]["w3_strict_ms"]

    bx1.plot(delays, w1, marker="s", linewidth=2, color="#0d9488", label="W=1 (Fastest ACK)")
    bx1.plot(delays, w2, marker="o", linewidth=2.2, color="#1e3a5f", label="W=2 (Quorum N=3)")
    bx1.plot(delays, w3, marker="^", linewidth=2, linestyle="--", color="#dc2626", label="W=3 (Strict All-Replica)")
    bx1.set_title("A. Write RPC Latency vs. Base Network Delay", fontsize=10.5, fontweight="bold", color="#1e3a5f")
    bx1.set_xlabel("Base Network Delay D (ms) [Straggler = 2.2D]")
    bx1.set_ylabel("Mean Write Latency (ms)")
    bx1.legend(loc="upper left", frameon=True, fontsize=8)
    bx1.grid(True, linestyle="--", alpha=0.4)
    bx1.spines["top"].set_visible(False)
    bx1.spines["right"].set_visible(False)

    scen = results["fault_availability"]["scenarios"]
    q_av = results["fault_availability"]["quorum_w2_pct"]
    s_av = results["fault_availability"]["strict_w3_pct"]
    x2 = np.arange(len(scen))
    r_q = bx2.bar(x2 - width / 2, q_av, width, label="Quorum (N=3, W=2)", color="#1e3a5f")
    r_s = bx2.bar(x2 + width / 2, s_av, width, label="Strict (N=3, W=3)", color="#dc2626")
    for r in list(r_q) + list(r_s):
        h = r.get_height()
        bx2.annotate(
            f"{h:.0f}%",
            xy=(r.get_x() + r.get_width() / 2, h),
            xytext=(0, 3),
            textcoords="offset points",
            ha="center",
            fontsize=8,
            fontweight="bold",
        )
    bx2.set_title("B. Write Availability Under Node Failures", fontsize=10.5, fontweight="bold", color="#1e3a5f")
    bx2.set_xlabel("Cluster Failure Scenario (5-Node Cluster)")
    bx2.set_ylabel("Successful Writes (%)")
    bx2.set_xticks(x2)
    bx2.set_xticklabels(scen, fontsize=8)
    bx2.set_ylim(0, 118)
    bx2.legend(loc="upper right", frameon=True, fontsize=8)
    bx2.grid(True, axis="y", linestyle="--", alpha=0.4)
    bx2.spines["top"].set_visible(False)
    bx2.spines["right"].set_visible(False)

    fig2.savefig(os.path.join(INTERMEDIATE_DIR, "chart_comm_fault.png"), dpi=220, bbox_inches="tight")
    plt.close(fig2)


def build_github_zip(results: dict) -> None:
    zip_path = os.path.join(OUTPUT_DIR, "DistriKV_GitHub_Repo.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(os.path.join(OUTPUT_DIR, "distrikv_cluster.py"), arcname="distrikv_cluster.py")
        zf.write(os.path.join(OUTPUT_DIR, "README.md"), arcname="README.md")
        zf.write(os.path.join(INTERMEDIATE_DIR, "run_benchmarks.py"), arcname="benchmark_suite.py")
        zf.writestr(
            "benchmark_results.json",
            json.dumps(results, indent=2),
        )
        zf.writestr(
            ".gitignore",
            "__pycache__/\n*.pyc\n.DS_Store\n.venv/\n",
        )


if __name__ == "__main__":
    res = run_experiments()
    with open(os.path.join(INTERMEDIATE_DIR, "benchmark_results.json"), "w") as f:
        json.dump(res, f, indent=2)
    generate_charts(res)
    build_github_zip(res)
    print(json.dumps(res, indent=2))
