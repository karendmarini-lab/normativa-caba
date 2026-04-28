#!/usr/bin/env python3
"""
EdificIA production monitor — run via SSH or cron.

Usage:
    python3 monitor.py              # One-shot status
    python3 monitor.py --watch 10   # Refresh every 10 seconds
    python3 monitor.py --json       # Machine-readable output

Reports: CPU load, RAM/swap, uvicorn status, CLI processes,
precache state, nginx status, and recent errors.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path


def get_memory():
    """Parse /proc/meminfo for RAM and swap."""
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 2:
                info[parts[0].rstrip(":")] = int(parts[1])  # kB
    return {
        "ram_total_mb": info.get("MemTotal", 0) // 1024,
        "ram_used_mb": (info.get("MemTotal", 0) - info.get("MemAvailable", 0)) // 1024,
        "ram_available_mb": info.get("MemAvailable", 0) // 1024,
        "swap_total_mb": info.get("SwapTotal", 0) // 1024,
        "swap_used_mb": (info.get("SwapTotal", 0) - info.get("SwapFree", 0)) // 1024,
    }


def get_load():
    """Get CPU load averages."""
    load1, load5, load15 = os.getloadavg()
    return {"load_1m": round(load1, 2), "load_5m": round(load5, 2), "load_15m": round(load15, 2)}


def get_uptime():
    """Get system uptime in seconds."""
    with open("/proc/uptime") as f:
        return int(float(f.read().split()[0]))


def get_processes():
    """Find uvicorn and claude CLI processes with RSS."""
    procs = {"uvicorn": [], "claude_cli": []}
    for pid_dir in Path("/proc").iterdir():
        if not pid_dir.name.isdigit():
            continue
        try:
            cmdline = (pid_dir / "cmdline").read_text().replace("\0", " ").strip()
            status = (pid_dir / "status").read_text()
            rss_kb = 0
            state = "?"
            for line in status.splitlines():
                if line.startswith("VmRSS:"):
                    rss_kb = int(line.split()[1])
                if line.startswith("State:"):
                    state = line.split()[1]
            rss_mb = round(rss_kb / 1024, 1)
            pid = int(pid_dir.name)

            if "uvicorn" in cmdline and "server:app" in cmdline:
                procs["uvicorn"].append({"pid": pid, "rss_mb": rss_mb, "state": state})
            elif "claude" in cmdline and "stream-json" in cmdline:
                procs["claude_cli"].append({"pid": pid, "rss_mb": rss_mb, "state": state})
        except (PermissionError, FileNotFoundError, ProcessLookupError, ValueError):
            continue
    return procs


def check_http(port=8765, path="/api/health", timeout=5):
    """Check if uvicorn responds locally."""
    import urllib.request
    try:
        url = f"http://127.0.0.1:{port}{path}"
        req = urllib.request.Request(url)
        start = time.time()
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode()
            elapsed = time.time() - start
            try:
                data = json.loads(body)
                return {"status": "ok", "elapsed_s": round(elapsed, 2), "data": data}
            except json.JSONDecodeError:
                return {"status": "error", "elapsed_s": round(elapsed, 2), "body": body[:200]}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_recent_errors(n=5):
    """Get last N error lines from journalctl."""
    try:
        result = subprocess.run(
            ["journalctl", "-u", "edificia", "--no-pager", "-n", "50", "--since", "5 min ago"],
            capture_output=True, text=True, timeout=5,
        )
        errors = [
            line for line in result.stdout.splitlines()
            if any(kw in line.lower() for kw in ["error", "exception", "traceback", "killed", "oom", "timeout"])
        ]
        return errors[-n:]
    except Exception:
        return []


def get_precache_status():
    """Check if precache completed by looking at recent logs."""
    try:
        result = subprocess.run(
            ["journalctl", "-u", "edificia", "--no-pager", "-n", "200"],
            capture_output=True, text=True, timeout=5,
        )
        lines = result.stdout.splitlines()
        for line in reversed(lines):
            if "precache: done" in line:
                return {"status": "done", "log": line.strip()}
            if "precache: starting" in line:
                return {"status": "running", "log": line.strip()}
        return {"status": "unknown"}
    except Exception:
        return {"status": "unknown"}


def monitor():
    """Collect all metrics."""
    return {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "uptime_s": get_uptime(),
        "load": get_load(),
        "memory": get_memory(),
        "processes": get_processes(),
        "http_check": check_http(),
        "precache": get_precache_status(),
        "recent_errors": get_recent_errors(),
    }


def format_human(data):
    """Pretty-print for terminal."""
    mem = data["memory"]
    load = data["load"]
    procs = data["processes"]
    http = data["http_check"]
    precache = data["precache"]

    ram_pct = round(mem["ram_used_mb"] / mem["ram_total_mb"] * 100) if mem["ram_total_mb"] else 0

    lines = [
        f"=== EdificIA Monitor — {data['timestamp']} (up {data['uptime_s'] // 60}m) ===",
        f"",
        f"CPU:  load {load['load_1m']} / {load['load_5m']} / {load['load_15m']}",
        f"RAM:  {mem['ram_used_mb']}MB / {mem['ram_total_mb']}MB ({ram_pct}%) — {mem['ram_available_mb']}MB available",
        f"Swap: {mem['swap_used_mb']}MB / {mem['swap_total_mb']}MB",
        f"",
        f"Uvicorn:  {len(procs['uvicorn'])} process(es)",
    ]
    for p in procs["uvicorn"]:
        lines.append(f"  PID {p['pid']}: {p['rss_mb']}MB (state: {p['state']})")

    lines.append(f"Claude CLI: {len(procs['claude_cli'])} process(es)")
    total_cli_mb = sum(p["rss_mb"] for p in procs["claude_cli"])
    for p in procs["claude_cli"]:
        lines.append(f"  PID {p['pid']}: {p['rss_mb']}MB (state: {p['state']})")
    if procs["claude_cli"]:
        lines.append(f"  Total CLI: {total_cli_mb}MB")

    lines.append(f"")
    if http["status"] == "ok":
        health = http.get("data", {})
        sys_block = health.get("system", {})
        lines.append(f"HTTP:     OK ({http['elapsed_s']}s)")
        if sys_block:
            lines.append(f"  Precache keys: {sys_block.get('precache_keys', '?')}")
            lines.append(f"  Chat sessions: {sys_block.get('chat_sessions', '?')}")
    else:
        lines.append(f"HTTP:     FAIL — {http.get('error', http.get('body', '?'))}")

    lines.append(f"Precache: {precache['status']}")
    if precache.get("log"):
        lines.append(f"  {precache['log']}")

    if data["recent_errors"]:
        lines.append(f"")
        lines.append(f"Recent errors:")
        for e in data["recent_errors"]:
            lines.append(f"  {e}")

    return "\n".join(lines)


if __name__ == "__main__":
    if "--json" in sys.argv:
        print(json.dumps(monitor(), indent=2))
    elif "--watch" in sys.argv:
        try:
            idx = sys.argv.index("--watch")
            interval = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 10
        except (ValueError, IndexError):
            interval = 10
        while True:
            os.system("clear")
            print(format_human(monitor()))
            time.sleep(interval)
    else:
        print(format_human(monitor()))
