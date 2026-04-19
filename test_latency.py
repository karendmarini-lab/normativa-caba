"""Latency test for EdificIA — run from the server."""
import os
import sqlite3
import time

import httpx

BASE = "http://127.0.0.1:8765"
DB = "caba_normativa.db"


def main() -> None:
    print("\n=== EDIFICIA LATENCY TEST ===\n")

    print("--- Static files ---")
    for path, label in [
        ("/manzanas_heatmap.json", "Heatmap JSON"),
        ("/index.html", "index.html"),
        ("/static/js/app.js", "app.js"),
    ]:
        t = time.monotonic()
        r = httpx.get(f"{BASE}{path}", timeout=30, follow_redirects=True)
        ms = round((time.monotonic() - t) * 1000)
        kb = round(len(r.content) / 1024, 1)
        flag = "RED" if ms > 2000 else "YEL" if ms > 500 else "GRN"
        print(f"  [{flag}] {label:20s} {ms:5d}ms  {kb:7.1f}KB")

    print("\n--- DB queries ---")
    conn = sqlite3.connect(DB, timeout=20)
    conn.row_factory = sqlite3.Row

    queries = [
        ("barrios", "SELECT barrio, COUNT(*) FROM parcelas WHERE barrio IS NOT NULL GROUP BY barrio", {}),
        ("geo Palermo", "SELECT smp FROM parcelas WHERE barrio = :b AND polygon_geojson IS NOT NULL LIMIT 3000", {"b": "Palermo"}),
        ("geo Caballito", "SELECT smp FROM parcelas WHERE barrio = :b AND polygon_geojson IS NOT NULL LIMIT 3000", {"b": "Caballito"}),
        ("nearest parcel", "SELECT smp FROM parcelas WHERE lat IS NOT NULL ORDER BY (lat+34.5883)*(lat+34.5883)+(lng+58.4215)*(lng+58.4215) LIMIT 1", {}),
        ("search acoyte", "SELECT smp FROM parcelas WHERE epok_direccion LIKE :q LIMIT 10", {"q": "%ACOYTE%"}),
    ]
    for label, q, params in queries:
        t = time.monotonic()
        rows = conn.execute(q, params).fetchall()
        ms = round((time.monotonic() - t) * 1000)
        flag = "RED" if ms > 1000 else "YEL" if ms > 200 else "GRN"
        print(f"  [{flag}] {label:20s} {ms:5d}ms  {len(rows)} rows")

    print("\n--- API endpoints ---")
    for ep in ["/api/barrios", "/api/health"]:
        t = time.monotonic()
        r = httpx.get(f"{BASE}{ep}", timeout=30, follow_redirects=True)
        ms = round((time.monotonic() - t) * 1000)
        flag = "RED" if ms > 2000 else "YEL" if ms > 500 else "GRN"
        print(f"  [{flag}] {ep:40s} {ms:5d}ms  HTTP {r.status_code}")

    print("\n--- File sizes ---")
    for fn in ["manzanas_heatmap.json", "caba_normativa.db"]:
        if os.path.exists(fn):
            mb = os.path.getsize(fn) / 1024 / 1024
            print(f"  {fn:35s} {mb:6.1f} MB")

    print()


if __name__ == "__main__":
    main()
