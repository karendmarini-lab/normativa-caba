"""Match Zonaprop terrenos to EdificIA parcelas via USIG geocoding."""

import json
import math
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass

ZONAPROP_DB = "zonaprop.db"
EDIFICIA_DB = "caba_normativa.db"
USIG_URL = "https://servicios.usig.buenosaires.gob.ar/normalizar"
DELAY = 0.1  # seconds between USIG calls


def log(msg: str) -> None:
    print(msg, flush=True)


@dataclass
class GeoResult:
    lat: float
    lng: float
    direccion_norm: str


def clean_address(raw: str) -> str:
    """Clean Zonaprop address for USIG geocoding."""
    addr = raw.strip()
    addr = re.sub(r"\s+al\s+", " ", addr)  # "Inclan al 3000" → "Inclan 3000"
    addr = re.sub(r"\.\s*Entre.*$", "", addr, flags=re.IGNORECASE)
    addr = re.sub(r"\s*E/.*$", "", addr, flags=re.IGNORECASE)
    addr = re.sub(r"\s*\|.*$", "", addr)  # "Terrada 1500 | Villa Santa Rita"
    addr = re.sub(r"\s*y\s+\w.*$", "", addr, flags=re.IGNORECASE)  # "X y Y"
    addr = addr.rstrip(".")
    addr = re.sub(r"\s+", " ", addr).strip()
    return addr


def geocode_usig(direccion: str) -> GeoResult | None:
    """Geocode an address using USIG API."""
    addr = clean_address(direccion)

    params = urllib.parse.urlencode(
        {"direccion": addr, "maxOptions": "1", "geocodificar": "true"}
    )
    url = f"{USIG_URL}?{params}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "EdificIA/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return None

    results = data.get("direccionesNormalizadas", [])
    if not results:
        return None

    r = results[0]
    coords = r.get("coordenadas", {})
    x, y = coords.get("x"), coords.get("y")
    if not x or not y:
        return None

    return GeoResult(
        lat=float(y),
        lng=float(x),
        direccion_norm=r.get("direccion", ""),
    )


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Distance in meters between two lat/lng points."""
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlng / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def smp_to_manzana(smp_norm: str) -> str:
    """Extract seccion-manzana from smp_norm (e.g. '16-44-38' → '16-44')."""
    parts = smp_norm.split("-")
    return f"{parts[0]}-{parts[1]}" if len(parts) >= 2 else smp_norm


def find_nearest_manzana(
    edificia: sqlite3.Connection, lat: float, lng: float, max_dist_m: float = 80
) -> tuple[str, str, float] | None:
    """Find nearest parcela's manzana by lat/lng. Returns (smp_norm, manzana, dist)."""
    delta = max_dist_m / 111000
    rows = edificia.execute(
        """SELECT smp_norm, lat, lng FROM parcelas
           WHERE lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?""",
        (lat - delta, lat + delta, lng - delta, lng + delta),
    ).fetchall()

    if not rows:
        return None

    best_smp, best_dist = None, float("inf")
    for smp, plat, plng in rows:
        d = haversine_m(lat, lng, plat, plng)
        if d < best_dist:
            best_smp, best_dist = smp, d

    if best_dist > max_dist_m or not best_smp:
        return None

    return best_smp, smp_to_manzana(best_smp), best_dist


def init_match_table(zp: sqlite3.Connection) -> None:
    zp.execute("DROP TABLE IF EXISTS matches")
    zp.execute("""
        CREATE TABLE matches (
            posting_id     TEXT PRIMARY KEY,
            smp_nearest    TEXT,
            manzana        TEXT,
            lat            REAL,
            lng            REAL,
            direccion_norm TEXT,
            distancia_m    REAL,
            FOREIGN KEY (posting_id) REFERENCES listings(posting_id)
        )
    """)
    zp.execute("CREATE INDEX idx_matches_manzana ON matches(manzana)")
    zp.commit()


def main() -> None:
    zp = sqlite3.connect(ZONAPROP_DB)
    edificia = sqlite3.connect(EDIFICIA_DB)
    init_match_table(zp)

    # Get terrenos not yet matched
    already = {r[0] for r in zp.execute("SELECT posting_id FROM matches").fetchall()}

    terrenos = zp.execute("""
        SELECT posting_id, direccion, barrio FROM listings
        WHERE tipo='terrenos' AND precio_usd > 100
          AND superficie_m2 > 0 AND superficie_m2 < 5000
          AND length(direccion) > 5
    """).fetchall()

    to_process = [(pid, d, b) for pid, d, b in terrenos if pid not in already]
    log(f"Total terrenos: {len(terrenos)}, already matched: {len(already)}, "
        f"to process: {len(to_process)}")

    matched, failed_geo, failed_match = 0, 0, 0

    for i, (posting_id, direccion, barrio) in enumerate(to_process):
        geo = geocode_usig(direccion)
        if not geo:
            failed_geo += 1
            if i < 20 or i % 200 == 0:
                log(f"  [{i}] GEOCODE FAIL: {direccion}")
            time.sleep(DELAY)
            continue

        result = find_nearest_manzana(edificia, geo.lat, geo.lng)
        if not result:
            failed_match += 1
            time.sleep(DELAY)
            continue

        smp_nearest, manzana, dist = result
        zp.execute(
            """INSERT OR REPLACE INTO matches
               (posting_id, smp_nearest, manzana, lat, lng, direccion_norm, distancia_m)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (posting_id, smp_nearest, manzana, geo.lat, geo.lng,
             geo.direccion_norm, round(dist, 1)),
        )
        matched += 1

        if i % 50 == 0:
            zp.commit()
            log(f"  [{i}/{len(to_process)}] matched={matched} "
                f"geo_fail={failed_geo} match_fail={failed_match}")

        time.sleep(DELAY)

    zp.commit()
    log(f"\nDone. {matched} matched, {failed_geo} geocode failures, "
        f"{failed_match} no parcela within 80m")

    # Show sample
    log("\nSample matches:")
    for row in zp.execute("""
        SELECT m.manzana, m.distancia_m, l.precio_usd, l.superficie_m2,
               l.direccion, m.direccion_norm
        FROM matches m JOIN listings l ON m.posting_id = l.posting_id
        ORDER BY m.distancia_m LIMIT 10
    """):
        log(f"  Mza {row[0]} ({row[1]}m) → USD {row[2]:,.0f} / {row[3]}m² — {row[4]}")

    zp.close()
    edificia.close()


if __name__ == "__main__":
    main()
