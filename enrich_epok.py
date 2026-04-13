"""
Enrich caba_normativa.db with EPOK catastro data for all parcels.

Calls EPOK API per parcel and stores: dirección oficial, superficie cubierta,
propiedad horizontal, pisos construidos, unidades funcionales, y métricas
de subutilización (pisos construidos vs permitidos).

Designed to run for hours in background. Saves progress continuously —
safe to interrupt and resume.
"""

import json
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path

DB_PATH = Path(__file__).parent / "caba_normativa.db"
EPOK_URL = "https://epok.buenosaires.gob.ar/catastro/parcela/?smp={smp}"
HEADERS = {"Referer": "https://ciudad3d.buenosaires.gob.ar/"}
TIMEOUT_S = 5
DELAY_S = 0.25  # 4 req/sec — gentle rate limit
BATCH_COMMIT = 100  # commit every N parcels
LOG_EVERY = 500


def add_epok_columns(conn: sqlite3.Connection) -> None:
    """Add EPOK columns if they don't exist yet."""
    cur = conn.cursor()
    columns = {
        "epok_direccion": "TEXT",
        "epok_sup_cubierta": "REAL",
        "epok_propiedad_horizontal": "INTEGER",
        "epok_pisos_sobre": "INTEGER",
        "epok_pisos_bajo": "INTEGER",
        "epok_unidades_func": "INTEGER",
        "epok_locales": "INTEGER",
        "epok_calle": "TEXT",
        "epok_altura": "INTEGER",
        "epok_frente": "REAL",
        "epok_fondo": "REAL",
        "epok_sup_total": "REAL",
        "epok_enriched": "INTEGER DEFAULT 0",
        "delta_pisos": "INTEGER",
        "ratio_subutilizacion": "REAL",
    }
    existing = {
        row[1] for row in cur.execute("PRAGMA table_info(parcelas)").fetchall()
    }
    for col, dtype in columns.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE parcelas ADD COLUMN {col} {dtype}")
    conn.commit()


def fetch_epok(smp: str) -> dict | None:
    """Fetch EPOK data for a single SMP. Returns parsed JSON or None."""
    url = EPOK_URL.format(smp=smp)
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:
            return json.loads(r.read())
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return None
    except json.JSONDecodeError:
        return None


def parse_epok(data: dict) -> dict:
    """Extract all useful fields from EPOK response."""
    puertas = data.get("puertas", [])
    principal = next((p for p in puertas if p.get("puerta_principal")), None)

    sup_total = _float(data.get("superficie_total"))
    sup_cubierta = _float(data.get("superficie_cubierta"))
    frente = _float(data.get("frente"))
    fondo = _float(data.get("fondo"))
    pisos_sobre = _int(data.get("pisos_sobre_rasante"))
    pisos_bajo = _int(data.get("pisos_bajo_rasante"))
    uf = _int(data.get("unidades_funcionales"))
    locales = _int(data.get("locales"))
    ph = 1 if data.get("propiedad_horizontal") == "Si" else 0

    return {
        "direccion": data.get("direccion", ""),
        "sup_total": sup_total,
        "sup_cubierta": sup_cubierta,
        "frente": frente,
        "fondo": fondo,
        "ph": ph,
        "pisos_sobre": pisos_sobre,
        "pisos_bajo": pisos_bajo,
        "uf": uf,
        "locales": locales,
        "calle": principal["calle"] if principal else "",
        "altura": principal.get("altura") if principal else None,
    }


def _float(v: object) -> float | None:
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _int(v: object) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    add_epok_columns(conn)
    cur = conn.cursor()

    # Count pending
    cur.execute(
        "SELECT COUNT(*) FROM parcelas WHERE epok_enriched IS NULL OR epok_enriched = 0"
    )
    pending = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM parcelas WHERE epok_enriched = 1")
    done = cur.fetchone()[0]
    total = pending + done
    print(f"EPOK enrichment: {done:,} done, {pending:,} pending, {total:,} total")

    if pending == 0:
        print("Nothing to do.")
        return

    eta_hours = pending * DELAY_S / 3600
    print(f"Estimated time: ~{eta_hours:.1f} hours at {1/DELAY_S:.0f} req/sec")
    print(f"Safe to interrupt — progress saves every {BATCH_COMMIT} parcels\n")

    cur.execute(
        "SELECT id, smp, pisos FROM parcelas "
        "WHERE epok_enriched IS NULL OR epok_enriched = 0 "
        "ORDER BY id"
    )
    rows = cur.fetchall()

    enriched = 0
    errors = 0
    t0 = time.time()

    for i, (row_id, smp, pisos_permitidos) in enumerate(rows):
        data = fetch_epok(smp)

        if data:
            parsed = parse_epok(data)

            # Subutilización: pisos construidos vs permitidos
            delta = None
            ratio = None
            if parsed["pisos_sobre"] is not None and pisos_permitidos and pisos_permitidos > 0:
                delta = pisos_permitidos - parsed["pisos_sobre"]
                ratio = round(parsed["pisos_sobre"] / pisos_permitidos, 3)

            for _attempt in range(15):
                try:
                    cur.execute(
                        """UPDATE parcelas SET
                            epok_direccion=?, epok_sup_cubierta=?, epok_propiedad_horizontal=?,
                            epok_pisos_sobre=?, epok_pisos_bajo=?, epok_unidades_func=?,
                            epok_locales=?, epok_calle=?, epok_altura=?,
                            epok_frente=?, epok_fondo=?, epok_sup_total=?,
                            epok_enriched=1, delta_pisos=?, ratio_subutilizacion=?
                        WHERE id=?""",
                        (
                            parsed["direccion"], parsed["sup_cubierta"], parsed["ph"],
                            parsed["pisos_sobre"], parsed["pisos_bajo"], parsed["uf"],
                            parsed["locales"], parsed["calle"], parsed["altura"],
                            parsed["frente"], parsed["fondo"], parsed["sup_total"],
                            delta, ratio, row_id,
                        ),
                    )
                    break
                except sqlite3.OperationalError:
                    time.sleep(1 + _attempt * 0.5)
            enriched += 1
        else:
            for _attempt in range(15):
                try:
                    cur.execute(
                        "UPDATE parcelas SET epok_enriched = -1 WHERE id = ?", (row_id,)
                    )
                    break
                except sqlite3.OperationalError:
                    time.sleep(1 + _attempt * 0.5)
            errors += 1

        if (i + 1) % BATCH_COMMIT == 0:
            conn.commit()

        if (i + 1) % LOG_EVERY == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            remaining = (len(rows) - i - 1) / rate / 3600
            print(
                f"  [{i+1:>7,}/{len(rows):,}] "
                f"ok={enriched:,} err={errors:,} "
                f"rate={rate:.1f}/s "
                f"ETA={remaining:.1f}h"
            )

        time.sleep(DELAY_S)

    conn.commit()
    conn.close()

    elapsed_h = (time.time() - t0) / 3600
    print(f"\nDone in {elapsed_h:.1f}h — enriched={enriched:,}, errors={errors:,}")


if __name__ == "__main__":
    main()
