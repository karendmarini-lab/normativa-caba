"""
Enrich caba_normativa.db with Ciudad 3D API data for all parcels.

Endpoints:
1. cur3d/seccion_edificabilidad — buildable envelope, FOTs, plusvalía, retiros, catalogación
2. cur3d/parcelas_plausibles_a_enrase — can the parcel match neighbor heights
3. datos_utiles — comuna, barrio, comisaría, hospital, distrito escolar

Safe to interrupt and resume.
"""

import json
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path

DB_PATH = Path(__file__).parent / "caba_normativa.db"
DELAY_S = 0.25
BATCH_COMMIT = 100
LOG_EVERY = 500

HEADERS = {
    "Referer": "https://ciudad3d.buenosaires.gob.ar/",
    "User-Agent": "Mozilla/5.0",
}


def fetch_json(url: str) -> dict | None:
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=6) as r:
            return json.loads(r.read())
    except Exception:
        return None


def add_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    existing = {r[1] for r in cur.execute("PRAGMA table_info(parcelas)").fetchall()}
    columns = {
        # seccion_edificabilidad
        "edif_sup_max_edificable": "REAL",
        "edif_sup_edificable_planta": "REAL",
        "edif_altura_max_1": "REAL",
        "edif_altura_max_2": "REAL",
        "edif_altura_max_3": "REAL",
        "edif_altura_max_4": "REAL",
        "edif_plano_limite": "REAL",
        "edif_fot_medianera": "REAL",
        "edif_fot_perim_libre": "REAL",
        "edif_fot_semi_libre": "REAL",
        "edif_plusvalia_incidencia_uva": "REAL",
        "edif_plusvalia_alicuota": "REAL",
        "edif_tipica": "TEXT",
        "edif_irregular": "INTEGER",
        "edif_superficie_parcela": "REAL",
        "edif_catalogacion_proteccion": "TEXT",
        "edif_catalogacion_denominacion": "TEXT",
        "edif_riesgo_hidrico": "INTEGER",
        "edif_lep": "INTEGER",
        "edif_ensanche": "INTEGER",
        "edif_apertura": "INTEGER",
        "edif_enrase": "INTEGER",
        "edif_linderas": "TEXT",
        "edif_rivolta": "INTEGER",
        "edif_croquis_url": "TEXT",
        "edif_perimetro_url": "TEXT",
        "edif_plano_indice_url": "TEXT",
        # datos_utiles
        "du_comuna": "TEXT",
        "du_barrio": "TEXT",
        "du_comisaria": "TEXT",
        "du_hospital": "TEXT",
        "du_distrito_escolar": "TEXT",
        "du_comisaria_vecinal": "TEXT",
        "du_distrito_economico": "TEXT",
        # control
        "cur3d_enriched": "INTEGER DEFAULT 0",
    }
    for col, dtype in columns.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE parcelas ADD COLUMN {col} {dtype}")
    conn.commit()


def process_parcel(conn: sqlite3.Connection, row_id: int, smp: str,
                   lat: float, lng: float) -> bool:
    """Fetch all 3 endpoints and update DB. Returns True if successful."""
    # 1. seccion_edificabilidad
    edif = fetch_json(
        f"https://epok.buenosaires.gob.ar/cur3d/seccion_edificabilidad/?smp={smp}"
    )

    # 2. enrase
    enrase_data = fetch_json(
        f"https://epok.buenosaires.gob.ar/cur3d/parcelas_plausibles_a_enrase/?smp={smp}"
    )

    # 3. datos_utiles (uses lat/lng)
    du = fetch_json(
        f"https://ws.usig.buenosaires.gob.ar/datos_utiles?x={lng}&y={lat}"
    )

    if not edif:
        conn.execute("UPDATE parcelas SET cur3d_enriched=-1 WHERE id=?", (row_id,))
        return False

    alturas = edif.get("altura_max", [0, 0, 0, 0])
    while len(alturas) < 4:
        alturas.append(0)
    plusv = edif.get("plusvalia", {})
    fot = edif.get("fot", {})
    cat = edif.get("catalogacion", {})
    afect = edif.get("afectaciones", {})
    links = edif.get("link_imagen", {})
    linderas = edif.get("parcelas_linderas", {})

    enrase_val = 1 if (enrase_data and enrase_data.get("enrase")) else 0

    conn.execute(
        """UPDATE parcelas SET
            edif_sup_max_edificable=?, edif_sup_edificable_planta=?,
            edif_altura_max_1=?, edif_altura_max_2=?, edif_altura_max_3=?, edif_altura_max_4=?,
            edif_plano_limite=?,
            edif_fot_medianera=?, edif_fot_perim_libre=?, edif_fot_semi_libre=?,
            edif_plusvalia_incidencia_uva=?, edif_plusvalia_alicuota=?,
            edif_tipica=?, edif_irregular=?, edif_superficie_parcela=?,
            edif_catalogacion_proteccion=?, edif_catalogacion_denominacion=?,
            edif_riesgo_hidrico=?, edif_lep=?, edif_ensanche=?, edif_apertura=?,
            edif_enrase=?, edif_linderas=?, edif_rivolta=?,
            edif_croquis_url=?, edif_perimetro_url=?, edif_plano_indice_url=?,
            du_comuna=?, du_barrio=?, du_comisaria=?, du_hospital=?,
            du_distrito_escolar=?, du_comisaria_vecinal=?, du_distrito_economico=?,
            cur3d_enriched=1
        WHERE id=?""",
        (
            edif.get("sup_max_edificable"),
            edif.get("sup_edificable_planta"),
            alturas[0], alturas[1], alturas[2], alturas[3],
            edif.get("altura_max_plano_limite"),
            fot.get("fot_medianera"), fot.get("fot_perim_libre"), fot.get("fot_semi_libre"),
            plusv.get("incidencia_uva"), plusv.get("alicuota"),
            edif.get("tipica"), 1 if edif.get("irregular") else 0,
            edif.get("superficie_parcela"),
            cat.get("proteccion"), cat.get("denominacion"),
            afect.get("riesgo_hidrico", 0), afect.get("lep", 0),
            afect.get("ensanche", 0), afect.get("apertura", 0),
            enrase_val,
            json.dumps(linderas.get("smp_linderas", [])),
            edif.get("rivolta", 0),
            links.get("croquis_parcela"), links.get("perimetro_manzana"),
            links.get("plano_indice"),
            (du or {}).get("comuna"), (du or {}).get("barrio"),
            (du or {}).get("comisaria"), (du or {}).get("area_hospitalaria"),
            (du or {}).get("distrito_escolar"), (du or {}).get("comisaria_vecinal"),
            (du or {}).get("distrito_economico"),
            row_id,
        ),
    )
    return True


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    add_columns(conn)

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM parcelas WHERE cur3d_enriched IS NULL OR cur3d_enriched=0")
    pending = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM parcelas WHERE cur3d_enriched=1")
    done = cur.fetchone()[0]
    total = pending + done
    print(f"CUR3D enrichment: {done:,} done, {pending:,} pending, {total:,} total")

    if pending == 0:
        print("Nothing to do.")
        return

    # 3 API calls per parcel, DELAY_S between parcels
    eta_hours = pending * DELAY_S * 3 / 3600
    print(f"Estimated time: ~{eta_hours:.1f} hours (3 calls/parcel at {1/DELAY_S:.0f} req/sec)")
    print(f"Safe to interrupt — progress saves every {BATCH_COMMIT} parcels\n")

    cur.execute(
        "SELECT id, smp, lat, lng FROM parcelas "
        "WHERE cur3d_enriched IS NULL OR cur3d_enriched=0 "
        "ORDER BY id"
    )
    rows = cur.fetchall()

    enriched = 0
    errors = 0
    t0 = time.time()

    for i, (row_id, smp, lat, lng) in enumerate(rows):
        ok = process_parcel(conn, row_id, smp, lat, lng)
        if ok:
            enriched += 1
        else:
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
