"""
Parallel enrichment: EPOK and CUR3D run on separate threads.
While CUR3D waits 3-5s for seccion_edificabilidad, EPOK processes another parcel.
Single DB connection with WAL mode for concurrent writes.
Idempotent — safe to kill and restart.
"""

import json
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

DB_PATH = Path(__file__).parent / "caba_normativa.db"
LOG_EVERY = 100
STALL_S = 20

HEADERS = {
    "Referer": "https://ciudad3d.buenosaires.gob.ar/",
    "User-Agent": "Mozilla/5.0",
}

# Thread-safe DB lock
db_lock = threading.Lock()


def fetch(url: str, label: str = "") -> dict | None:
    req = urllib.request.Request(url, headers=HEADERS)
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=STALL_S) as r:
            data = json.loads(r.read())
        elapsed = time.time() - t0
        if elapsed > 10:
            print(f"  SLOW: {label} {elapsed:.1f}s", flush=True)
        return data
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  FAIL: {label} {elapsed:.1f}s {e}", flush=True)
        return None


def db_execute(conn, sql, params, retries=15):
    for attempt in range(retries):
        try:
            with db_lock:
                conn.execute(sql, params)
                if attempt == 0:
                    return True
                return True
        except sqlite3.OperationalError:
            time.sleep(0.3 + attempt * 0.3)
    return False


def db_commit(conn, retries=15):
    for attempt in range(retries):
        try:
            with db_lock:
                conn.commit()
                return
        except sqlite3.OperationalError:
            time.sleep(0.3 + attempt * 0.3)


def run_epok(conn):
    """Thread: enrich parcels with EPOK catastro data."""
    cur = conn.cursor()
    with db_lock:
        cur.execute(
            "SELECT id, smp, pisos FROM parcelas "
            "WHERE COALESCE(epok_enriched,0)<=0 ORDER BY id"
        )
        rows = cur.fetchall()

    total = len(rows)
    print(f"[EPOK] Starting: {total:,} parcels", flush=True)
    ok = 0
    err = 0
    t0 = time.time()

    for i, (row_id, smp, pisos_perm) in enumerate(rows):
        data = fetch(
            f"https://epok.buenosaires.gob.ar/catastro/parcela/?smp={smp}",
            f"EPOK {smp}",
        )

        if data:
            puertas = data.get("puertas", [])
            principal = next((p for p in puertas if p.get("puerta_principal")), None)

            def _f(v):
                try:
                    f = float(v)
                    return f if f > 0 else None
                except (TypeError, ValueError):
                    return None

            def _i(v):
                try:
                    return int(v)
                except (TypeError, ValueError):
                    return None

            pisos_s = _i(data.get("pisos_sobre_rasante"))
            delta = None
            ratio = None
            if pisos_s is not None and pisos_perm and pisos_perm > 0:
                delta = pisos_perm - pisos_s
                ratio = round(pisos_s / pisos_perm, 3)

            db_execute(conn,
                """UPDATE parcelas SET
                    epok_direccion=?, epok_sup_cubierta=?, epok_propiedad_horizontal=?,
                    epok_pisos_sobre=?, epok_pisos_bajo=?, epok_unidades_func=?,
                    epok_locales=?, epok_calle=?, epok_altura=?,
                    epok_frente=?, epok_fondo=?, epok_sup_total=?,
                    epok_enriched=1, delta_pisos=?, ratio_subutilizacion=?
                WHERE id=?""",
                (
                    data.get("direccion", ""),
                    _f(data.get("superficie_cubierta")),
                    1 if data.get("propiedad_horizontal") == "Si" else 0,
                    pisos_s, _i(data.get("pisos_bajo_rasante")),
                    _i(data.get("unidades_funcionales")),
                    _i(data.get("locales")),
                    principal["calle"] if principal else "",
                    principal.get("altura") if principal else None,
                    _f(data.get("frente")), _f(data.get("fondo")),
                    _f(data.get("superficie_total")),
                    delta, ratio, row_id,
                ),
            )
            ok += 1
        else:
            db_execute(conn, "UPDATE parcelas SET epok_enriched=-1 WHERE id=?", (row_id,))
            err += 1

        if (i + 1) % LOG_EVERY == 0:
            db_commit(conn)
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (total - i - 1) / rate / 3600
            print(f"[EPOK] [{i+1:>7,}/{total:,}] ok={ok:,} err={err} {rate:.1f}/s ETA={eta:.1f}h", flush=True)

    db_commit(conn)
    print(f"[EPOK] Done: ok={ok:,} err={err}", flush=True)


def run_cur3d(conn):
    """Thread: enrich parcels with CUR3D edificabilidad data."""
    cur = conn.cursor()
    with db_lock:
        cur.execute(
            "SELECT id, smp, lat, lng FROM parcelas "
            "WHERE COALESCE(cur3d_enriched,0)<=0 ORDER BY id"
        )
        rows = cur.fetchall()

    total = len(rows)
    print(f"[CUR3D] Starting: {total:,} parcels", flush=True)
    ok = 0
    err = 0
    t0 = time.time()

    for i, (row_id, smp, lat, lng) in enumerate(rows):
        # seccion_edificabilidad (slow: 3-5s)
        edif = fetch(
            f"https://epok.buenosaires.gob.ar/cur3d/seccion_edificabilidad/?smp={smp}",
            f"EDIF {smp}",
        )

        # enrase (fast)
        enrase_data = fetch(
            f"https://epok.buenosaires.gob.ar/cur3d/parcelas_plausibles_a_enrase/?smp={smp}",
            f"ENR {smp}",
        )

        if edif:
            alturas = edif.get("altura_max", [0, 0, 0, 0])
            while len(alturas) < 4:
                alturas.append(0)
            plusv = edif.get("plusvalia", {})
            fot = edif.get("fot", {})
            cat = edif.get("catalogacion", {})
            afect = edif.get("afectaciones", {})
            links = edif.get("link_imagen", {})
            linderas = edif.get("parcelas_linderas", {})
            enrase = 1 if (enrase_data and enrase_data.get("enrase")) else 0

            db_execute(conn,
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
                    cur3d_enriched=1
                WHERE id=?""",
                (
                    edif.get("sup_max_edificable"), edif.get("sup_edificable_planta"),
                    alturas[0], alturas[1], alturas[2], alturas[3],
                    edif.get("altura_max_plano_limite"),
                    fot.get("fot_medianera"), fot.get("fot_perim_libre"), fot.get("fot_semi_libre"),
                    plusv.get("incidencia_uva"), plusv.get("alicuota"),
                    edif.get("tipica"), 1 if edif.get("irregular") else 0,
                    edif.get("superficie_parcela"),
                    cat.get("proteccion"), cat.get("denominacion"),
                    afect.get("riesgo_hidrico", 0), afect.get("lep", 0),
                    afect.get("ensanche", 0), afect.get("apertura", 0),
                    enrase, json.dumps(linderas.get("smp_linderas", [])),
                    edif.get("rivolta", 0),
                    links.get("croquis_parcela"), links.get("perimetro_manzana"),
                    links.get("plano_indice"),
                    row_id,
                ),
            )
            ok += 1
        else:
            db_execute(conn, "UPDATE parcelas SET cur3d_enriched=-1 WHERE id=?", (row_id,))
            err += 1

        if (i + 1) % LOG_EVERY == 0:
            db_commit(conn)
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (total - i - 1) / rate / 3600
            print(f"[CUR3D] [{i+1:>7,}/{total:,}] ok={ok:,} err={err} {rate:.1f}/s ETA={eta:.1f}h", flush=True)

    db_commit(conn)
    print(f"[CUR3D] Done: ok={ok:,} err={err}", flush=True)


def main():
    conn = sqlite3.connect(str(DB_PATH), timeout=60, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")

    # Ensure columns exist
    cur = conn.cursor()
    existing = {r[1] for r in cur.execute("PRAGMA table_info(parcelas)").fetchall()}
    all_cols = {
        "epok_direccion": "TEXT", "epok_sup_cubierta": "REAL",
        "epok_propiedad_horizontal": "INTEGER", "epok_pisos_sobre": "INTEGER",
        "epok_pisos_bajo": "INTEGER", "epok_unidades_func": "INTEGER",
        "epok_locales": "INTEGER", "epok_calle": "TEXT", "epok_altura": "INTEGER",
        "epok_frente": "REAL", "epok_fondo": "REAL", "epok_sup_total": "REAL",
        "epok_enriched": "INTEGER DEFAULT 0", "delta_pisos": "INTEGER",
        "ratio_subutilizacion": "REAL",
        "edif_sup_max_edificable": "REAL", "edif_sup_edificable_planta": "REAL",
        "edif_altura_max_1": "REAL", "edif_altura_max_2": "REAL",
        "edif_altura_max_3": "REAL", "edif_altura_max_4": "REAL",
        "edif_plano_limite": "REAL",
        "edif_fot_medianera": "REAL", "edif_fot_perim_libre": "REAL",
        "edif_fot_semi_libre": "REAL",
        "edif_plusvalia_incidencia_uva": "REAL", "edif_plusvalia_alicuota": "REAL",
        "edif_tipica": "TEXT", "edif_irregular": "INTEGER",
        "edif_superficie_parcela": "REAL",
        "edif_catalogacion_proteccion": "TEXT", "edif_catalogacion_denominacion": "TEXT",
        "edif_riesgo_hidrico": "INTEGER", "edif_lep": "INTEGER",
        "edif_ensanche": "INTEGER", "edif_apertura": "INTEGER",
        "edif_enrase": "INTEGER", "edif_linderas": "TEXT", "edif_rivolta": "INTEGER",
        "edif_croquis_url": "TEXT", "edif_perimetro_url": "TEXT",
        "edif_plano_indice_url": "TEXT",
        "du_comuna": "TEXT", "du_barrio": "TEXT", "du_comisaria": "TEXT",
        "du_hospital": "TEXT", "du_distrito_escolar": "TEXT",
        "du_comisaria_vecinal": "TEXT", "du_distrito_economico": "TEXT",
        "cur3d_enriched": "INTEGER DEFAULT 0",
    }
    for col, dtype in all_cols.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE parcelas ADD COLUMN {col} {dtype}")
    conn.commit()

    t_epok = threading.Thread(target=run_epok, args=(conn,), name="EPOK")
    t_cur3d = threading.Thread(target=run_cur3d, args=(conn,), name="CUR3D")

    t_epok.start()
    t_cur3d.start()

    t_epok.join()
    t_cur3d.join()

    conn.close()
    print("\nAll done.", flush=True)


if __name__ == "__main__":
    main()
