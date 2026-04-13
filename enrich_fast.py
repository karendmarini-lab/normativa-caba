"""
Fast parallel enrichment: 12 concurrent CUR3D workers + 2 EPOK workers.
Uses ThreadPoolExecutor for concurrent HTTP requests.
Single DB connection with WAL mode. Idempotent.
"""

import json
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

DB_PATH = Path(__file__).parent / "caba_normativa.db"
CUR3D_WORKERS = 5
EPOK_WORKERS = 2
BATCH_SIZE = 50
LOG_EVERY = 200

HEADERS = {
    "Referer": "https://ciudad3d.buenosaires.gob.ar/",
    "User-Agent": "Mozilla/5.0",
}

db_lock = threading.Lock()


def fetch(url: str) -> dict | None:
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read())
    except Exception:
        return None


def db_write(conn, sql, params):
    for attempt in range(20):
        try:
            with db_lock:
                conn.execute(sql, params)
            return True
        except sqlite3.OperationalError:
            time.sleep(0.2 + attempt * 0.2)
    return False


def db_commit(conn):
    for attempt in range(20):
        try:
            with db_lock:
                conn.commit()
            return
        except sqlite3.OperationalError:
            time.sleep(0.2 + attempt * 0.2)


# ── EPOK worker ──

def process_epok(conn, row_id, smp, pisos_perm):
    data = fetch(f"https://epok.buenosaires.gob.ar/catastro/parcela/?smp={smp}")
    if not data:
        db_write(conn, "UPDATE parcelas SET epok_enriched=-1 WHERE id=?", (row_id,))
        return False

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
    delta = pisos_perm - pisos_s if pisos_s is not None and pisos_perm and pisos_perm > 0 else None
    ratio = round(pisos_s / pisos_perm, 3) if pisos_s is not None and pisos_perm and pisos_perm > 0 else None

    db_write(conn,
        """UPDATE parcelas SET
            epok_direccion=?, epok_sup_cubierta=?, epok_propiedad_horizontal=?,
            epok_pisos_sobre=?, epok_pisos_bajo=?, epok_unidades_func=?,
            epok_locales=?, epok_calle=?, epok_altura=?,
            epok_frente=?, epok_fondo=?, epok_sup_total=?,
            epok_enriched=1, delta_pisos=?, ratio_subutilizacion=?
        WHERE id=?""",
        (data.get("direccion", ""), _f(data.get("superficie_cubierta")),
         1 if data.get("propiedad_horizontal") == "Si" else 0,
         pisos_s, _i(data.get("pisos_bajo_rasante")),
         _i(data.get("unidades_funcionales")), _i(data.get("locales")),
         principal["calle"] if principal else "",
         principal.get("altura") if principal else None,
         _f(data.get("frente")), _f(data.get("fondo")),
         _f(data.get("superficie_total")),
         delta, ratio, row_id))
    return True


# ── CUR3D worker ──

def process_cur3d(conn, row_id, smp):
    edif = fetch(f"https://epok.buenosaires.gob.ar/cur3d/seccion_edificabilidad/?smp={smp}")
    enrase_data = fetch(f"https://epok.buenosaires.gob.ar/cur3d/parcelas_plausibles_a_enrase/?smp={smp}")

    if not edif:
        db_write(conn, "UPDATE parcelas SET cur3d_enriched=-1 WHERE id=?", (row_id,))
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
    enrase = 1 if (enrase_data and enrase_data.get("enrase")) else 0

    db_write(conn,
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
        (edif.get("sup_max_edificable"), edif.get("sup_edificable_planta"),
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
         row_id))
    return True


def run_pool(conn, name, worker_fn, rows, n_workers):
    total = len(rows)
    ok = 0
    err = 0
    t0 = time.time()

    print(f"[{name}] Starting {total:,} parcels with {n_workers} workers", flush=True)

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {}
        submitted = 0
        batch_start = 0

        while batch_start < total:
            batch_end = min(batch_start + BATCH_SIZE * n_workers, total)
            batch = rows[batch_start:batch_end]

            for row in batch:
                future = pool.submit(worker_fn, conn, *row)
                futures[future] = row[1]  # smp
                submitted += 1

            for future in as_completed(futures):
                if future.result():
                    ok += 1
                else:
                    err += 1

            futures.clear()
            db_commit(conn)
            batch_start = batch_end

            elapsed = time.time() - t0
            rate = (ok + err) / elapsed if elapsed > 0 else 0
            remaining = (total - ok - err) / rate / 3600 if rate > 0 else 0
            print(
                f"[{name}] {ok + err:>7,}/{total:,} ok={ok:,} err={err} "
                f"{rate:.1f}/s ETA={remaining:.1f}h",
                flush=True,
            )

    print(f"[{name}] Done: ok={ok:,} err={err}", flush=True)


def main():
    conn = sqlite3.connect(str(DB_PATH), timeout=60, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")

    cur = conn.cursor()

    # Get pending rows
    epok_rows = cur.execute(
        "SELECT id, smp, pisos FROM parcelas WHERE COALESCE(epok_enriched,0)<=0 ORDER BY id"
    ).fetchall()

    cur3d_rows = cur.execute(
        "SELECT id, smp FROM parcelas WHERE COALESCE(cur3d_enriched,0)<=0 ORDER BY id"
    ).fetchall()

    print(f"Pending: EPOK={len(epok_rows):,} CUR3D={len(cur3d_rows):,}", flush=True)

    # Run both in parallel threads
    t_epok = threading.Thread(
        target=run_pool,
        args=(conn, "EPOK", process_epok, epok_rows, EPOK_WORKERS),
    )
    t_cur3d = threading.Thread(
        target=run_pool,
        args=(conn, "CUR3D", process_cur3d, cur3d_rows, CUR3D_WORKERS),
    )

    t_epok.start()
    t_cur3d.start()
    t_epok.join()
    t_cur3d.join()

    conn.close()
    print("\nAll done.", flush=True)


if __name__ == "__main__":
    main()
