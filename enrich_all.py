"""
Unified enrichment: EPOK + CUR3D intercalated.
Single DB connection, no lock contention.
Logs every 100 parcels. Safe to interrupt and resume.
"""

import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DB_PATH = Path(__file__).parent / "caba_normativa.db"
DELAY_S = 0.0  # API is already slow (3-7s for edificabilidad), no artificial delay
LOG_EVERY = 50
STALL_THRESHOLD_S = 15  # warn if a single call takes longer than this

HEADERS = {
    "Referer": "https://ciudad3d.buenosaires.gob.ar/",
    "User-Agent": "Mozilla/5.0",
}


def fetch(url: str, label: str = "") -> dict | None:
    req = urllib.request.Request(url, headers=HEADERS)
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
        elapsed = time.time() - t0
        if elapsed > STALL_THRESHOLD_S:
            print(f"  STALL: {label} took {elapsed:.1f}s ({url.split('?')[0].split('gob.ar/')[1]})", flush=True)
        return data
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  FAIL: {label} after {elapsed:.1f}s → {e}", flush=True)
        return None


def enrich_epok(smp: str) -> dict | None:
    data = fetch(f"https://epok.buenosaires.gob.ar/catastro/parcela/?smp={smp}", f"EPOK {smp}")
    if not data:
        return None
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

    return {
        "dir": data.get("direccion", ""),
        "sup_cub": _f(data.get("superficie_cubierta")),
        "sup_tot": _f(data.get("superficie_total")),
        "fr": _f(data.get("frente")),
        "fo": _f(data.get("fondo")),
        "ph": 1 if data.get("propiedad_horizontal") == "Si" else 0,
        "pisos_s": _i(data.get("pisos_sobre_rasante")),
        "pisos_b": _i(data.get("pisos_bajo_rasante")),
        "uf": _i(data.get("unidades_funcionales")),
        "loc": _i(data.get("locales")),
        "calle": principal["calle"] if principal else "",
        "altura": principal.get("altura") if principal else None,
    }


def enrich_cur3d(smp: str) -> dict | None:
    data = fetch(f"https://epok.buenosaires.gob.ar/cur3d/seccion_edificabilidad/?smp={smp}", f"EDIF {smp}")
    if not data:
        return None

    alturas = data.get("altura_max", [0, 0, 0, 0])
    while len(alturas) < 4:
        alturas.append(0)
    plusv = data.get("plusvalia", {})
    fot = data.get("fot", {})
    cat = data.get("catalogacion", {})
    afect = data.get("afectaciones", {})
    links = data.get("link_imagen", {})
    linderas = data.get("parcelas_linderas", {})

    return {
        "sup_max": data.get("sup_max_edificable"),
        "sup_planta": data.get("sup_edificable_planta"),
        "alt": alturas,
        "pl": data.get("altura_max_plano_limite"),
        "fot_m": fot.get("fot_medianera"),
        "fot_pl": fot.get("fot_perim_libre"),
        "fot_sl": fot.get("fot_semi_libre"),
        "uva": plusv.get("incidencia_uva"),
        "alic": plusv.get("alicuota"),
        "tipica": data.get("tipica"),
        "irreg": 1 if data.get("irregular") else 0,
        "sup_parc": data.get("superficie_parcela"),
        "cat_prot": cat.get("proteccion"),
        "cat_denom": cat.get("denominacion"),
        "rh": afect.get("riesgo_hidrico", 0),
        "lep": afect.get("lep", 0),
        "ens": afect.get("ensanche", 0),
        "ape": afect.get("apertura", 0),
        "linderas": json.dumps(linderas.get("smp_linderas", [])),
        "rivolta": data.get("rivolta", 0),
        "croquis": links.get("croquis_parcela"),
        "perim": links.get("perimetro_manzana"),
        "plano_i": links.get("plano_indice"),
    }


def enrich_enrase(smp: str) -> int | None:
    data = fetch(f"https://epok.buenosaires.gob.ar/cur3d/parcelas_plausibles_a_enrase/?smp={smp}", f"ENRASE {smp}")
    if data is None:
        return None
    return 1 if data.get("enrase") else 0


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")

    # Ensure all columns exist
    cur_tmp = conn.cursor()
    existing = {r[1] for r in cur_tmp.execute("PRAGMA table_info(parcelas)").fetchall()}
    epok_cols = {
        "epok_direccion": "TEXT", "epok_sup_cubierta": "REAL",
        "epok_propiedad_horizontal": "INTEGER", "epok_pisos_sobre": "INTEGER",
        "epok_pisos_bajo": "INTEGER", "epok_unidades_func": "INTEGER",
        "epok_locales": "INTEGER", "epok_calle": "TEXT", "epok_altura": "INTEGER",
        "epok_frente": "REAL", "epok_fondo": "REAL", "epok_sup_total": "REAL",
        "epok_enriched": "INTEGER DEFAULT 0", "delta_pisos": "INTEGER",
        "ratio_subutilizacion": "REAL",
    }
    cur3d_cols = {
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
    for col, dtype in {**epok_cols, **cur3d_cols}.items():
        if col not in existing:
            cur_tmp.execute(f"ALTER TABLE parcelas ADD COLUMN {col} {dtype}")
    conn.commit()

    cur = conn.cursor()

    # Find parcels that need either enrichment
    cur.execute("""
        SELECT id, smp, lat, lng, pisos,
               COALESCE(epok_enriched, 0) as epok_done,
               COALESCE(cur3d_enriched, 0) as cur3d_done
        FROM parcelas
        WHERE COALESCE(epok_enriched, 0) <= 0
           OR COALESCE(cur3d_enriched, 0) <= 0
        ORDER BY id
    """)
    rows = cur.fetchall()

    total = len(rows)
    if total == 0:
        print("Nothing to do.")
        return

    # Count what needs what
    need_epok = sum(1 for r in rows if r[5] <= 0)
    need_cur3d = sum(1 for r in rows if r[6] <= 0)
    print(f"Pending: {total:,} parcels ({need_epok:,} need EPOK, {need_cur3d:,} need CUR3D)")
    print(f"Delay: {DELAY_S}s/parcel, logging every {LOG_EVERY}")
    print(f"", flush=True)

    epok_ok = 0
    epok_err = 0
    cur3d_ok = 0
    cur3d_err = 0
    t0 = time.time()

    for i, (row_id, smp, lat, lng, pisos_perm, epok_done, cur3d_done) in enumerate(rows):

        # ── EPOK ──
        if epok_done <= 0:
            ep = enrich_epok(smp)
            if ep:
                delta = None
                ratio = None
                if ep["pisos_s"] is not None and pisos_perm and pisos_perm > 0:
                    delta = pisos_perm - ep["pisos_s"]
                    ratio = round(ep["pisos_s"] / pisos_perm, 3)
                for _a in range(10):
                    try:
                        cur.execute(
                            """UPDATE parcelas SET
                                epok_direccion=?, epok_sup_cubierta=?, epok_propiedad_horizontal=?,
                                epok_pisos_sobre=?, epok_pisos_bajo=?, epok_unidades_func=?,
                                epok_locales=?, epok_calle=?, epok_altura=?,
                                epok_frente=?, epok_fondo=?, epok_sup_total=?,
                                epok_enriched=1, delta_pisos=?, ratio_subutilizacion=?
                            WHERE id=?""",
                            (ep["dir"], ep["sup_cub"], ep["ph"],
                             ep["pisos_s"], ep["pisos_b"], ep["uf"],
                             ep["loc"], ep["calle"], ep["altura"],
                             ep["fr"], ep["fo"], ep["sup_tot"],
                             delta, ratio, row_id),
                        )
                        break
                    except sqlite3.OperationalError:
                        time.sleep(0.5)
                epok_ok += 1
            else:
                cur.execute("UPDATE parcelas SET epok_enriched=-1 WHERE id=?", (row_id,))
                epok_err += 1

        # ── CUR3D ──
        if cur3d_done <= 0:
            c3 = enrich_cur3d(smp)
            enrase = enrich_enrase(smp)
            if c3:
                for _a in range(10):
                    try:
                        cur.execute(
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
                            (c3["sup_max"], c3["sup_planta"],
                             c3["alt"][0], c3["alt"][1], c3["alt"][2], c3["alt"][3],
                             c3["pl"],
                             c3["fot_m"], c3["fot_pl"], c3["fot_sl"],
                             c3["uva"], c3["alic"],
                             c3["tipica"], c3["irreg"], c3["sup_parc"],
                             c3["cat_prot"], c3["cat_denom"],
                             c3["rh"], c3["lep"], c3["ens"], c3["ape"],
                             enrase, c3["linderas"], c3["rivolta"],
                             c3["croquis"], c3["perim"], c3["plano_i"],
                             row_id),
                        )
                        break
                    except sqlite3.OperationalError:
                        time.sleep(0.5)
                cur3d_ok += 1
            else:
                cur.execute("UPDATE parcelas SET cur3d_enriched=-1 WHERE id=?", (row_id,))
                cur3d_err += 1

        # Commit + log
        if (i + 1) % LOG_EVERY == 0:
            conn.commit()
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            remaining = (total - i - 1) / rate / 3600
            print(
                f"[{i+1:>7,}/{total:,}] "
                f"EPOK ok={epok_ok:,} err={epok_err} | "
                f"CUR3D ok={cur3d_ok:,} err={cur3d_err} | "
                f"{rate:.1f}/s ETA={remaining:.1f}h",
                flush=True,
            )

        time.sleep(DELAY_S)

    conn.commit()
    conn.close()
    elapsed_h = (time.time() - t0) / 3600
    print(f"\nDone in {elapsed_h:.1f}h — EPOK={epok_ok:,} CUR3D={cur3d_ok:,}")


if __name__ == "__main__":
    main()
