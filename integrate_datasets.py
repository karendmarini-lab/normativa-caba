"""
Integrate BA Data open datasets into caba_normativa.db.

Imports: tejido urbano, usos suelo, obras iniciadas, obras registradas,
certificados urbanísticos, and parcela polygons. Joins to existing parcelas
table by SMP.
"""

import csv
import re
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "caba_normativa.db"
DATA_DIR = Path(__file__).parent / "data"
BATCH_SIZE = 500  # commit in small batches to avoid lock contention


def retry_execute(conn: sqlite3.Connection, sql: str, params: tuple = (),
                  max_retries: int = 10) -> int:
    """Execute SQL with retry on database lock."""
    import time
    for attempt in range(max_retries):
        try:
            cur = conn.execute(sql, params)
            return cur.rowcount
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < max_retries - 1:
                time.sleep(0.5 + attempt * 0.5)
            else:
                raise
    return 0


def retry_commit(conn: sqlite3.Connection, max_retries: int = 10) -> None:
    """Commit with retry on database lock."""
    import time
    for attempt in range(max_retries):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < max_retries - 1:
                time.sleep(0.5 + attempt * 0.5)
            else:
                raise


def ensure_columns(cur: sqlite3.Cursor, conn: sqlite3.Connection,
                    cols: list[tuple[str, str]]) -> None:
    """Add columns to parcelas if they don't exist."""
    import time
    for attempt in range(10):
        try:
            existing = {r[1] for r in cur.execute("PRAGMA table_info(parcelas)").fetchall()}
            for col, dtype in cols:
                if col not in existing:
                    cur.execute(f"ALTER TABLE parcelas ADD COLUMN {col} {dtype}")
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < 9:
                time.sleep(1 + attempt)
            else:
                raise


def smp_norm(smp: str) -> str:
    """Normalize SMP removing leading zeros and spaces."""
    if not smp:
        return ""
    smp = smp.replace(" ", "")
    parts = smp.upper().split("-")
    normalized = []
    for p in parts:
        digits = re.sub(r"[^0-9]", "", p)
        letters = re.sub(r"[^A-Z]", "", p)
        if digits:
            normalized.append(str(int(digits)) + letters)
        else:
            normalized.append(p)
    return "-".join(normalized)


def import_tejido(conn: sqlite3.Connection) -> None:
    """Import tejido urbano: real building heights by SMP (photogrammetry).

    Multiple rows per SMP (one per building on the parcel). We aggregate
    to get max height and count of structures.
    """
    print("=== Importing Tejido Urbano ===")
    cur = conn.cursor()

    ensure_columns(cur, conn, [
        ("tejido_altura_max", "REAL"),
        ("tejido_altura_avg", "REAL"),
        ("tejido_estructuras", "INTEGER"),
        ("tejido_tipo", "TEXT"),
        ("delta_altura", "REAL"),
    ])

    # Read and aggregate tejido by SMP
    path = DATA_DIR / "tejido.csv"
    tejido: dict[str, dict] = {}
    with open(path, newline="", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            smp = smp_norm(row.get("smp", ""))
            if not smp:
                continue
            try:
                altura = float(row.get("altura", 0) or 0)
            except ValueError:
                altura = 0
            tipo = row.get("tipo", "")

            if smp not in tejido:
                tejido[smp] = {"max": altura, "sum": altura, "count": 1, "tipo": tipo}
            else:
                t = tejido[smp]
                t["max"] = max(t["max"], altura)
                t["sum"] += altura
                t["count"] += 1

    print(f"  {len(tejido):,} unique SMPs with tejido data")

    # Update parcelas
    matched = 0
    for i, (smp, t) in enumerate(tejido.items()):
        avg = round(t["sum"] / t["count"], 2) if t["count"] > 0 else 0
        rc = retry_execute(
            conn,
            """UPDATE parcelas SET
                tejido_altura_max=?, tejido_altura_avg=?, tejido_estructuras=?,
                tejido_tipo=?, delta_altura=ROUND(plano_san - ?, 1)
            WHERE smp_norm=?""",
            (t["max"], avg, t["count"], t["tipo"], t["max"], smp),
        )
        if rc > 0:
            matched += 1
        if (i + 1) % BATCH_SIZE == 0:
            retry_commit(conn)

    retry_commit(conn)
    print(f"  Matched {matched:,} parcelas with tejido data")


def import_usos_suelo(conn: sqlite3.Connection) -> None:
    """Import usos del suelo 2022-2024: current land use per parcel."""
    print("\n=== Importing Usos del Suelo 2022-2024 ===")
    cur = conn.cursor()

    ensure_columns(cur, conn, [
        ("uso_tipo1", "TEXT"),
        ("uso_tipo2", "TEXT"),
        ("uso_estado", "TEXT"),
        ("uso_pisos", "INTEGER"),
        ("uso_calle", "TEXT"),
        ("uso_puerta", "TEXT"),
        ("uso_anio", "INTEGER"),
    ])

    path = DATA_DIR / "usos_suelo_2022_2024.csv"
    # Multiple rows per SMP possible — keep the most recent
    usos: dict[str, dict] = {}
    with open(path, newline="", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            smp = smp_norm(row.get("SMP", ""))
            if not smp:
                continue
            try:
                anio = int(row.get("AÑO", 0) or row.get("A\u00d1O", 0) or 0)
            except ValueError:
                anio = 0

            if smp not in usos or anio > usos[smp].get("anio", 0):
                usos[smp] = {
                    "tipo1": row.get("TIPO1", ""),
                    "tipo2": row.get("TIPO2", ""),
                    "estado": row.get("ESTADO", ""),
                    "pisos": row.get("PISOS", ""),
                    "calle": row.get("CALLE", ""),
                    "puerta": row.get("PUERTA", ""),
                    "anio": anio,
                }

    print(f"  {len(usos):,} unique SMPs with uso data")

    matched = 0
    for i, (smp, u) in enumerate(usos.items()):
        try:
            pisos = int(u["pisos"]) if u["pisos"] else None
        except ValueError:
            pisos = None
        rc = retry_execute(
            conn,
            """UPDATE parcelas SET
                uso_tipo1=?, uso_tipo2=?, uso_estado=?, uso_pisos=?,
                uso_calle=?, uso_puerta=?, uso_anio=?
            WHERE smp_norm=?""",
            (u["tipo1"], u["tipo2"], u["estado"], pisos,
             u["calle"], u["puerta"], u["anio"], smp),
        )
        if rc > 0:
            matched += 1
        if (i + 1) % BATCH_SIZE == 0:
            retry_commit(conn)

    retry_commit(conn)
    print(f"  Matched {matched:,} parcelas with uso data")


def import_obras_iniciadas(conn: sqlite3.Connection) -> None:
    """Import obras iniciadas: active construction projects."""
    print("\n=== Importing Obras Iniciadas ===")
    cur = conn.cursor()

    ensure_columns(cur, conn, [
        ("obra_tipo", "TEXT"),
        ("obra_destino", "TEXT"),
        ("obra_m2", "REAL"),
        ("obra_estado", "TEXT"),
        ("obra_fecha_inicio", "TEXT"),
        ("obra_expediente", "TEXT"),
    ])

    path = DATA_DIR / "obras_iniciadas.csv"
    obras: dict[str, dict] = {}
    with open(path, newline="", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sec = row.get("seccion", "").strip()
            mza = row.get("manzana", "").strip()
            par = row.get("parcela", "").strip()
            if not sec or not mza or not par:
                continue
            smp = smp_norm(f"{sec}-{mza}-{par}")
            if not smp:
                continue

            m2_str = (row.get("metrosaconstruir", "") or "").replace(",", "").replace(".", "").strip()
            try:
                m2 = float(row.get("metrosaconstruir", "0").replace(",", "")) if row.get("metrosaconstruir") else 0
            except ValueError:
                m2 = 0

            obras[smp] = {
                "tipo": row.get("tipo_obra", ""),
                "destino": row.get("destino", ""),
                "m2": m2,
                "estado": row.get("estadotramite", ""),
                "fecha": row.get("fecha_inicio_obra", ""),
                "exp": row.get("exp_dgroc", ""),
            }

    print(f"  {len(obras):,} unique SMPs with obra data")

    matched = 0
    for i, (smp, o) in enumerate(obras.items()):
        rc = retry_execute(
            conn,
            """UPDATE parcelas SET
                obra_tipo=?, obra_destino=?, obra_m2=?, obra_estado=?,
                obra_fecha_inicio=?, obra_expediente=?
            WHERE smp_norm=?""",
            (o["tipo"], o["destino"], o["m2"], o["estado"],
             o["fecha"], o["exp"], smp),
        )
        if rc > 0:
            matched += 1
        if (i + 1) % BATCH_SIZE == 0:
            retry_commit(conn)

    retry_commit(conn)
    print(f"  Matched {matched:,} parcelas with obra data")


def import_obras_registradas(conn: sqlite3.Connection) -> None:
    """Import obras registradas: registered construction permits."""
    print("\n=== Importing Obras Registradas ===")
    cur = conn.cursor()

    ensure_columns(cur, conn, [
        ("obra_reg_tipo", "TEXT"),
        ("obra_reg_fecha", "TEXT"),
        ("obra_reg_expediente", "TEXT"),
        ("obra_reg_ubicacion", "TEXT"),
    ])

    path = DATA_DIR / "obras_registradas.csv"
    obras: dict[str, dict] = {}
    with open(path, newline="", errors="replace") as f:
        # This CSV uses semicolons
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            smp_raw = row.get("smp", "")
            smp = smp_norm(smp_raw)
            if not smp:
                continue

            obras[smp] = {
                "tipo": row.get("descripcio", ""),
                "fecha": row.get("fecha", ""),
                "exp": row.get("expediente", ""),
                "ubicacion": row.get("ubicacion", ""),
            }

    print(f"  {len(obras):,} unique SMPs with obra registrada")

    matched = 0
    for i, (smp, o) in enumerate(obras.items()):
        rc = retry_execute(
            conn,
            """UPDATE parcelas SET
                obra_reg_tipo=?, obra_reg_fecha=?, obra_reg_expediente=?,
                obra_reg_ubicacion=?
            WHERE smp_norm=?""",
            (o["tipo"], o["fecha"], o["exp"], o["ubicacion"], smp),
        )
        if rc > 0:
            matched += 1
        if (i + 1) % BATCH_SIZE == 0:
            retry_commit(conn)

    retry_commit(conn)
    print(f"  Matched {matched:,} parcelas with obra registrada")


def import_certificados(conn: sqlite3.Connection) -> None:
    """Import certificados urbanísticos: normativa queries by parcel."""
    print("\n=== Importing Certificados Urbanísticos ===")
    cur = conn.cursor()

    ensure_columns(cur, conn, [
        ("cert_anio", "INTEGER"),
        ("cert_obra", "TEXT"),
        ("cert_fecha_egreso", "TEXT"),
    ])

    path = DATA_DIR / "certificados_urbanisticos.csv"
    certs: dict[str, dict] = {}
    with open(path, newline="", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            smp = smp_norm(row.get("\ufeffSMP", row.get("SMP", "")))
            if not smp:
                continue
            try:
                anio = int(row.get("ANIO", 0) or 0)
            except ValueError:
                anio = 0

            if smp not in certs or anio > certs[smp].get("anio", 0):
                certs[smp] = {
                    "anio": anio,
                    "obra": row.get("OBRA", ""),
                    "fecha": row.get("FECHA_EGRESO", ""),
                }

    print(f"  {len(certs):,} unique SMPs with certificado")

    matched = 0
    for i, (smp, c) in enumerate(certs.items()):
        rc = retry_execute(
            conn,
            """UPDATE parcelas SET cert_anio=?, cert_obra=?, cert_fecha_egreso=?
            WHERE smp_norm=?""",
            (c["anio"], c["obra"], c["fecha"], smp),
        )
        if rc > 0:
            matched += 1
        if (i + 1) % BATCH_SIZE == 0:
            retry_commit(conn)

    retry_commit(conn)
    print(f"  Matched {matched:,} parcelas with certificado")


def import_parcela_metadata(conn: sqlite3.Connection) -> None:
    """Import barrio and comuna from parcelas CSV (skip geometry for now)."""
    print("\n=== Importing Parcela Metadata (barrio, comuna) ===")
    cur = conn.cursor()

    ensure_columns(cur, conn, [
        ("barrio", "TEXT"),
        ("comuna", "TEXT"),
        ("partida_matriz", "TEXT"),
    ])

    path = DATA_DIR / "parcelas.csv"
    if not path.exists():
        print("  parcelas.csv not found, skipping")
        return

    parcelas: dict[str, dict] = {}
    with open(path, newline="", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            smp = smp_norm(row.get("\ufeffsmp", row.get("smp", "")))
            if not smp:
                continue
            parcelas[smp] = {
                "barrio": row.get("barrio", ""),
                "comuna": row.get("comuna", ""),
                "partida": row.get("partida_ma", ""),
            }

    print(f"  {len(parcelas):,} unique SMPs in parcelas CSV")

    matched = 0
    for i, (smp, p) in enumerate(parcelas.items()):
        rc = retry_execute(
            conn,
            "UPDATE parcelas SET barrio=?, comuna=?, partida_matriz=? WHERE smp_norm=?",
            (p["barrio"], p["comuna"], p["partida"], smp),
        )
        if rc > 0:
            matched += 1
        if (i + 1) % BATCH_SIZE == 0:
            retry_commit(conn)

    retry_commit(conn)
    print(f"  Matched {matched:,} parcelas with barrio/comuna")


def print_summary(conn: sqlite3.Connection) -> None:
    """Print enrichment summary."""
    cur = conn.cursor()
    print("\n" + "=" * 60)
    print("  ENRICHMENT SUMMARY")
    print("=" * 60)

    checks = [
        ("Total parcelas", "SELECT COUNT(*) FROM parcelas"),
        ("Con tejido (altura real)", "SELECT COUNT(*) FROM parcelas WHERE tejido_altura_max IS NOT NULL"),
        ("Con uso suelo", "SELECT COUNT(*) FROM parcelas WHERE uso_tipo1 IS NOT NULL AND uso_tipo1 != ''"),
        ("Con obra iniciada", "SELECT COUNT(*) FROM parcelas WHERE obra_tipo IS NOT NULL AND obra_tipo != ''"),
        ("Con obra registrada", "SELECT COUNT(*) FROM parcelas WHERE obra_reg_tipo IS NOT NULL AND obra_reg_tipo != ''"),
        ("Con certificado", "SELECT COUNT(*) FROM parcelas WHERE cert_anio IS NOT NULL AND cert_anio > 0"),
        ("Con barrio/comuna", "SELECT COUNT(*) FROM parcelas WHERE barrio IS NOT NULL AND barrio != ''"),
        ("Con EPOK (enriching...)", "SELECT COUNT(*) FROM parcelas WHERE epok_enriched = 1"),
        ("Subutilizadas (delta>5 pisos)", "SELECT COUNT(*) FROM parcelas WHERE delta_altura > 15"),
    ]
    for label, sql in checks:
        cur.execute(sql)
        val = cur.fetchone()[0]
        print(f"  {label:<35} {val:>10,}")

    # Top subutilized
    print("\n  Top 5 parcelas más subutilizadas (delta altura):")
    cur.execute("""
        SELECT smp, cpu, plano_san, tejido_altura_max, delta_altura, area, barrio
        FROM parcelas
        WHERE delta_altura > 0 AND area > 100
        ORDER BY delta_altura DESC LIMIT 5
    """)
    for r in cur.fetchall():
        print(f"    SMP={r[0]} CPU={r[1]} PL={r[2]}m construido={r[3]}m delta={r[4]}m area={r[5]}m² {r[6] or ''}")

    # Top uses in high-rise zones
    print("\n  Usos más comunes en zonas de >8 pisos permitidos:")
    cur.execute("""
        SELECT uso_tipo1, COUNT(*) as n
        FROM parcelas
        WHERE pisos >= 8 AND uso_tipo1 IS NOT NULL AND uso_tipo1 != ''
        GROUP BY uso_tipo1 ORDER BY n DESC LIMIT 8
    """)
    for r in cur.fetchall():
        print(f"    {r[0]:<40} {r[1]:>6,}")

    print("=" * 60)


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    import_tejido(conn)
    import_usos_suelo(conn)
    import_obras_iniciadas(conn)
    import_obras_registradas(conn)
    import_certificados(conn)
    import_parcela_metadata(conn)
    print_summary(conn)

    conn.close()


if __name__ == "__main__":
    main()
