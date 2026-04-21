"""
Feasibility model for Zonaprop terrenos × EdificIA normativa.

Computes asking incidencia vs max feasible incidencia per listing.
Incidencia = land cost / m² vendible — THE metric developers use.

Assumptions (calibrated from 108 real Zonaprop listings):
  - Ratio vendible/construible: 0.78 (market average)
  - Construction cost: tiered by height
  - Cocheras: 1 per 120m² vendible, USD 35k each
  - Comercialización + impuestos: 8.5% of revenue
  - Honorarios: 12% of construction
  - Financial: 8% annual on deployed capital
  - Target: 10% annual ROI on total cost
"""

import json
import re
import sqlite3
from dataclasses import dataclass

ZP_DB = "zonaprop.db"
ED_DB = "caba_normativa.db"

# Calibrated from 108 listings with both construible and vendible
RATIO_VENDIBLE = 0.80  # average of density-based ratio (0.86→0.60)

# Construction cost USD/m² by floor count
COST_PER_M2 = {3: 1050, 7: 1300, 12: 1550, 99: 1800}

# Timeline months by floor count
TIMELINE = {3: 15, 7: 21, 12: 30, 99: 36}

TARGET_ROI_ANNUAL = 0.10
FINANCIAL_RATE = 0.08
COCHERA_COST = 35000
COCHERA_RATIO = 120  # 1 per 120m² vendible
COMERC_IMP_PCT = 0.085
HONORARIOS_PCT = 0.12
DEMOL_COST_M2 = 80
UVA_TO_USD = 0.81


def get_tier_value(pisos: int, table: dict) -> int | float:
    for threshold in sorted(table.keys()):
        if pisos <= threshold:
            return table[threshold]
    return table[max(table.keys())]


@dataclass
class ManzanaProfile:
    """Normative profile for a manzana (city block)."""
    manzana: str
    barrio: str
    pisos: int
    plano_san: float
    cur_distrito: str
    fot_efectivo: float  # m² edificable por m² de lote
    plusv_per_m2: float  # plusvalia cost per m² de lote
    avg_existing_m2: float  # avg existing building to demolish


@dataclass
class Listing:
    """A Zonaprop terreno listing with estimated vendibles."""
    posting_id: str
    precio_usd: float
    direccion: str
    barrio: str
    zp_superficie: float
    m2_vendibles: float
    m2v_source: str  # "descripcion", "zp_vendibles", "calculado"
    manzana: str
    url: str


@dataclass
class Feasibility:
    """Feasibility result for a listing."""
    listing: Listing
    profile: ManzanaProfile
    asking_incid: float  # USD/m² vendible (what seller asks)
    max_incid: float  # USD/m² vendible (max for 10% annual)
    gap_pct: float  # how much overpriced (positive = overpriced)
    venta_m2: float  # sale price per m² in this barrio
    n_comps: int


def extract_vendibles_from_description(desc: str) -> float | None:
    """Try to extract m² vendibles from listing description."""
    desc_l = desc.lower()
    # Patterns like "vendible: 883" or "883 m2 vendible" or "vendibles: 883"
    patterns = [
        r'vendible[s]?\s*[:=]?\s*([\d.,]+)',
        r'([\d.,]+)\s*m[2²]?\s*vendible',
        r'vendible[s]?\s*(?:estimad[oa]s?)?\s*[:=]?\s*([\d.,]+)',
    ]
    for pat in patterns:
        matches = re.findall(pat, desc_l)
        for m in matches:
            try:
                val = float(m.replace(".", "").replace(",", "."))
                if 100 < val < 50000:
                    return val
            except ValueError:
                continue
    return None


def build_manzana_profiles(ed: sqlite3.Connection) -> dict[str, ManzanaProfile]:
    """Build normative profiles per manzana from EdificIA data."""
    rows = ed.execute("""
        SELECT
            substr(smp_norm, 1, instr(smp_norm, '-') - 1) || '-' ||
            substr(smp_norm, instr(smp_norm, '-') + 1,
                instr(substr(smp_norm, instr(smp_norm, '-') + 1), '-') - 1) as manzana,
            MAX(barrio) as barrio,
            ROUND(AVG(pisos)) as pisos,
            ROUND(AVG(plano_san), 1) as plano_san,
            MAX(cur_distrito) as cur_distrito,
            AVG(CASE
                WHEN edif_sup_edificable_planta > 0 AND area > 0
                     AND edif_sup_edificable_planta / area < 1
                THEN edif_sup_edificable_planta / area * pisos
                WHEN edif_sup_edificable_planta > 0 AND area > 0
                     AND edif_sup_edificable_planta / area >= 1
                THEN edif_sup_edificable_planta / area
                ELSE pisos * 0.65
            END) as fot_ef,
            AVG(COALESCE(
                edif_plusvalia_incidencia_uva * edif_plusvalia_alicuota, 0
            )) as plusv_per_m2,
            AVG(COALESCE(epok_sup_cubierta, 0)) as avg_exist
        FROM parcelas
        GROUP BY 1
    """).fetchall()

    profiles = {}
    for r in rows:
        mza, barrio, pisos, plano, dist, fot, plusv, exist = r
        if pisos and pisos >= 1:
            profiles[mza] = ManzanaProfile(
                manzana=mza, barrio=barrio, pisos=int(pisos),
                plano_san=plano or 0, cur_distrito=dist or "",
                fot_efectivo=fot or 0, plusv_per_m2=plusv or 0,
                avg_existing_m2=exist or 0,
            )
    return profiles


def build_depto_prices(zp: sqlite3.Connection) -> dict[str, tuple[float, int]]:
    """Average depto sale price per barrio (blended new+used)."""
    rows = zp.execute("""
        SELECT
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(barrio,
                ', Capital Federal', ''), ', Palermo', ''),
                ', Belgrano', ''), ', Almagro', ''),
                ', Caballito', '') as b,
            AVG(precio_usd / superficie_m2) as vm2,
            COUNT(*) as n
        FROM listings
        WHERE tipo='departamentos' AND operacion='venta'
            AND precio_usd > 10000 AND superficie_m2 BETWEEN 20 AND 300
        GROUP BY 1 HAVING n >= 10
    """).fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}


def estimate_m2_vendibles(
    zp_sup: float,
    descripcion: str,
    epok_area: float,
    fot_efectivo: float,
    pisos: int,
) -> tuple[float, str]:
    """Estimate m² vendibles from best available source."""
    # 1. Try extracting from description (most reliable)
    from_desc = extract_vendibles_from_description(descripcion)
    if from_desc:
        return from_desc, "descripcion"

    # 2. If ZP superficie >> EPOK area, seller put vendibles
    if epok_area > 0 and zp_sup / epok_area > 1.5:
        return zp_sup, "zp_es_vendibles"

    # 3. Compute from lot area
    lot_area = zp_sup
    if epok_area > 0 and 0.5 < zp_sup / epok_area < 1.5:
        lot_area = (zp_sup + epok_area) / 2  # average both
    elif epok_area > 0:
        lot_area = epok_area  # trust EPOK

    m2c = lot_area * fot_efectivo
    m2v = m2c * RATIO_VENDIBLE
    return m2v, "calculado"


def compute_max_incidencia(
    m2v: float,
    pisos: int,
    venta_m2: float,
    plusv_per_m2_lote: float,
    lot_area_approx: float,
    avg_existing_m2: float,
) -> float:
    """Max land price per m² vendible for target annual ROI."""
    m2c = m2v / RATIO_VENDIBLE
    cc = get_tier_value(pisos, COST_PER_M2)
    meses = get_tier_value(pisos, TIMELINE)
    t = TARGET_ROI_ANNUAL * meses / 12  # target total ROI over period

    revenue = m2v * venta_m2

    # Costs excluding land
    construction = m2c * cc * (1 + HONORARIOS_PCT)
    cocheras = (m2v / COCHERA_RATIO) * COCHERA_COST
    comerc_imp = revenue * COMERC_IMP_PCT
    plusvalia = plusv_per_m2_lote * lot_area_approx * UVA_TO_USD
    demolition = avg_existing_m2 * DEMOL_COST_M2
    fin_construction = 0.5 * m2c * cc * FINANCIAL_RATE * meses / 12

    cost_ex_land = (
        construction + cocheras + comerc_imp
        + plusvalia + demolition + fin_construction
    )

    # Solve: (revenue - land - cost_ex_land) / (land + cost_ex_land) = t
    # land_max = (revenue - cost_ex_land * (1+t)) / (1+t) / (1 + fin_factor)
    fin_factor = FINANCIAL_RATE * meses / 12  # financial cost factor on land
    land_max = (revenue - cost_ex_land * (1 + t)) / ((1 + fin_factor) * (1 + t))

    if land_max <= 0 or m2v <= 0:
        return 0.0
    return land_max / m2v


def run() -> None:
    zp = sqlite3.connect(ZP_DB)
    ed = sqlite3.connect(ED_DB)

    print("Building manzana profiles...", flush=True)
    profiles = build_manzana_profiles(ed)
    print(f"  {len(profiles)} manzanas", flush=True)

    print("Building depto prices...", flush=True)
    prices = build_depto_prices(zp)
    print(f"  {len(prices)} barrios", flush=True)

    # Get matched terrenos
    rows = zp.execute("""
        SELECT m.posting_id, l.precio_usd, l.direccion, l.barrio,
            l.superficie_m2, l.descripcion, m.manzana, m.smp_nearest, l.url
        FROM matches m
        JOIN listings l ON m.posting_id = l.posting_id
        WHERE l.precio_usd > 10000 AND l.tipo = 'terrenos'
    """).fetchall()

    results: list[Feasibility] = []
    skipped = {"no_profile": 0, "no_price": 0, "no_m2v": 0, "no_incid": 0}

    for row in rows:
        pid, precio, direccion, barrio_zp, zp_sup, desc, manzana, smp, url = row

        profile = profiles.get(manzana)
        if not profile:
            skipped["no_profile"] += 1
            continue

        price_data = prices.get(profile.barrio)
        if not price_data:
            skipped["no_price"] += 1
            continue
        venta_m2, n_comps = price_data

        # Get EPOK area for the matched parcela
        epok_row = ed.execute(
            "SELECT area FROM parcelas WHERE smp_norm = ?", (smp,)
        ).fetchone()
        epok_area = epok_row[0] if epok_row else 0

        m2v, source = estimate_m2_vendibles(
            zp_sup, desc or "", epok_area,
            profile.fot_efectivo, profile.pisos,
        )

        if m2v < 50:
            skipped["no_m2v"] += 1
            continue

        asking_incid = precio / m2v

        max_incid = compute_max_incidencia(
            m2v, profile.pisos, venta_m2,
            profile.plusv_per_m2, epok_area or zp_sup,
            profile.avg_existing_m2,
        )

        if max_incid <= 0:
            skipped["no_incid"] += 1
            continue

        gap_pct = (asking_incid - max_incid) / max_incid * 100

        listing = Listing(
            posting_id=pid, precio_usd=precio, direccion=direccion,
            barrio=profile.barrio, zp_superficie=zp_sup,
            m2_vendibles=m2v, m2v_source=source, manzana=manzana, url=url,
        )

        results.append(Feasibility(
            listing=listing, profile=profile,
            asking_incid=asking_incid, max_incid=max_incid,
            gap_pct=gap_pct, venta_m2=venta_m2, n_comps=n_comps,
        ))

    results.sort(key=lambda r: r.gap_pct)

    # Output
    print(f"\n{'='*80}")
    print(f"FEASIBILITY ANALYSIS — {len(results)} terrenos evaluados")
    print(f"Skipped: {skipped}")
    print(f"{'='*80}\n")

    # Distribution
    ranges = [
        ("Cierra a precio publicado (gap < 0%)", lambda r: r.gap_pct < 0),
        ("Negociable (gap 0-30%)", lambda r: 0 <= r.gap_pct < 30),
        ("Difícil (gap 30-60%)", lambda r: 30 <= r.gap_pct < 60),
        ("No cierra (gap > 60%)", lambda r: r.gap_pct >= 60),
    ]
    print("DISTRIBUCIÓN:")
    for label, pred in ranges:
        n = sum(1 for r in results if pred(r))
        print(f"  {label}: {n} ({100*n/len(results):.0f}%)")

    # Max incidencia by barrio
    print(f"\nINCIDENCIA MÁXIMA POR BARRIO (para 10% anual):")
    barrio_incid: dict[str, list[float]] = {}
    for r in results:
        barrio_incid.setdefault(r.profile.barrio, []).append(r.max_incid)
    for barrio in sorted(barrio_incid, key=lambda b: -sum(barrio_incid[b])/len(barrio_incid[b])):
        vals = barrio_incid[barrio]
        if len(vals) >= 3:
            avg = sum(vals) / len(vals)
            vta = prices.get(barrio, (0, 0))[0]
            print(f"  {barrio:25s}  max ${avg:,.0f}/m²v  "
                  f"(venta ${vta:,.0f}/m², {len(vals)} lotes)")

    # Top opportunities
    print(f"\nTOP 20 OPORTUNIDADES (menor gap):")
    print(f"{'Barrio':20s} {'Dirección':30s} {'Pide':>8s} {'m²v':>6s} "
          f"{'Fuente':>12s} {'Incid':>7s} {'Max':>7s} {'Gap':>6s} {'Pisos':>5s}")
    print("-" * 110)
    for r in results[:20]:
        l = r.listing
        print(
            f"{l.barrio:20s} {l.direccion[:30]:30s} "
            f"${l.precio_usd/1000:>6,.0f}k {l.m2_vendibles:>5,.0f} "
            f"{l.m2v_source:>12s} ${r.asking_incid:>5,.0f} "
            f"${r.max_incid:>5,.0f} {r.gap_pct:>+5.0f}% "
            f"{r.profile.pisos:>4d}p"
        )
    for r in results[:20]:
        print(f"  → {r.listing.url}")

    # Save full results to JSON
    output = []
    for r in results:
        output.append({
            "posting_id": r.listing.posting_id,
            "barrio": r.listing.barrio,
            "direccion": r.listing.direccion,
            "precio_usd": r.listing.precio_usd,
            "m2_vendibles": round(r.listing.m2_vendibles),
            "m2v_source": r.listing.m2v_source,
            "asking_incidencia": round(r.asking_incid),
            "max_incidencia": round(r.max_incid),
            "gap_pct": round(r.gap_pct, 1),
            "pisos": r.profile.pisos,
            "venta_m2": round(r.venta_m2),
            "url": r.listing.url,
        })
    with open("feasibility_results.json", "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\nResultados guardados en feasibility_results.json")

    ed.close()
    zp.close()


if __name__ == "__main__":
    run()
