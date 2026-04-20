"""
Compute m² construibles for any parcel in CABA.

Three-tier approach:
1. GCBA exact (edif_sup_max_edificable) — 0% error, 5% coverage (growing)
2. GCBA planta + CUR rules (model V12) — 3.8% median error, 71% coverage
3. CUR rule estimation — ~15% error, 24% coverage

Calibrated against 14,008 parcels with GCBA ground truth.
Validated against 25 real Zonaprop publications.

Model V12 performance by zone:
  USAB1:  2.6% median, 92% ±10%
  USAA:   3.6% median, 82% ±10%
  USAB2:  4.2% median, 81% ±10%
  E3:     4.9% median, 72% ±10%
  CM:    12.9% median, 39% ±10% (irreducible without 3D geometry)
  TOTAL:  3.8% median, 80% ±10%
"""

import json
import math
from pathlib import Path

RATIO_VENDIBLE = 0.78  # calibrated from 108 publications


def get_m2_construibles(
    edif_sup_max: float | None,
    edif_planta: float | None,
    area: float,
    frente: float,
    fondo: float,
    alt1: float | None,
    plano_lim: float | None,
    dist: str | None,
) -> tuple[float, str]:
    """Compute m² construibles from best available data.

    Returns (m2_construibles, source).
    Source is one of: "gcba_exact", "model_v12", "rule_fallback".
    """
    # Tier 0: GCBA computed exact total
    if edif_sup_max and edif_sup_max > 100:
        return edif_sup_max, "gcba_exact"

    # Tier 1: GCBA planta + model V12
    if edif_planta and edif_planta > 10 and area > 0:
        ratio = edif_planta / area
        if ratio >= 1.05:
            # planta is already total, not per-floor
            return edif_planta, "model_v12"
        return _model_v12(edif_planta, alt1, plano_lim, dist, frente, fondo), "model_v12"

    # Tier 2: estimate pisada from CUR rules, then apply model
    pisada = _estimate_pisada(area, frente, fondo, dist)
    return _model_v12(pisada, alt1, plano_lim, dist, frente, fondo), "rule_fallback"


def _model_v12(
    planta: float,
    alt1: float | None,
    plano: float | None,
    dist: str | None,
    frente: float,
    fondo: float,
) -> float:
    """CUR rule-based model, calibrated against 14k GCBA parcels."""
    d = (dist or "").upper()
    altura = alt1 if alt1 and alt1 > 0 else 14.6
    pl = plano if plano and plano > 0 else altura
    pc = 1 + (math.floor((altura - 3) / 2.8) if altura > 3 else 0)
    has_ret = pl > altura + 1

    # Low-rise regimes (h <= 12, no retiro): FOT ≈ 1
    if abs(pl - altura) < 1 and altura <= 12.5:
        if "USAB1" in d or "U.S.A.B. 1" in d or "USAB0" in d or "U.S.A.B. 0" in d:
            return planta * 1.0
        # USAB2 with low height: empirical multiplier
        if altura <= 9.5:
            return planta * 5.0
        return planta * 4.8

    # E3/E2: FOT=3 allows more than envelope
    if "E3" in d or "E2" in d:
        mult = 1.4 if pc <= 5 else 1.2
        return planta * pc * mult

    # Corredores: calibrated multiplier by height (basamento/torre baked in)
    if any(z in d for z in ["CORREDOR", "CM", "CA"]):
        h_round = round(altura)
        if h_round <= 15:
            mult = 4.89
        elif h_round <= 17:
            mult = 6.43
        elif h_round <= 23:
            mult = 8.37
        else:
            mult = 10.08
        return planta * mult

    # Standard zones (USAB2, USAM, USAA, E1): pisos × planta + retiros
    total = planta * pc
    if has_ret:
        if "USAB" in d:
            ret1 = max(0, planta - 2 * frente)
            n_ret = max(1, math.floor((pl - altura) / 2.8))
            total += ret1 * n_ret
        else:
            # USAM/USAA: retiro correction ×0.92
            ret1 = max(0, planta - 2 * frente) * 0.92
            total += ret1
            ret2 = max(0, planta - 4 * frente) * 0.92
            n_ret2 = max(0, math.floor((pl - altura) / 2.8) - 1)
            total += ret2 * n_ret2

    return max(0, total)


def _estimate_pisada(
    area: float, frente: float, fondo: float, dist: str | None,
) -> float:
    """Estimate edificable footprint from CUR rules when no GCBA data."""
    d = (dist or "").upper()

    if any(z in d for z in ["USAB0", "USAB1", "USAB2"]):
        retiro = 4
    elif any(z in d for z in ["USAM", "USAA"]):
        retiro = 6
    else:
        retiro = 8

    if fondo <= 16:
        prof_edif = fondo
    else:
        prof_edif = max(16, fondo - retiro)

    return frente * prof_edif


def get_m2_vendibles(m2_construibles: float) -> float:
    """Vendibles = construibles × 0.78 (calibrated from 108 publications)."""
    return m2_construibles * RATIO_VENDIBLE
