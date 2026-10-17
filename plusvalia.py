"""
Cálculo de plusvalía (Derecho para el Desarrollo Urbano y el Hábitat Sustentable,
DDHUS — Ley 6062) con el mismo criterio que el simulador de Ciudad 3D.

    a  = m² sobre rasante susceptibles de DDHUS
         (m² totales del proyecto − subsuelos − balcones − planta baja)
         La planta baja solo se descuenta en los U.S.A.B. (Ciudad 3D pide ese dato
         únicamente ahí); en el resto de los distritos queda en 0.
    A1 = a × 0.8
    A2 = FOT del CPU anterior × superficie de la parcela
    A  = A1 − A2
    B  = alícuota × incidencia (UVA/m²)
    UVAs a pagar = A × B        (0 si A <= 0)

El FOT, la superficie de la parcela, la incidencia y la alícuota salen de los
datos de EdificIA para esa parcela. Los m² del proyecto los carga el usuario.
"""

from __future__ import annotations

from typing import Any

FACTOR_A1 = 0.8


def _num(value: Any) -> float | None:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return n


def normalizar_alicuota(alicuota: Any) -> float | None:
    """La alícuota se usa como fracción (0,35). Si viene como 35, se pasa a 0,35."""
    n = _num(alicuota)
    if n is None or n <= 0:
        return None
    return n / 100 if n > 1 else n


def calcular_plusvalia(
    *,
    fot: Any,
    sup_parcela: Any,
    incidencia_uva: Any,
    alicuota: Any,
    m2_totales: float = 0.0,
    m2_subsuelos: float = 0.0,
    m2_balcones: float = 0.0,
    m2_pb: float = 0.0,
) -> dict[str, Any]:
    """Devuelve el desglose del cálculo. Si falta algún dato de la parcela,
    `calculable` es False y `faltantes` lista qué falta."""
    fot_n = _num(fot)
    sup_n = _num(sup_parcela)
    inc_n = _num(incidencia_uva)
    ali_n = normalizar_alicuota(alicuota)

    faltantes = []
    if not fot_n or fot_n <= 0:
        faltantes.append("FOT del CPU anterior")
    if not sup_n or sup_n <= 0:
        faltantes.append("superficie de la parcela")
    if not inc_n or inc_n <= 0:
        faltantes.append("incidencia UVA")
    if not ali_n:
        faltantes.append("alícuota")

    if faltantes:
        return {"calculable": False, "faltantes": faltantes}

    a = max(0.0, m2_totales - m2_subsuelos - m2_balcones - m2_pb)
    a1 = a * FACTOR_A1
    a2 = fot_n * sup_n
    diferencia = a1 - a2
    b = ali_n * inc_n
    uvas = max(0.0, diferencia) * b

    return {
        "calculable": True,
        "faltantes": [],
        "fot": fot_n,
        "sup_parcela": sup_n,
        "incidencia_uva": inc_n,
        "alicuota": ali_n,
        "a": round(a, 2),
        "a1": round(a1, 2),
        "a2": round(a2, 2),
        "a_menos_a2": round(diferencia, 2),
        "b": round(b, 4),
        # m² sobre rasante a partir de los cuales aparece plusvalía
        "umbral_m2": round(a2 / FACTOR_A1, 2),
        "uvas": round(uvas, 2),
        "aplica": diferencia > 0,
    }
