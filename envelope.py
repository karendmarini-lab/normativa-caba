"""
Compute the stepped buildable envelope (sobre edificable) for a parcel.

CUR rules (Ley 6099/2018):
- The edificable footprint is the parcel clipped at the LFI
  (LFI = 1/4 of block depth from each street, minimum 16m)
- Section 1 (0 to altura_max): full edificable footprint
- Section 2 (altura_max to +3m): footprint clipped 2m from L.O. (front)
- Section 3 (+3m to plano_limite): footprint clipped 4m from L.O. AND 4m from LFI
- USAB zones (plano_limite == altura_max): single box, no retiros

Geometry: real polygon clipping via Sutherland-Hodgman, not offset hacks.
"""

from __future__ import annotations

import math


# ── Geometry primitives ──────────────────────────────────────────

def _edge_length_m(p1: list[float], p2: list[float]) -> float:
    """Edge length in meters from WGS84 coords."""
    cos_lat = math.cos(math.radians((p1[1] + p2[1]) / 2))
    dlng = (p2[0] - p1[0]) * 111_000 * cos_lat
    dlat = (p2[1] - p1[1]) * 111_000
    return math.sqrt(dlng**2 + dlat**2)


def _midpoint(p1: list[float], p2: list[float]) -> list[float]:
    return [(p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2]


def _centroid(polygon: list[list[float]]) -> list[float]:
    pts = polygon if polygon[0] != polygon[-1] else polygon[:-1]
    n = len(pts)
    return [sum(p[0] for p in pts) / n, sum(p[1] for p in pts) / n]


def _unit_normal_inward(
    p1: list[float], p2: list[float], centroid: list[float],
) -> tuple[float, float]:
    """Unit normal to edge p1->p2, pointing toward centroid."""
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    length = math.sqrt(dx**2 + dy**2)
    if length == 0:
        return (0.0, 0.0)
    n1 = (-dy / length, dx / length)
    n2 = (dy / length, -dx / length)
    mid = _midpoint(p1, p2)
    dot1 = (centroid[0] - mid[0]) * n1[0] + (centroid[1] - mid[1]) * n1[1]
    return n1 if dot1 > 0 else n2


def _clip_line_from_edge(
    edge_p1: list[float],
    edge_p2: list[float],
    normal: tuple[float, float],
    distance_m: float,
) -> tuple[list[float], list[float]]:
    """Return a clip line (two points) parallel to edge, offset inward by distance_m."""
    cos_lat = math.cos(math.radians(edge_p1[1]))
    offset_lng = normal[0] * distance_m / (111_000 * cos_lat)
    offset_lat = normal[1] * distance_m / 111_000

    return (
        [edge_p1[0] + offset_lng, edge_p1[1] + offset_lat],
        [edge_p2[0] + offset_lng, edge_p2[1] + offset_lat],
    )


# ── Sutherland-Hodgman polygon clipping ──────────────────────────

def _side(point: list[float], line_a: list[float], line_b: list[float]) -> float:
    """Positive = left of line A->B, negative = right."""
    return ((line_b[0] - line_a[0]) * (point[1] - line_a[1])
            - (line_b[1] - line_a[1]) * (point[0] - line_a[0]))


def _line_intersect(
    p1: list[float], p2: list[float],
    p3: list[float], p4: list[float],
) -> list[float]:
    """Intersection of line p1-p2 with line p3-p4."""
    x1, y1 = p1
    x2, y2 = p2
    x3, y3 = p3
    x4, y4 = p4
    denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
    if abs(denom) < 1e-15:
        return _midpoint(p1, p2)
    t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom
    return [x1 + t * (x2 - x1), y1 + t * (y2 - y1)]


def clip_polygon(
    polygon: list[list[float]],
    clip_a: list[float],
    clip_b: list[float],
) -> list[list[float]]:
    """Clip polygon, keeping the side where _side() > 0 (left of clip_a->clip_b)."""
    if len(polygon) < 3:
        return polygon

    output = list(polygon)
    if output[0] == output[-1]:
        output = output[:-1]

    clipped: list[list[float]] = []
    n = len(output)

    for i in range(n):
        curr = output[i]
        nxt = output[(i + 1) % n]
        s_curr = _side(curr, clip_a, clip_b)
        s_nxt = _side(nxt, clip_a, clip_b)

        if s_curr >= 0:
            clipped.append(curr)
            if s_nxt < 0:
                clipped.append(_line_intersect(curr, nxt, clip_a, clip_b))
        elif s_nxt >= 0:
            clipped.append(_line_intersect(curr, nxt, clip_a, clip_b))

    if clipped and clipped[0] != clipped[-1]:
        clipped.append(clipped[0])

    return clipped


# ── Main envelope computation ────────────────────────────────────

def find_front_edge(
    polygon: list[list[float]], frente_m: float,
) -> int:
    """Find the edge whose length best matches the parcel's frente."""
    pts = polygon if polygon[0] != polygon[-1] else polygon[:-1]
    best_idx = 0
    best_diff = float("inf")
    for i in range(len(pts)):
        j = (i + 1) % len(pts)
        diff = abs(_edge_length_m(pts[i], pts[j]) - frente_m)
        if diff < best_diff:
            best_diff = diff
            best_idx = i
    return best_idx


def find_back_edge(
    polygon: list[list[float]], front_idx: int,
) -> int:
    """Find the edge farthest from the front edge (the back/fondo)."""
    pts = polygon if polygon[0] != polygon[-1] else polygon[:-1]
    n = len(pts)
    front_mid = _midpoint(pts[front_idx], pts[(front_idx + 1) % n])
    best_idx = 0
    best_dist = 0.0
    for i in range(n):
        if i == front_idx:
            continue
        mid = _midpoint(pts[i], pts[(i + 1) % n])
        dist = _edge_length_m(front_mid, mid)
        if dist > best_dist:
            best_dist = dist
            best_idx = i
    return best_idx


def compute_envelope(
    polygon: list[list[float]],
    altura_max: float,
    plano_limite: float,
    frente_m: float,
    fondo_m: float,
    sup_edificable: float | None = None,
    sup_parcela: float | None = None,
) -> list[dict]:
    """Compute the stepped buildable envelope using real polygon clipping.

    Returns list of {"polygon", "base", "top", "label"}.
    """
    if not polygon or not altura_max:
        return []

    pts = polygon if polygon[0] != polygon[-1] else polygon[:-1]
    cent = _centroid(pts)
    n = len(pts)

    has_retiro = plano_limite and (plano_limite - altura_max) > 1.0

    # Identify front and back edges
    front_idx = find_front_edge(pts, frente_m)
    front_p1 = pts[front_idx]
    front_p2 = pts[(front_idx + 1) % n]
    front_normal = _unit_normal_inward(front_p1, front_p2, cent)

    back_idx = find_back_edge(pts, front_idx)
    back_p1 = pts[back_idx]
    back_p2 = pts[(back_idx + 1) % n]
    back_normal = _unit_normal_inward(back_p1, back_p2, cent)

    # ── Clip 1: LFI cut (edificable footprint) ──
    # Use sup_edificable/sup_parcela ratio to estimate LFI depth
    if sup_edificable and sup_parcela and sup_parcela > 0:
        edif_ratio = min(1.0, sup_edificable / sup_parcela)
    else:
        edif_ratio = 1.0

    if edif_ratio < 0.95:
        # LFI cuts from the back. Cut depth = fondo * (1 - ratio)
        lfi_cut = fondo_m * (1 - edif_ratio)
        clip_a, clip_b = _clip_line_from_edge(
            back_p1, back_p2, back_normal, lfi_cut,
        )
        edif_poly = clip_polygon(polygon, clip_b, clip_a)
    else:
        edif_poly = list(polygon)

    if len(edif_poly) < 3:
        edif_poly = list(polygon)

    sections = []

    # ── Section 1: main body (0 to altura_max) ──
    sections.append({
        "polygon": edif_poly,
        "base": 0,
        "top": altura_max,
        "label": f"Cuerpo (0–{altura_max}m)",
    })

    if has_retiro:
        retiro_total = plano_limite - altura_max  # typically 7m
        mid_h = min(3.0, retiro_total)

        # ── Section 2: retiro 1 (–2m from front) ──
        # Clip keeps left side of A->B. We want to keep the interior
        # (away from front), so we flip the clip line direction.
        clip_a, clip_b = _clip_line_from_edge(
            front_p1, front_p2, front_normal, 2.0,
        )
        retiro1_poly = clip_polygon(edif_poly, clip_b, clip_a)
        if len(retiro1_poly) >= 3:
            sections.append({
                "polygon": retiro1_poly,
                "base": altura_max,
                "top": altura_max + mid_h,
                "label": f"Retiro 1 (–2m frente, {altura_max}–{altura_max + mid_h}m)",
            })

        # ── Section 3: retiro 2 (–4m from front AND –4m from LFI) ──
        if retiro_total > mid_h:
            clip_a2, clip_b2 = _clip_line_from_edge(
                front_p1, front_p2, front_normal, 4.0,
            )
            retiro2_poly = clip_polygon(edif_poly, clip_b2, clip_a2)

            # Also clip 4m from the LFI (back) side
            if edif_ratio < 0.95:
                clip_a3, clip_b3 = _clip_line_from_edge(
                    back_p1, back_p2, back_normal, lfi_cut + 4.0,
                )
                retiro2_poly = clip_polygon(retiro2_poly, clip_b3, clip_a3)
            else:
                clip_a3, clip_b3 = _clip_line_from_edge(
                    back_p1, back_p2, back_normal, 4.0,
                )
                retiro2_poly = clip_polygon(retiro2_poly, clip_b3, clip_a3)

            if len(retiro2_poly) >= 3:
                sections.append({
                    "polygon": retiro2_poly,
                    "base": altura_max + mid_h,
                    "top": plano_limite,
                    "label": f"Retiro 2 (–4m frente/fondo, {altura_max + mid_h}–{plano_limite}m)",
                })

    return sections
