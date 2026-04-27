/**
 * solar.js — Módulo de análisis solar para EdificIA.
 * Extraído de bacodeai, adaptado para static/js/.
 *
 * Dependencia: turf.js cargado como global vía CDN en index.html
 *   <script src="https://cdn.jsdelivr.net/npm/@turf/turf@6/turf.min.js"></script>
 *
 * Uso principal:
 *   import { computeSolarAnalysis } from './solar.js';
 *   const result = computeSolarAnalysis(parcel.polygon_geojson, { x: lng, y: lat });
 */

// ─────────────────────────────────────────────────────────────────────────────
// TABLAS DE DATOS (Buenos Aires, hemisferio sur, ~34°S)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Radiación solar media mensual por orientación cardinal (kWh/m²/día).
 * Índice 0 = Enero, 11 = Diciembre.
 * Fuente: valores empíricos para latitud ~34°S (Buenos Aires).
 */
const RADIATION_TABLE = {
  N:  [2.8, 3.4, 4.2, 4.8, 5.0, 5.1, 5.0, 4.7, 4.0, 3.3, 2.8, 2.6],
  NE: [3.5, 3.8, 4.0, 3.9, 3.6, 3.3, 3.4, 3.7, 3.8, 3.7, 3.5, 3.4],
  E:  [3.8, 3.6, 3.2, 2.6, 2.2, 1.9, 2.0, 2.4, 3.0, 3.4, 3.7, 3.8],
  SE: [3.2, 2.8, 2.2, 1.6, 1.2, 1.0, 1.1, 1.5, 2.1, 2.7, 3.1, 3.3],
  S:  [2.0, 1.6, 1.1, 0.7, 0.5, 0.4, 0.5, 0.7, 1.0, 1.5, 1.9, 2.1],
  SO: [3.2, 2.8, 2.2, 1.6, 1.2, 1.0, 1.1, 1.5, 2.1, 2.7, 3.1, 3.3],
  O:  [3.8, 3.6, 3.2, 2.6, 2.2, 1.9, 2.0, 2.4, 3.0, 3.4, 3.7, 3.8],
  NO: [3.5, 3.8, 4.0, 3.9, 3.6, 3.3, 3.4, 3.7, 3.8, 3.7, 3.5, 3.4],
};

/**
 * Score solar (0–100) por punto cardinal (16 posiciones).
 * N=100 porque en hemisferio sur el sol viene del norte.
 */
const SCORE_TABLE = {
  N:   100,
  NNE: 92, NNO: 92,
  NE:  82, NO:  82,
  ENE: 68, ONO: 68,
  E:   55, O:   55,
  ESE: 42, OSO: 42,
  SE:  32, SO:  32,
  SSE: 22, SSO: 22,
  S:   15,
};

/** Etiquetas en español por punto cardinal (16 posiciones). */
const CARDINAL_LABELS = {
  N:   "Norte",
  NNE: "Nor-Noreste",
  NE:  "Noreste",
  ENE: "Este-Noreste",
  E:   "Este",
  ESE: "Este-Sureste",
  SE:  "Sureste",
  SSE: "Sur-Sureste",
  S:   "Sur",
  SSO: "Sur-Suroeste",
  SO:  "Suroeste",
  OSO: "Oeste-Suroeste",
  O:   "Oeste",
  ONO: "Oeste-Noroeste",
  NO:  "Noroeste",
  NNO: "Nor-Noroeste",
};

const MONTH_LABELS = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];

// ─────────────────────────────────────────────────────────────────────────────
// FUNCIONES AUXILIARES
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Convierte grados a punto cardinal de 16 posiciones.
 * @param {number} degrees - Azimut en grados (0–360)
 * @returns {string} Cardinal code, ej: "NNE"
 */
function azimuthToCardinal16(degrees) {
  const d = ((degrees % 360) + 360) % 360;
  const points = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSO","SO","OSO","O","ONO","NO","NNO"];
  return points[Math.round(d / 22.5) % 16];
}

/**
 * Convierte grados a punto cardinal de 8 posiciones (para tabla de radiación).
 * @param {number} degrees
 * @returns {string} Cardinal code, ej: "NE"
 */
function azimuthToCardinal8(degrees) {
  const d = ((degrees % 360) + 360) % 360;
  const points = ["N","NE","E","SE","S","SO","O","NO"];
  return points[Math.round(d / 45) % 8];
}

/**
 * Devuelve el score solar (0–100) para un azimut dado.
 * @param {number} azimuth
 * @returns {number|null}
 */
function getSolarScore(azimuth) {
  if (azimuth == null) return null;
  const cardinal = azimuthToCardinal16(azimuth);
  return SCORE_TABLE[cardinal] ?? 50;
}

/**
 * Devuelve la radiación media anual (o mensual) para un azimut dado.
 * @param {number} azimuth
 * @param {number|null} monthIndex - 0=Enero … 11=Diciembre. Si es null devuelve promedio anual.
 * @returns {number|null} kWh/m²/día
 */
function getRadiation(azimuth, monthIndex = null) {
  if (azimuth == null) return null;
  const cardinal = azimuthToCardinal8(azimuth);
  const row = RADIATION_TABLE[cardinal];
  if (!row) return null;
  if (monthIndex !== null && monthIndex >= 0 && monthIndex <= 11) {
    return row[monthIndex];
  }
  return Math.round((row.reduce((a, b) => a + b, 0) / 12) * 10) / 10;
}

/**
 * Devuelve los datos mensuales del frente (array de 12 objetos { label, value }).
 * @param {number} azimuth
 * @returns {Array<{label: string, value: number}>|null}
 */
function getMonthlyData(azimuth) {
  if (azimuth == null) return null;
  const cardinal = azimuthToCardinal8(azimuth);
  const row = RADIATION_TABLE[cardinal];
  if (!row) return null;
  return MONTH_LABELS.map((label, i) => ({ label, value: row[i] }));
}

/**
 * Clasifica el asoleamiento según el score máximo.
 * @param {number} score
 * @returns {{ level: string, color: string, emoji: string }|null}
 */
function classifyScore(score) {
  if (score == null) return null;
  if (score >= 85) return { level: "Excelente",  color: "#22c55e", emoji: "☀️"  };
  if (score >= 70) return { level: "Muy Buena",  color: "#84cc16", emoji: "🌤️" };
  if (score >= 50) return { level: "Buena",      color: "#eab308", emoji: "⛅"  };
  if (score >= 30) return { level: "Moderada",   color: "#f97316", emoji: "🌥️" };
  return              { level: "Limitada",   color: "#ef4444", emoji: "☁️"  };
}

/**
 * Genera el texto de insight/recomendación.
 * @param {{ label: string, rearLabel: string }} orientation
 * @param {number} facadeScore
 * @param {number} rearScore
 * @returns {string}
 */
function generateInsight(orientation, facadeScore, rearScore) {
  const bestScore = Math.max(facadeScore, rearScore);
  const bestSide  = facadeScore >= rearScore ? "frente" : "contrafrente";
  const bestLabel = facadeScore >= rearScore ? orientation.label : orientation.rearLabel;

  if (bestScore >= 85)
    return `Excelente asoleamiento. El ${bestSide} tiene orientación ${bestLabel}, ideal para unidades premium con máxima luz natural.`;
  if (bestScore >= 70)
    return `Muy buen asoleamiento. La orientación ${bestLabel} del ${bestSide} garantiza buena luz natural durante todo el año.`;
  if (bestScore >= 50)
    return `Asoleamiento aceptable. Se recomienda priorizar las unidades con vista al ${bestSide} (${bestLabel}) para mayor confort.`;
  if (bestScore >= 30)
    return `Asoleamiento moderado. Considerar estrategias de diseño para maximizar la entrada de luz natural, como ventanas más amplias o doble orientación.`;
  return `Asoleamiento limitado. Las orientaciones ${orientation.label} (frente) y ${orientation.rearLabel} (contrafrente) tienen poca radiación directa. Evaluar soluciones de iluminación complementarias.`;
}

// ─────────────────────────────────────────────────────────────────────────────
// DETECCIÓN DE FACHADA (requiere turf.js global)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Detecta la orientación de la fachada a partir de la geometría GeoJSON de la parcela
 * y las coordenadas del inmueble.
 *
 * @param {Object} geometry - GeoJSON Feature, FeatureCollection, Polygon o MultiPolygon
 * @param {{ x: number, y: number }} coords - { x: lng, y: lat } del inmueble
 */
function detectFacadeOrientation(geometry, coords) {
  try {
    if (!geometry || !coords) return null;

    // ── 1. Normalizar a Feature
    let feature = null;
    if (geometry.type === "FeatureCollection") {
      feature = geometry.features?.find(
        f => f.properties?.tipo === "parcela" ||
             f.properties?.type === "parcela" ||
             f.geometry?.type === "Polygon" ||
             f.geometry?.type === "MultiPolygon"
      ) || geometry.features?.[0];
    } else if (geometry.type === "Feature") {
      feature = geometry;
    } else if (geometry.type === "Polygon" || geometry.type === "MultiPolygon") {
      feature = turf.feature(geometry);
    }

    if (!feature?.geometry) return null;

    // ── 2. Extraer anillo exterior
    let ring;
    if (feature.geometry.type === "MultiPolygon") {
      const coords_mp = feature.geometry.coordinates;
      let maxArea = 0;
      let bestRing = coords_mp[0][0];
      for (const poly of coords_mp) {
        const area = turf.area(turf.polygon(poly));
        if (area > maxArea) { maxArea = area; bestRing = poly[0]; }
      }
      ring = bestRing;
    } else {
      ring = feature.geometry.coordinates[0];
    }

    if (!ring || ring.length < 4) return null;

    // ── 3. Punto del inmueble como Feature
    const inmueblePoint = turf.point([parseFloat(coords.x), parseFloat(coords.y)]);

    // ── 4. Encontrar el segmento más cercano al inmueble
    let closestSegment = null;
    let minDist = Infinity;

    for (let i = 0; i < ring.length - 1; i++) {
      const segStart = ring[i];
      const segEnd   = ring[i + 1];
      const segLen   = turf.distance(turf.point(segStart), turf.point(segEnd), { units: "meters" });
      if (segLen < 1) continue;

      const segLine = turf.lineString([segStart, segEnd]);
      const dist    = turf.pointToLineDistance(inmueblePoint, segLine, { units: "meters" });

      if (dist < minDist) {
        minDist = dist;
        closestSegment = { start: segStart, end: segEnd, length: segLen };
      }
    }

    if (!closestSegment) return null;

    // ── 5. Calcular azimut de la fachada
    const bearing    = turf.bearing(turf.point(closestSegment.start), turf.point(closestSegment.end));
    const midpoint   = turf.midpoint(turf.point(closestSegment.start), turf.point(closestSegment.end));
    const centroid   = turf.centroid(feature);

    const candidateA = (bearing + 90 + 360) % 360;
    const candidateB = (bearing - 90 + 360) % 360;

    const pointA = turf.destination(midpoint, 0.01, candidateA, { units: "kilometers" });
    const pointB = turf.destination(midpoint, 0.01, candidateB, { units: "kilometers" });

    const distA = turf.distance(pointA, centroid);
    const distB = turf.distance(pointB, centroid);
    const facadeAzimuth = distA > distB
      ? (candidateA + 360) % 360
      : (candidateB + 360) % 360;

    const facadeCardinal = azimuthToCardinal16(facadeAzimuth);
    const rearAzimuth    = (facadeAzimuth + 180) % 360;
    const rearCardinal   = azimuthToCardinal16(rearAzimuth);

    return {
      azimuth:         Math.round(facadeAzimuth * 10) / 10,
      cardinal:        facadeCardinal,
      label:           CARDINAL_LABELS[facadeCardinal] || facadeCardinal,
      frontEdgeLength: Math.round(closestSegment.length * 100) / 100,
      rearAzimuth:     Math.round(rearAzimuth * 10) / 10,
      rearCardinal,
      rearLabel:       CARDINAL_LABELS[rearCardinal] || "",
    };

  } catch (err) {
    console.error("[solar] detectFacadeOrientation error:", err);
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// FUNCIÓN PRINCIPAL
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Calcula el análisis solar completo de una parcela.
 *
 * @param {Object} geometry  - GeoJSON de la geometría de la parcela
 * @param {{ x: number, y: number }} coords - { x: lng, y: lat } del inmueble
 * @returns {Object|null}
 */
function computeSolarAnalysis(geometry, coords) {
  const orientation = detectFacadeOrientation(geometry, coords);
  if (!orientation) return null;

  const facadeScore  = getSolarScore(orientation.azimuth);
  const rearScore    = getSolarScore(orientation.rearAzimuth);
  const overallScore = Math.round((0.4 * facadeScore + 0.6 * rearScore) * 10) / 10;

  return {
    facade: {
      azimuth:    orientation.azimuth,
      cardinal:   orientation.cardinal,
      label:      orientation.label,
      edgeLength: orientation.frontEdgeLength,
    },
    rear: {
      azimuth:  orientation.rearAzimuth,
      cardinal: orientation.rearCardinal,
      label:    orientation.rearLabel,
    },
    facadeScore,
    rearScore,
    overallScore,
    classification:  classifyScore(facadeScore),
    facadeRadiation: getRadiation(orientation.azimuth),
    rearRadiation:   getRadiation(orientation.rearAzimuth),
    monthlyData:     getMonthlyData(orientation.azimuth),
    bestSide:        facadeScore >= rearScore ? "frente" : "contrafrente",
    insight:         generateInsight(orientation, facadeScore, rearScore),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// EXPORTS
// ─────────────────────────────────────────────────────────────────────────────

export {
  computeSolarAnalysis,
  detectFacadeOrientation,
  getSolarScore,
  getRadiation,
  getMonthlyData,
  classifyScore,
  generateInsight,
  azimuthToCardinal16,
  azimuthToCardinal8,
  RADIATION_TABLE,
  SCORE_TABLE,
  CARDINAL_LABELS,
  MONTH_LABELS,
};
