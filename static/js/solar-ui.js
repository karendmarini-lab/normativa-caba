/**
 * solar-ui.js — Adaptador de UI para el módulo de análisis solar.
 *
 * Responsabilidades:
 *   - Invocar computeSolarAnalysis() con los datos de la parcela activa.
 *   - Renderizar el badge compacto en el sidebar (#solar-sidebar-badge).
 *   - Renderizar la sección completa con gráfico de barras en el informe
 *     (#frm-solar-section > .frm-solar-inner).
 *
 * Requiere que turf.js esté cargado como global (CDN en index.html).
 */

import { computeSolarAnalysis } from './solar.js';

// Altura máxima en px para las barras del chart (área de barras = 80px)
const BAR_MAX_H = 80;

// ── Punto de entrada público ─────────────────────────────────────────────────

/**
 * Inicializa el módulo solar para la parcela activa.
 * Llamar desde app.js::showParcelDetail() cuando polygon_geojson esté disponible.
 *
 * @param {Object} geometry  - parcel.polygon_geojson (GeoJSON)
 * @param {{ x: number, y: number }} coords - { x: lng, y: lat }
 */
export function initializeSolarModule(geometry, coords) {
  if (!geometry || !coords) return;

  // Turf.js debe estar cargado como global vía CDN
  if (typeof turf === 'undefined') {
    console.warn('[solar-ui] turf.js no disponible — análisis solar omitido.');
    return;
  }

  try {
    const data = computeSolarAnalysis(geometry, coords);
    if (!data) {
      console.warn('[solar-ui] No se pudo calcular el análisis solar.');
      return;
    }
    _updateSolarBadge(data);
    _renderSolarReportSection(data);
  } catch (err) {
    console.error('[solar-ui] Error en initializeSolarModule:', err);
  }
}

// ── Badge en sidebar ─────────────────────────────────────────────────────────

function _updateSolarBadge(data) {
  const el = document.getElementById('solar-sidebar-badge');
  if (!el) return;

  const { facade, facadeScore, classification, facadeRadiation } = data;

  el.style.display = 'block';
  el.innerHTML = `
    <div class="slabel" style="margin-bottom:8px">Asoleamiento</div>
    <div class="solar-badge-card">
      <div class="solar-badge-emoji">${classification.emoji}</div>
      <div class="solar-badge-info">
        <div class="solar-badge-level" style="color:${classification.color}">${classification.level}</div>
        <div class="solar-badge-orient">Frente ${facade.label} &middot; ${facadeScore}/100</div>
      </div>
      <div class="solar-badge-rad">
        <div class="solar-badge-rad-val">${facadeRadiation}</div>
        <div class="solar-badge-rad-unit">kWh/m²/día</div>
      </div>
    </div>
  `;
}

// ── Sección completa en el informe ───────────────────────────────────────────

function _renderSolarReportSection(data) {
  const section = document.getElementById('frm-solar-section');
  if (!section) return;

  const {
    facade, rear,
    facadeScore, rearScore, overallScore,
    classification,
    facadeRadiation, rearRadiation,
    monthlyData, insight,
  } = data;

  // ── Gráfico de barras ──
  const maxVal = Math.max(...monthlyData.map(m => m.value));
  const bars = monthlyData.map(m => {
    const barH = Math.max(3, Math.round((m.value / maxVal) * BAR_MAX_H));
    return `
      <div class="solar-bar-col">
        <div class="solar-bar-val">${m.value}</div>
        <div class="solar-bar-fill" style="height:${barH}px"></div>
        <div class="solar-bar-label">${m.label}</div>
      </div>`;
  }).join('');

  // ── Score del contrafrente (color atenuado) ──
  const rearClass = _classifyRear(rearScore);

  section.style.display = 'block';
  section.querySelector('.frm-solar-inner').innerHTML = `
    <div class="frm-table-title">☀️ Análisis de Asoleamiento</div>

    <div class="solar-scores-row">
      <div class="solar-score-item">
        <div class="solar-score-label">FRENTE</div>
        <div class="solar-score-orient">${facade.label}</div>
        <div class="solar-score-val" style="color:${classification.color}">${facadeScore}</div>
        <div class="solar-score-sub">/ 100</div>
        <div class="solar-score-rad">${facadeRadiation} kWh/m²/día</div>
      </div>

      <div class="solar-score-divider"></div>

      <div class="solar-score-item solar-score-center">
        <div class="solar-score-label">PUNTAJE GLOBAL</div>
        <div class="solar-score-emoji">${classification.emoji}</div>
        <div class="solar-score-val solar-score-global" style="color:${classification.color}">${Math.round(overallScore)}</div>
        <div class="solar-score-level" style="color:${classification.color}">${classification.level}</div>
      </div>

      <div class="solar-score-divider"></div>

      <div class="solar-score-item">
        <div class="solar-score-label">CONTRAFRENTE</div>
        <div class="solar-score-orient">${rear.label}</div>
        <div class="solar-score-val" style="color:${rearClass.color}">${rearScore}</div>
        <div class="solar-score-sub">/ 100</div>
        <div class="solar-score-rad">${rearRadiation} kWh/m²/día</div>
      </div>
    </div>

    <div class="solar-chart-title">Radiación mensual — frente (kWh/m²/día)</div>
    <div class="solar-chart">${bars}</div>

    <div class="solar-insight">${insight}</div>
  `;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/** Devuelve solo el color para el score del contrafrente (sin emoji). */
function _classifyRear(score) {
  if (score >= 85) return { color: '#22c55e' };
  if (score >= 70) return { color: '#84cc16' };
  if (score >= 50) return { color: '#eab308' };
  if (score >= 30) return { color: '#f97316' };
  return { color: '#6b7280' };
}
