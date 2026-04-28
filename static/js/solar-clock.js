/**
 * solar-clock.js — Reloj solar dinámico para el mapa circular del informe.
 *
 * Dibuja sobre un <canvas> overlay:
 *   - Arco del día (trayectoria solar, norte arriba)
 *   - Disco del sol con halo en la posición actual
 *   - Vector de sombra punteado desde el centro
 *   - Puntos cardinales (N en amarillo)
 *
 * Requiere: SunCalc (global CDN), canvas #solar-clock-canvas,
 *           slider #solar-time-slider, label #solar-time-label,
 *           párrafo #solar-impact-text.
 */

// ── Estado del módulo ────────────────────────────────────────────────────────

let _state = null; // { lat, lng, solarData, canvas, ctx, times, size }

// ── Punto de entrada público ─────────────────────────────────────────────────

/**
 * Inicializa el reloj solar para la parcela activa.
 * Llamar desde solar-ui.js::initializeSolarModule() cuando los datos estén listos.
 *
 * @param {number} lat
 * @param {number} lng
 * @param {Object} solarData  — resultado de computeSolarAnalysis()
 */
export function initSolarClock(lat, lng, solarData) {
  const canvas = document.getElementById('solar-clock-canvas');
  if (!canvas) return;

  if (typeof SunCalc === 'undefined') {
    console.warn('[solar-clock] SunCalc no disponible.');
    return;
  }

  const today = new Date();
  const times = SunCalc.getTimes(today, lat, lng);

  // Tamaño real del canvas (sincronizado con CSS)
  const ring = canvas.parentElement;
  const size = ring ? ring.offsetWidth : 260;
  canvas.width  = size;
  canvas.height = size;

  _state = {
    lat, lng, solarData,
    canvas,
    ctx: canvas.getContext('2d'),
    times,
    size,
  };

  // Configurar slider
  const slider = document.getElementById('solar-time-slider');
  if (slider) {
    const now = today.getHours() + today.getMinutes() / 60;
    slider.value = Math.min(18, Math.max(8, now)).toFixed(2);
    slider.addEventListener('input', _onSliderChange);
  }

  // Mostrar controles
  const wrap = document.getElementById('solar-clock-controls');
  if (wrap) wrap.style.display = 'block';

  // Generar párrafo de impacto
  _generateImpactText(lat, lng, solarData, times);

  // Primer dibujo
  const hour = slider ? parseFloat(slider.value) : 12;
  _drawClock(_state, hour);
}

// ── Eventos ──────────────────────────────────────────────────────────────────

function _onSliderChange(e) {
  if (!_state) return;
  _drawClock(_state, parseFloat(e.target.value));
}

// ── Motor de dibujo ──────────────────────────────────────────────────────────

function _drawClock(state, hour) {
  const { canvas, ctx, lat, lng, times, size, solarData } = state;
  const cx = size / 2;
  const cy = size / 2;
  const R  = size / 2 - 2;  // radio exterior del círculo

  ctx.clearRect(0, 0, size, size);

  // Construir Date para la hora seleccionada
  const d = new Date();
  d.setHours(Math.floor(hour), Math.round((hour % 1) * 60), 0, 0);

  // Posición del sol en el momento seleccionado
  const sunPos = SunCalc.getPosition(d, lat, lng);
  const sunBearing  = _suncalcToBearing(sunPos.azimuth);
  const isUp = sunPos.altitude > 0;

  // Azimuths de salida y puesta
  const srBearing = _suncalcToBearing(SunCalc.getPosition(times.sunrise, lat, lng).azimuth);
  const ssBearing = _suncalcToBearing(SunCalc.getPosition(times.sunset,  lat, lng).azimuth);

  const arcR = R * 0.68; // radio del arco solar

  // ── Dibujar capas (orden: arc → shadow → sun → labels) ──
  _drawSunArc(ctx, cx, cy, arcR, srBearing, ssBearing);
  if (isUp) _drawShadowVector(ctx, cx, cy, R * 0.58, sunBearing, sunPos.altitude);
  if (isUp) _drawSunDisk(ctx, cx, cy, arcR, sunBearing);
  _drawCardinals(ctx, cx, cy, R);

  // Actualizar etiqueta de hora
  _updateTimeLabel(hour, isUp);
}

// ── Funciones de dibujo ──────────────────────────────────────────────────────

/** Arco amarillo traslúcido de salida a puesta del sol (a través del norte). */
function _drawSunArc(ctx, cx, cy, r, srBearing, ssBearing) {
  // En el hemisferio sur el sol pasa por el norte (arriba del círculo).
  // El arco va en sentido antihorario desde srBearing → 0°(N) → ssBearing.
  let spanCCW = srBearing - ssBearing;
  if (spanCCW <= 0) spanCCW += 360;

  const steps = 200;

  // Sombra del arco
  ctx.beginPath();
  for (let i = 0; i <= steps; i++) {
    const deg = ((srBearing - (i / steps) * spanCCW) + 360) % 360;
    const { x, y } = _bearingToXY(cx, cy, r + 2, deg);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }
  ctx.strokeStyle = 'rgba(255,204,0,0.06)';
  ctx.lineWidth = 7;
  ctx.lineCap = 'round';
  ctx.stroke();

  // Arco principal
  ctx.beginPath();
  for (let i = 0; i <= steps; i++) {
    const deg = ((srBearing - (i / steps) * spanCCW) + 360) % 360;
    const { x, y } = _bearingToXY(cx, cy, r, deg);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }
  ctx.strokeStyle = 'rgba(255,204,0,0.20)';
  ctx.lineWidth = 2.5;
  ctx.lineCap = 'round';
  ctx.stroke();

  // Ticks en sunrise y sunset
  [srBearing, ssBearing].forEach(b => {
    const inner = _bearingToXY(cx, cy, r - 5, b);
    const outer = _bearingToXY(cx, cy, r + 5, b);
    ctx.beginPath();
    ctx.moveTo(inner.x, inner.y);
    ctx.lineTo(outer.x, outer.y);
    ctx.strokeStyle = 'rgba(255,204,0,0.35)';
    ctx.lineWidth = 1.5;
    ctx.stroke();
  });
}

/** Línea punteada gris desde el centro hacia la dirección de la sombra. */
function _drawShadowVector(ctx, cx, cy, length, sunBearing, altitude) {
  const shadowBearing = (sunBearing + 180) % 360;

  // Largo proporcional a la inversa de la altitud (sombras más largas al amanecer/atardecer)
  const altFactor = Math.max(0.15, Math.min(1, altitude / (Math.PI / 4)));
  const len = length * (0.5 + 0.5 / altFactor);
  const clampedLen = Math.min(len, length * 1.6);

  const tip = _bearingToXY(cx, cy, clampedLen, shadowBearing);

  // Línea punteada
  ctx.save();
  ctx.setLineDash([3, 5]);
  ctx.beginPath();
  ctx.moveTo(cx, cy);
  ctx.lineTo(tip.x, tip.y);
  ctx.strokeStyle = 'rgba(190,190,210,0.25)';
  ctx.lineWidth = 1.5;
  ctx.lineCap = 'round';
  ctx.stroke();

  // Punta de flecha
  ctx.setLineDash([]);
  const arrowRad = shadowBearing * Math.PI / 180;
  const al = 7;
  const aa = 0.45;
  ctx.beginPath();
  ctx.moveTo(tip.x, tip.y);
  ctx.lineTo(
    tip.x - al * Math.sin(arrowRad + aa),
    tip.y + al * Math.cos(arrowRad + aa),
  );
  ctx.moveTo(tip.x, tip.y);
  ctx.lineTo(
    tip.x - al * Math.sin(arrowRad - aa),
    tip.y + al * Math.cos(arrowRad - aa),
  );
  ctx.strokeStyle = 'rgba(190,190,210,0.30)';
  ctx.lineWidth = 1.5;
  ctx.stroke();
  ctx.restore();
}

/** Disco del sol: halo radial + punto dorado. */
function _drawSunDisk(ctx, cx, cy, arcR, sunBearing) {
  const { x, y } = _bearingToXY(cx, cy, arcR, sunBearing);

  // Halo
  const g = ctx.createRadialGradient(x, y, 0, x, y, 16);
  g.addColorStop(0,   'rgba(255,220,0,0.75)');
  g.addColorStop(0.35,'rgba(255,204,0,0.35)');
  g.addColorStop(1,   'rgba(255,204,0,0)');
  ctx.beginPath();
  ctx.arc(x, y, 16, 0, Math.PI * 2);
  ctx.fillStyle = g;
  ctx.fill();

  // Núcleo
  ctx.beginPath();
  ctx.arc(x, y, 5, 0, Math.PI * 2);
  ctx.fillStyle = '#ffdd00';
  ctx.shadowColor = 'rgba(255,220,0,0.8)';
  ctx.shadowBlur = 8;
  ctx.fill();
  ctx.shadowBlur = 0;
}

/** Etiquetas N / E / S / O en los bordes del círculo. */
function _drawCardinals(ctx, cx, cy, R) {
  const labels = [['N', 0], ['E', 90], ['S', 180], ['O', 270]];
  ctx.font = 'bold 9px Inter, system-ui, sans-serif';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';

  labels.forEach(([label, bearing]) => {
    const { x, y } = _bearingToXY(cx, cy, R - 11, bearing);
    ctx.fillStyle = label === 'N'
      ? 'rgba(255,220,0,0.85)'
      : 'rgba(255,255,255,0.22)';
    ctx.fillText(label, x, y);
  });
}

// ── Párrafo de impacto ───────────────────────────────────────────────────────

function _generateImpactText(lat, lng, solarData, times) {
  const el = document.getElementById('solar-impact-text');
  if (!el) return;

  const { facade, facadeScore, facadeRadiation, rear } = solarData;

  // Horarios
  const sr = _fmtTime(times.sunrise);
  const ss = _fmtTime(times.sunset);
  const dayH = times.sunrise && times.sunset
    ? ((times.sunset - times.sunrise) / 3_600_000).toFixed(1)
    : '—';

  // Dirección de sombra a las 15:00
  const t15 = new Date();
  t15.setHours(15, 0, 0, 0);
  const pos15 = SunCalc.getPosition(t15, lat, lng);
  const shadow15 = (_suncalcToBearing(pos15.azimuth) + 180) % 360;
  const shadowDir = _bearingToCardinal(shadow15);

  let text;
  if (facadeScore >= 85) {
    text = `El frente tiene orientación <strong>${facade.label}</strong>, la óptima para el hemisferio sur. Con ${facadeRadiation} kWh/m²/día, recibe luz directa durante las ${dayH}h de sol (${sr}–${ss}). A las 15h las sombras se proyectan hacia el <strong>${shadowDir}</strong>, manteniendo despejada la fachada principal.`;
  } else if (facadeScore >= 70) {
    text = `Orientación <strong>${facade.label}</strong> con buen aprovechamiento solar (${facadeRadiation} kWh/m²/día). Jornada solar: ${sr}–${ss} (${dayH}h). Las aberturas principales hacia el frente maximizarán el ingreso de luz natural en invierno.`;
  } else if (facadeScore >= 50) {
    text = `Orientación <strong>${facade.label}</strong> — asoleamiento aceptable (${facadeRadiation} kWh/m²/día, ${dayH}h de sol hoy). Se recomienda doble orientación: combinar el frente con el contrafrente <strong>${rear.label}</strong> para compensar.`;
  } else {
    text = `Orientación <strong>${facade.label}</strong> con captación solar limitada (${facadeRadiation} kWh/m²/día). La cara <strong>${rear.label}</strong> (contrafrente) ofrece mayor radiación. Evaluar configuración de unidades hacia ese lado.`;
  }

  el.innerHTML = text;
  el.style.display = 'block';
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Convierte el azimut de SunCalc (radianes desde el sur, + al oeste)
 * a compass bearing (grados desde el norte, + al este, 0–360).
 */
function _suncalcToBearing(az) {
  return (((az + Math.PI) * 180 / Math.PI) + 360) % 360;
}

/**
 * Convierte un compass bearing (grados, norte=0, este=90) a
 * coordenadas de pantalla (norte = arriba, este = derecha).
 */
function _bearingToXY(cx, cy, r, bearingDeg) {
  const rad = bearingDeg * Math.PI / 180;
  return {
    x: cx + r * Math.sin(rad),
    y: cy - r * Math.cos(rad),
  };
}

/** Formatea un objeto Date a "HH:MM". */
function _fmtTime(d) {
  if (!d || isNaN(d)) return '--:--';
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

/** Bearing a cardinal español (8 posiciones). */
function _bearingToCardinal(b) {
  const dirs = ['Norte','Noreste','Este','Sureste','Sur','Suroeste','Oeste','Noroeste'];
  return dirs[Math.round(((b % 360) + 360) % 360 / 45) % 8];
}

/** Actualiza la etiqueta de hora bajo el slider. */
function _updateTimeLabel(hour, isUp) {
  const el = document.getElementById('solar-time-label');
  if (!el) return;
  const h = Math.floor(hour);
  const m = Math.round((hour % 1) * 60);
  el.textContent = `${isUp ? '☀' : '🌙'} ${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}`;
}
