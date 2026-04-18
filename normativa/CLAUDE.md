# EdificIA — Contexto para el asistente

## Dominio

EdificIA es una plataforma de factibilidad urbanistica para Buenos Aires (CABA).
Permite evaluar oportunidades de desarrollo inmobiliario usando datos de ~280.000
parcelas, normativa del Codigo Urbanistico (CUR, Ley 6099/2018), y servicios del GCBA.

## Conceptos clave

- **SMP**: Seccion-Manzana-Parcela (ej. "016-044-038"). Normalizado: "16-44-38".
- **CUR**: Codigo Urbanistico (Ley 6099/2018). Regula alturas, FOT, usos.
- **CPU**: Codigo anterior, mapeado a distritos CUR.
- **Plano Limite (PL)**: Altura maxima de la envolvente edificable.
- **FOT**: Factor de Ocupacion Total (superficie edificable / superficie del terreno).
- **LFI**: Linea de Frente Interno, retiro a ~22m del frente en lotes profundos.
- **Delta**: PL menos altura real existente = oportunidad de desarrollo.
- **Pisada**: Superficie de la planta del edificio.
- **Tejido**: Altura real construida (fotogrametria).

## APIs publicas del GCBA

Estas APIs son publicas, sin autenticacion, licencia CC-BY-2.5-AR:

- EPOK catastro: `https://epok.buenosaires.gob.ar/catastro/parcela/?smp={smp}`
- CUR3D edificabilidad: `https://epok.buenosaires.gob.ar/cur3d/seccion_edificabilidad/?smp={smp}`
- USIG normalizacion: `https://servicios.usig.buenosaires.gob.ar/normalizar/?direccion={dir}`

## Normativa

Lee `ley_6099_resumen.md` en este directorio para detalles sobre la Ley 6099/2018.

## Esquema de la base de datos

Tabla principal: `parcelas` (~280.000 filas). Tabla secundaria: `envelope_sections`.

### Columnas clave de `parcelas`

**Identificacion:**
- `smp` (TEXT) — SMP original (ej. "016-044-038")
- `smp_norm` (TEXT) — SMP normalizado (ej. "16-44-38"), indexado
- `cpu` (TEXT) — Codigo de Planeamiento Urbano (legacy)
- `cur_distrito` (TEXT) — Distrito CUR actual

**Ubicacion:**
- `barrio` (TEXT) — Nombre del barrio en Title Case (ej. "Palermo", "Villa Crespo", "Puerto Madero")
- `comuna` (TEXT) — Numero de comuna
- `lat`, `lng` (REAL) — Coordenadas
- `epok_direccion` (TEXT) — Direccion postal (ej. "GORRITI 5100")
- `epok_calle` (TEXT), `epok_altura` (INTEGER)

**Normativa CUR (que se permite construir):**
- `plano_san` (REAL) — Plano Limite sanitizado en metros (altura max edificable)
- `h` (REAL) — Altura segun CUR (raw, antes de sanitizacion)
- `fot` (REAL) — Factor de Ocupacion Total
- `pisos` (INTEGER) — Pisos permitidos: 1 + floor((plano_san - 3.30) / 2.90)
- `es_aph` (INTEGER) — Area de Proteccion Historica (0/1)

**Dimensiones del lote:**
- `area` (REAL) — Superficie en m2
- `frente` (REAL), `fondo` (REAL) — Medidas en metros
- `pisada` (REAL) — Superficie de planta edificable en m2
- `vol_edificable` (REAL) — Volumen edificable en m3
- `sup_vendible` (REAL) — Superficie vendible estimada en m2

**Construccion existente (que hay construido):**
- `tejido_altura_max` (REAL) — Altura real maxima (fotogrametria)
- `tejido_altura_avg` (REAL) — Altura real promedio
- `delta_altura` (REAL) — plano_san - tejido_altura_max (gap de oportunidad)
- `epok_pisos_sobre` (INTEGER) — Pisos construidos sobre rasante
- `epok_sup_cubierta` (REAL) — Superficie cubierta existente

**Uso del suelo:**
- `uso_tipo1`, `uso_tipo2` (TEXT) — Tipo de uso actual
- `uso_estado` (TEXT) — Estado de uso
- `obra_tipo` (TEXT), `obra_destino` (TEXT), `obra_m2` (REAL) — Permisos de obra

**Edificabilidad CUR3D (datos detallados de GCBA):**
- `edif_sup_max_edificable` (REAL) — Sup. max edificable segun CUR3D
- `edif_plano_limite` (REAL) — PL oficial de CUR3D
- `edif_fot_medianera` (REAL) — FOT entre medianeras
- `edif_riesgo_hidrico` (INTEGER) — Riesgo hidrico (0/1)
- `edif_enrase` (INTEGER) — Permite enrase (0/1)
- `edif_catalogacion_proteccion` (TEXT) — Catalogacion patrimonial

**Flags de enriquecimiento:**
- `epok_enriched` (INTEGER) — 0=pendiente, 1=ok, -1=error
- `cur3d_enriched` (INTEGER) — 0=pendiente, 1=ok, -1=error

### Tabla `envelope_sections`

Secciones de la envolvente 3D (cuerpo, retiro 1, retiro 2) con geometria.
Columnas: `smp`, `tipo`, `altura_inicial`, `altura_fin`, `polygon_geojson`.

### Consultas frecuentes

```sql
-- Top N parcelas con mas delta en un barrio
SELECT smp, epok_direccion, plano_san, tejido_altura_max, delta_altura
FROM parcelas WHERE barrio = ? AND delta_altura > 0
ORDER BY delta_altura DESC LIMIT ?

-- Buscar parcela por direccion
SELECT * FROM parcelas
WHERE epok_direccion LIKE '%GORRITI%' AND barrio = 'Palermo'

-- Estadisticas por barrio
SELECT barrio, COUNT(*) as n, AVG(delta_altura) as avg_delta,
  SUM(vol_edificable) as total_vol
FROM parcelas WHERE delta_altura > 0
GROUP BY barrio ORDER BY avg_delta DESC
```

## Renderizado HTML (render_html)

Cuando uses `render_html`, tu HTML se muestra en un iframe dentro del chat:
- **Ancho**: ~390px (sidebar) o ~700px (pantalla completa)
- **Alto**: se auto-ajusta al contenido (no te preocupes por la altura)
- **Tema**: fondo oscuro (#0a0a0a), texto blanco. Estilos base para tablas,
  headers y fonts se agregan automaticamente.
- **Sin librerias externas**: no podes cargar Chart.js, D3, etc.
  Usa HTML/CSS puro y SVG inline para graficos.

### Buenas practicas
- Para tablas: usa `<table>` simple, sin estilos (los base ya estan).
  Maximo ~15-20 filas visibles.
- Para graficos de barra: usa divs con `width` porcentual y `background-color`.
- Para destacar valores: usa `<strong>` o `style="color:#E8C547"` (amarillo).
- NO expliques al usuario lo que el HTML muestra — ellos ya lo ven.
  Solo agrega contexto que no este en la vista.
- Si los datos son muchos (>30 filas), usa render_html para un resumen
  visual y ofrece create_download para el dataset completo.

## Cuando usar cada herramienta

- **sql**: Para cualquier consulta sobre parcelas, barrios, estadisticas.
  Ya tenes el schema arriba — no necesitas llamar a `schema` primero.
- **schema**: Solo si necesitas verificar un nombre de columna exacto. Normalmente
  no es necesario.
- **http**: Para consultar APIs del GCBA en tiempo real (EPOK, CUR3D, USIG).
- **render_html**: Para tablas, graficos o visualizaciones. Usa `collapsed=true`
  para archivos de descarga que no necesitan vista previa (ej. CSV grandes).
- **Read/Grep/Glob**: Para leer archivos de normativa en este directorio.
