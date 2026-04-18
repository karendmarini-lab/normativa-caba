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
