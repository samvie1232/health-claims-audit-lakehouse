# Auditoría de facturación en salud — Lakehouse con Databricks

Pipeline end-to-end que audita la facturación de prestadores de salud (RIPS en JSON, Resolución 2275 de 2023) contra la nota técnica contractual, detecta sobrecostos y facturación atípica, y publica métricas de riesgo por prestador.

> Todos los datos son **100 % sintéticos** (`generar_rips_sinteticos.py`, semilla fija). Ningún dato proviene de una EPS, IPS o persona real.

## Problema

Una EPS recibe cada mes miles de facturas con sus RIPS. Auditarlas a mano contra lo pactado en cada contrato (evento, PGP, cápita) es lento y deja pasar cobros por encima de la tarifa, servicios no contratados, duplicados y registros inconsistentes.

## Arquitectura (Medallion)

```
RIPS JSON (por factura) ──► BRONZE  ingesta incremental (Auto Loader), JSON crudo + metadatos de carga
Nota técnica, contratos ─┘     │
                               ▼
                           SILVER  aplanado usuarios/servicios, tipado, reglas de calidad
                               │   (expectativas), valorización contra la NT
                               ▼
                           GOLD    métricas por prestador-mes: valor facturado vs. NT,
                                   ejecución PGP vs. bandas, hallazgos por tipo de anomalía
                               ▼
                           Tablero (Databricks AI/BI o Power BI) + detector de anomalías
```

## Stack

Databricks Free Edition · PySpark · Delta Lake · Unity Catalog · Lakeflow Spark Declarative Pipelines · Lakeflow Jobs · Databricks Automation Bundles · GitHub Actions · Power BI

## Datos

```bash
python generar_rips_sinteticos.py --meses 12 --escala 20
```

| Ruta | Contenido |
| --- | --- |
| `data/raw/rips/AAAAMM/*.json` | Un RIPS por factura |
| `data/ref/nota_tecnica.csv` | Tarifa pactada por prestador y tecnología |
| `data/ref/pgp_bandas.csv` | Valor mensual y límites de los contratos PGP |
| `data/ref/prestadores.csv` | Prestadores ficticios y modalidad de contrato |
| `data/labels/anomalias_sembradas.csv` | Anomalías sembradas, para medir precisión y recall del detector |

## Resultados

_Por completar: volumen procesado, tiempo de ejecución, precisión/recall del detector, captura del tablero._

## Hoja de ruta

- [x] Generador de datos sintéticos con anomalías etiquetadas
- [ ] Bronze: ingesta incremental de JSON
- [ ] Silver: aplanado, calidad y valorización contra NT
- [ ] Gold: métricas por prestador y ejecución PGP
- [ ] Detector de anomalías y evaluación contra las etiquetas
- [ ] Tablero
- [ ] CI/CD con Automation Bundles y GitHub Actions
