# Auditoría de facturación en salud — Lakehouse con Databricks

Pipeline end-to-end que audita la facturación de prestadores de salud (RIPS en JSON, Resolución 2275 de 2023) contra la nota técnica contractual, detecta sobrecostos y facturación atípica, y prioriza el riesgo por prestador y mes.

> Todos los datos son **100 % sintéticos** (`generar_rips_sinteticos.py`, semilla fija). Ningún dato proviene de una EPS, IPS o persona real.

## Resultados (12 meses: oct 2025 – sep 2026)

![Tablero de auditoría en Databricks AI/BI](docs/tablero.png)

| Métrica | Valor |
| --- | --- |
| Facturas RIPS procesadas | 10.101 |
| Servicios auditados | 90.430 |
| Valor facturado | $3.212 millones COP |
| Glosa potencial identificada | **$54,9 millones COP** (1,7 %) |
| Servicios con hallazgo | 2.385 (2,6 %) |
| Valor en riesgo en alertas prestador-mes | **$49,1 millones COP** (6 alertas) |
| Reglas por servicio — precisión / recall | 100 % / 99,96 % |
| Alertas prestador-mes — precisión / recall | 100 % / 100 % |

Las métricas de precisión y recall se calculan contra anomalías **sembradas y etiquetadas** por el generador, que nunca se usan para construir las reglas.

## Problema

Una EPS recibe cada mes miles de facturas con sus RIPS. Auditarlas a mano contra lo pactado en cada contrato (evento, PGP, cápita) es lento y deja pasar dos tipos de problemas:

1. **Errores por servicio**: cobros por encima de la tarifa, servicios no contratados, duplicados, fechas imposibles, diagnósticos incompatibles.
2. **Patrones que ninguna fila revela**: un contrato PGP que se sale de su banda, o un prestador que empieza a facturar sistemáticamente las consultas más caras (*upcoding*) aunque cada una cobre la tarifa correcta.

## Arquitectura

```mermaid
flowchart LR
    A[RIPS JSON<br/>por factura] -->|Auto Loader| B[(Bronze<br/>bronze_rips)]
    R[Nota técnica, bandas PGP,<br/>prestadores, CIE-10] --> BR[(Bronze<br/>bronze_ref_*)]
    B --> S[(Silver<br/>silver_servicios)]
    BR --> S
    S --> G1[(Gold<br/>gold_prestador_mes)]
    G1 --> G2[(Gold<br/>gold_ejecucion_pgp)]
    G1 --> G3[(Gold<br/>gold_alertas)]
    G2 --> G3
    G3 --> D[Tablero]
    E[Etiquetas sembradas] -.evaluación.-> S
    E -.evaluación.-> G3
```

| Capa | Notebook | Qué hace |
| --- | --- | --- |
| Setup | `00_setup` | Crea esquema y volúmenes en Unity Catalog, genera los datos |
| Bronze | `01_bronze_ingesta` | Ingesta incremental de JSON con Auto Loader + tablas de referencia |
| Silver | `02_silver_servicios` | Aplana factura → usuario → servicio, aplica 5 reglas de auditoría, valoriza contra la NT |
| Gold | `03_gold_prestador_mes` | Agregados por prestador-mes, ejecución PGP vs. bandas, alertas de patrón |

## Reglas y alertas

**Por servicio (Silver)**

| Regla | Detecta |
| --- | --- |
| `DUPLICADO` | Mismo prestador, usuario, tecnología, fecha y valor facturado más de una vez |
| `NO_CONTRATADO` | Tecnología fuera de la nota técnica del prestador (`NT_NO_CRUZA`) |
| `SOBRECOSTO` | Valor unitario por encima de la tarifa pactada (tolerancia 5 %) |
| `FECHA_INVALIDA` | Atención antes del nacimiento, en el futuro o ilegible |
| `DX_SEXO_INCOMPATIBLE` | Diagnóstico restringido a un sexo distinto al del usuario |

**Por prestador-mes (Gold)**

| Alerta | Detecta | Valor en riesgo |
| --- | --- | --- |
| `SOBREEJECUCION_PGP` | Ejecución mensual por encima del límite superior de la banda (110 %) | Exceso sobre el límite |
| `CAMBIO_PATRON_CONSULTAS` | Valor medio por consulta > 1,2 × la mediana de sus 6 meses previos | Diferencia × consultas |

## Decisiones de diseño

- **Silver marca, no elimina.** En auditoría el hallazgo es el producto; cada servicio conserva sus banderas `dq_*`.
- **Valorización con la lógica real del negocio:** `VALORIZADO_SEGUN_NT` / `NT_NO_CRUZA`, valor reconocido = cantidad × mín(facturado, tarifa NT).
- **Evaluación separada de la construcción:** las etiquetas viven en tablas `eval_*` y solo se usan para medir.
- **Reproducible:** semilla fija; cualquiera obtiene los mismos datos y resultados.

## Limitaciones conocidas

- Las reglas detectan casi el 100 % porque las anomalías por servicio se sembraron con patrones detectables por regla. En datos reales, la precisión dependerá de la calidad de la nota técnica.
- El único falso negativo es un sobrecosto en un medicamento de muy bajo valor que, por redondeo, cae dentro de la tolerancia del 5 %.
- La alerta de patrón usa una línea base histórica: si el prestador sigue inflando, su línea base se contamina y la señal se debilita mes a mes (de ×1,29 a ×1,21 en los datos actuales).

## Cómo ejecutarlo

1. Crea una cuenta en [Databricks Free Edition](https://www.databricks.com/learn/free-edition).
2. **Workspace → Create → Git folder** con la URL de este repo.
3. Ejecuta en orden `notebooks/00_setup` → `01_bronze_ingesta` → `02_silver_servicios` → `03_gold_prestador_mes` sobre cómputo Serverless.

## Stack

Databricks Free Edition · PySpark · Delta Lake · Unity Catalog · Auto Loader · Databricks AI/BI Dashboards · Git Folders

## Hoja de ruta

- [x] Generador de datos sintéticos con anomalías etiquetadas
- [x] Bronze: ingesta incremental de JSON
- [x] Silver: aplanado, calidad y valorización contra NT
- [x] Gold: métricas por prestador, ejecución PGP y alertas de patrón
- [x] Evaluación de reglas y alertas contra etiquetas
- [x] Tablero en Databricks AI/BI (`dashboards/consultas_tablero.sql`)
- [ ] Orquestación con Lakeflow Jobs
- [ ] CI/CD con Automation Bundles y GitHub Actions
