# Healthcare Claims Audit — Databricks Lakehouse

🇪🇸 [Versión en español](README.es.md)

End-to-end pipeline that audits healthcare provider billing (Colombian RIPS claims in JSON, Resolution 2275/2023) against contracted rates, flags overbilling and anomalous billing patterns, and ranks financial risk by provider and month.

> All data is **100% synthetic** (`generar_rips_sinteticos.py`, fixed seed). No data comes from a real insurer, provider or person.

## Results (12 months: Oct 2025 – Sep 2026)

![Audit dashboard in Databricks AI/BI](docs/tablero.png)

| Metric | Value |
| --- | --- |
| RIPS invoices processed | 10,101 |
| Services audited | 90,430 |
| Billed amount | COP 3,212 million |
| Potential claim denials identified | **COP 54.9 million** (1.7%) |
| Services with findings | 2,385 (2.6%) |
| Value at risk in provider-month alerts | **COP 49.1 million** (6 alerts) |
| Service-level rules — precision / recall | 100% / 99.96% |
| Provider-month alerts — precision / recall | 100% / 100% |

Precision and recall are measured against anomalies **seeded and labeled** by the generator. Labels are never used to build the rules.

## Problem

A health insurer (EPS) receives thousands of invoices with their RIPS every month. Auditing them manually against each contract (fee-for-service, PGP global budget, capitation) is slow and misses two kinds of issues:

1. **Service-level errors**: charges above the contracted rate, non-contracted services, duplicates, impossible dates, sex-incompatible diagnoses.
2. **Patterns no single row reveals**: a PGP contract exceeding its monthly band, or a provider that starts systematically billing the most expensive consultation codes (*upcoding*) even though each one is priced correctly.

## Architecture

```mermaid
flowchart LR
    A[RIPS JSON<br/>per invoice] -->|Auto Loader| B[(Bronze<br/>bronze_rips)]
    R[Contract rates, PGP bands,<br/>providers, ICD-10] --> BR[(Bronze<br/>bronze_ref_*)]
    B --> S[(Silver<br/>silver_servicios)]
    BR --> S
    S --> G1[(Gold<br/>gold_prestador_mes)]
    G1 --> G2[(Gold<br/>gold_ejecucion_pgp)]
    G1 --> G3[(Gold<br/>gold_alertas)]
    G2 --> G3
    G3 --> D[Dashboard]
    E[Seeded labels] -.evaluation.-> S
    E -.evaluation.-> G3
```

| Layer | Notebook | What it does |
| --- | --- | --- |
| Setup | `00_setup` | Creates schema and volumes in Unity Catalog, generates the data |
| Bronze | `01_bronze_ingesta` | Incremental JSON ingestion with Auto Loader + reference tables |
| Silver | `02_silver_servicios` | Flattens invoice → patient → service, applies 5 audit rules, values each service against the contract |
| Gold | `03_gold_prestador_mes` | Provider-month aggregates, PGP execution vs. bands, pattern alerts |

## Rules and alerts

**Service level (Silver)**

| Rule | Detects |
| --- | --- |
| `DUPLICADO` | Same provider, patient, code, date and amount billed more than once |
| `NO_CONTRATADO` | Code not in the provider's contract (`NT_NO_CRUZA`) |
| `SOBRECOSTO` | Unit price above the contracted rate (5% tolerance) |
| `FECHA_INVALIDA` | Service before birth, in the future, or unreadable |
| `DX_SEXO_INCOMPATIBLE` | Diagnosis restricted to the other sex |

**Provider-month level (Gold)**

| Alert | Detects | Value at risk |
| --- | --- | --- |
| `SOBREEJECUCION_PGP` | Monthly execution above the band's upper limit (110%) | Excess over the limit |
| `CAMBIO_PATRON_CONSULTAS` | Average consultation price > 1.2 × median of the provider's previous 6 months | Difference × consultations |

## Design decisions

- **Silver flags, it does not drop.** In auditing, the finding is the product; every service keeps its `dq_*` flags.
- **Real business valuation logic:** `VALORIZADO_SEGUN_NT` / `NT_NO_CRUZA`; recognized value = quantity × min(billed, contracted rate).
- **Evaluation separated from construction:** labels live in `eval_*` tables and are used only to measure.
- **Reproducible:** fixed seed; anyone gets the same data and results.

## Known limitations

- Rules reach ~100% because service-level anomalies were seeded with rule-detectable patterns. On real data, precision depends on the quality of the contract rates.
- The only false negative is overbilling on a very low-cost drug that, after rounding, falls inside the 5% tolerance.
- The pattern alert uses a historical baseline: if a provider keeps inflating, its baseline gets contaminated and the signal weakens month after month (from ×1.29 to ×1.21 in the current data).

## How to run it

1. Create a [Databricks Free Edition](https://www.databricks.com/learn/free-edition) account.
2. **Workspace → Create → Git folder** with this repo's URL.
3. Run `notebooks/00_setup` once to create the schema and generate the data.
4. Run the `health-claims-audit-pipeline` job (or notebooks `01` → `02` → `03` in order) on Serverless compute.

## Orchestration

A Lakeflow Job chains the layers with dependencies: `bronze_ingesta` → `silver_servicios` → `gold_prestador_mes`, on Serverless compute. If a layer fails, downstream layers do not run.

![Lakeflow Job](docs/job.png)

## Stack

Databricks Free Edition · PySpark · Delta Lake · Unity Catalog · Auto Loader · Lakeflow Jobs · Databricks AI/BI Dashboards · Git Folders

## Roadmap

- [x] Synthetic data generator with labeled anomalies
- [x] Bronze: incremental JSON ingestion
- [x] Silver: flattening, data quality and contract valuation
- [x] Gold: provider metrics, PGP execution and pattern alerts
- [x] Evaluation of rules and alerts against labels
- [x] Databricks AI/BI dashboard (`dashboards/consultas_tablero.sql`)
- [x] Orchestration with Lakeflow Jobs
- [ ] CI/CD with Automation Bundles and GitHub Actions
