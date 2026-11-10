# QGI 4.0 — Local SCDI Econometric Engine, Pipelines A–E & X Bot v1 (2025)

**Generation 4.0 of 7.0** · Originally developed under **`AminAlAit/qgi-4.0`** · **Current Production Platform:** [**`AminAlAit/qgi` (QGI 7.0)**](https://github.com/AminAlAit/qgi) ([qgintelligence.com](https://qgintelligence.com))

---

## Overview

Built in 2025 (`76` commits), **QGI 4.0** is the generation where Quantitative Geopolitical Intelligence crystallized its core mathematical and pipeline architecture:
1. **Formalization of the SCDI Metric (`src/pipelines/b_scdi_finder/scdi_finder.py`):** Replaced single-metric correlation with the **Spearman / Cross-Correlation / Dynamic Time Warping Index (SCDI)** — evaluating monotonic rank order, lagged phase alignment, and elastic shape distance simultaneously across bilateral country time-series segments.
2. **The 5-Stage Pipeline Architecture (`Pipelines A → E`):** Split the monolithic engine into five sequential stages (`a_indicator_extractors`, `b_scdi_finder`, `c_pattern_synthesizer`, `d_recipe_maker`, and `e_trends_analyzer`) — the exact five-stage conceptual taxonomy that still powers **QGI 7.0** today.
3. **MySQL-to-Parquet Storage Migration:** Transitioned storage from local MySQL tables to partitioned **Apache Parquet** (`countries_sdci_parquet/`, `country_patterns_parquet/`) and initiated early **Amazon Athena** exploratory notebooks (`notebooks/athena_eda/eda.ipynb`).
4. **Automated X/Twitter Outreach Bot (`src/bots/x/x.py`):** Shipped `QGIXBot`, an automated analytical publisher that surfaced high-SCDI bilateral trajectory matches, rendered charts, and published thread briefings via the X API v2.

---

## The 5-Stage Pipeline (`src/pipelines/`)

| Stage | Package Path | Core Class / Module | Responsibility |
| :--- | :--- | :--- | :--- |
| **Pipeline A** | `src/pipelines/a_indicator_extractors/` | `wb_indicators_etl.py` | Extracts and normalizes World Bank macroeconomic indicator panels across 200+ countries |
| **Pipeline B** | `src/pipelines/b_scdi_finder/` | `ScdiFinder`, `SDCI` dataclass (`scdi_finder.py`) | Computes pairwise Spearman rank correlation, lagged cross-correlation, and DTW distance across sliding country windows |
| **Pipeline C** | `src/pipelines/c_pattern_synthesizer/` | `pattern_synthesizer.py` | Joins single-indicator SCDI matches into multi-indicator structural patterns shared by Country A and Country B |
| **Pipeline D** | `src/pipelines/d_recipe_maker/` | `recipe_maker.py` | Links multi-indicator patterns to historical geopolitical events (`events.ipynb`) to discover predictive "Recipes" |
| **Pipeline E** | `src/pipelines/e_trends_analyzer/` | `trends_analyzer.py` | Evaluates current-window country trajectories against historical Recipes to surface active risk trends |

---

## Repository Structure

```text
qgi-4.0/
├── src/
│   ├── pipelines/
│   │   ├── a_indicator_extractors/    # Stage A: World Bank WDI ingestion
│   │   ├── b_scdi_finder/             # Stage B: ScdiFinder & SDCI dataclass (Spearman + XCorr + DTW)
│   │   ├── c_pattern_synthesizer/     # Stage C: Multi-indicator pattern synthesis
│   │   ├── d_recipe_maker/            # Stage D: Event-linked geopolitical recipe discovery
│   │   └── e_trends_analyzer/         # Stage E: Active country trajectory scoring
│   ├── bots/
│   │   └── x/                         # QGIXBot automated X API v2 chart & insight publisher (x.py)
│   └── utils/                         # Database connectors, Parquet I/O & logging helpers
├── notebooks/
│   ├── athena_eda/                    # Early AWS Athena SQL exploration (eda.ipynb)
│   ├── events.ipynb                   # Historical geopolitical event catalog curation
│   ├── patterns.ipynb                 # Multi-indicator pattern inspection
│   └── recipes.ipynb                  # Recipe backtesting & threshold tuning
├── tests/                             # Pytest unit test suite across pipeline stages
├── pyproject.toml                     # Poetry project & dependency configuration
└── poetry.lock                        # Locked dependency graph
```

---

## Why We Outgrew QGI 4.0

- **Single-Machine Compute Wall:** Computing pairwise SCDI matrices across hundreds of indicators and all country pairs on a single local workstation took days of continuous CPU execution and produced tens of gigabytes of local Parquet partitions.
- **Move to the Cloud:** To scale from millions to billions of pairwise checks, Pipelines A, B, and C had to be containerized and distributed across **AWS Lambda**, **AWS Batch (with FFT vectorization)**, **Amazon S3**, and **Amazon Athena** — which became **QGI 5.0**.

---

## QGI Generational Roadmap (`v1.0` → `v7.0`)

| Generation | Year | Repository | Original Name(s) | Architectural Milestone |
| :--- | :--- | :--- | :--- | :--- |
| **QGI 1.0** | 2023 | [`AminAlAit/qgi-1.0`](https://github.com/AminAlAit/qgi-1.0) | `trojan`, `trojan-streamlit` | Initial pairwise country correlation engine & first Streamlit UI |
| **QGI 2.0** | 2023 | [`AminAlAit/qgi-2.0`](https://github.com/AminAlAit/qgi-2.0) | `trojan-2.0` | Poetry-packaged ETLP pipeline, `Index` OOP hierarchy & STUMPY matrix profiles |
| **QGI 3.0** | 2024 | [`AminAlAit/qgi-3.0`](https://github.com/AminAlAit/qgi-3.0) | `qgi` (2024), `qgi-streamlit`, `qgi-local` | Multi-page Streamlit analytical terminal backed by relational SQLite/MySQL queries |
| **QGI 4.0** *(Here)* | 2025 | **`AminAlAit/qgi-4.0`** | `qgi-4.0` | Formalization of the SCDI metric (Spearman + Cross-Correlation + DTW), Pipelines A–E & X Bot v1 |
| **QGI 5.0** | 2025–2026 | [`AminAlAit/qgi-5.0`](https://github.com/AminAlAit/qgi-5.0) | `qgi-pipeline-a/b/c` | First AWS cloud migration: Serverless Lambda ETL, Dockerized AWS Batch FFT worker & Athena CTAS |
| **QGI 6.0** | 2026 | `AminAlAit/qgi-6.0` | `qgi-aws-tf`, `qgi-infra`, `qgi-pipeline`, `qgi-data`, `qgi-ml`, `qgi-frontend`, `qgi-docs` | 4-account AWS Organization (14.3B FFT correlations), 42-model Trajectory Engine & Next.js 16 terminal |
| **QGI 7.0** *(Active)* | 2026–Present | [**`AminAlAit/qgi`**](https://github.com/AminAlAit/qgi) | `qgi`, `qgi-docs` | Unified production monorepo & live analytical terminal at [qgintelligence.com](https://qgintelligence.com) |