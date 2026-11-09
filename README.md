# QGI 4.0 — Local SCDI & Columnar Parquet Engine (2025)

> [!IMPORTANT]
> **Historical Archive (`QGI v4.0`):** This repository is part of the preserved evolutionary archive of **Quantitative Geopolitical Intelligence (QGI)**. Active production development, multi-account cloud infrastructure, and machine learning now reside in **QGI 7.0** at [`AminAlAit/qgi`](https://github.com/AminAlAit/qgi) (Live at [qgintelligence.com](https://qgintelligence.com)).

## QGI Generational Lineage (`v1.0` → `v7.0`)

| Generation | Period | Architecture & Milestone | Repository |
|---|---|---|---|
| **QGI 1.0** | Mar–Oct 2023 | Initial Pairwise Country Correlation Engine & Streamlit Prototype | `AminAlAit/trojan`, `AminAlAit/trojan-streamlit` |
| **QGI 2.0** | Nov 2023 | Soft Power & Institutional Composite Index Prototype | `AminAlAit/trojan-2.0` |
| **QGI 3.0** | Jan–Feb 2024 | Standalone Multi-Page Streamlit Analytical Terminal | [`AminAlAit/qgi-streamlit`](https://github.com/AminAlAit/qgi-streamlit) |
| **QGI 4.0** | Feb–Sep 2025 | **Local SCDI Formalization, MySQL→Parquet Engine (Pipelines A–E) & X Bot v1** | **`AminAlAit/qgi-4.0`** *(this repo)* |
| **QGI 5.0** | Sep 2025–Mar 2026 | First-Generation AWS Cloud Migration (Decoupled Lambda & Batch Microservices) | `AminAlAit/qgi-pipeline-a/b/c-*` |
| **QGI 6.0** | Mar–Aug 2026 | 7-Repo Multi-Account AWS Cloud (14.3B FFT Correlations) & Trajectory Engine (42 Models) | `AminAlAit/qgi-{aws-tf,infra,pipeline,data,ml,frontend}` |
| **QGI 7.0** | Oct 2026–Present | **Unified Public Monorepo & Sovereign IP Vault (Active Production)** | [`AminAlAit/qgi`](https://github.com/AminAlAit/qgi) & `AminAlAit/qgi-docs` |

---

## Architectural Overview (QGI 4.0)

**QGI 4.0** marked the formalization of the **Single Dyadic Correlating Index (SCDI)** pipeline and the transition from relational MySQL tables to high-throughput local columnar Apache Parquet storage (`pyarrow`), laying the algorithmic foundation for the subsequent AWS cloud migrations (QGI 5.0–7.0).

### Core Pipeline Topology (`src/pipelines/` & `src/bots/`)

1. **Pipeline A — World Bank Indicators ETL (`a_wb_indicators_etl/`):**
   - Automated indicator extraction and longitudinal normalization across sovereign countries.
2. **Pipeline B — SCDI Finder (`b_scdi_finder/`):**
   - Pairwise cross-country indicator lag/lead correlation discovery and statistical significance filtering.
3. **Pipeline C — Pattern Maker (`c_pattern_maker/`):**
   - Multi-indicator structural pattern synthesis over discovered SCDI dyads.
4. **Pipeline D — Recipe Maker (`d_recipe_maker/`):**
   - Historical geopolitical and macroeconomic event precursor recipe assembly.
5. **Pipeline E — Top Patterns Generator (`e_create_top_patterns/`):**
   - Ranking and curation of top-tier structural analogues into `top_patterns.parquet`.
6. **Automated X Outreach Bot (`src/bots/x/`):**
   - First-generation automated social publishing pipeline reading curated patterns and posting visual insights.

### Repository Layout

```text
qgi-4.0/
├── src/
│   ├── bots/x/                        # Automated X publishing bot (v1)
│   ├── pipelines/
│   │   ├── a_wb_indicators_etl/       # Pipeline A: World Bank indicator ingestion
│   │   ├── b_scdi_finder/             # Pipeline B: Dyadic SCDI correlation engine
│   │   ├── c_pattern_maker/           # Pipeline C: Structural pattern synthesis
│   │   ├── d_recipe_maker/            # Pipeline D: Event precursor recipe builder
│   │   └── e_create_top_patterns/     # Pipeline E: Top pattern curation
│   └── utils/                         # Shared Parquet & logging utilities
├── notebooks/                         # Exploratory research, network analysis & Athena EDA
├── tests/                             # Unit & pipeline integration tests
└── pyproject.toml                     # Poetry dependency specification
```
