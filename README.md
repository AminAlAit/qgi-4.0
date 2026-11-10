# QGI 4.0 — Local SCDI Econometric Engine & X Bot v1 (2025)

> **[Historical Archive — Generation 4.0 of 7.0]**
> This repository preserves **QGI 4.0** (2025), the pivotal fourth-generation engine that mathematically formalized the **Spearman-Cross-Correlation-DTW Index (SCDI)**, modularized the 5-stage local pipeline (pipeline_a through pipeline_e), transitioned storage from local MySQL (data_prep/) to partitioned Apache Parquet, and launched the first automated X/Twitter analytical bot (x_bot/).
> - **Original Repository Name:** `AminAlAit/qgi-4.0`
> - **Active Production Platform (QGI 7.0):** [**AminAlAit/qgi**](https://github.com/AminAlAit/qgi) ([qgintelligence.com](https://qgintelligence.com))

---

## Architectural Scope (2025)

1. **5-Stage Modular Econometric Pipeline:**
   - **pipeline_a/ (Indicator Extractor):** World Bank API ingestion and time-series normalization (indicator_extractor.py).
   - **pipeline_b/ (SCDI Finder):** Pairwise country-indicator correlation engine combining Spearman rank correlation, lagged cross-correlation, and Dynamic Time Warping (scdi_finder.py).
   - **pipeline_c/ (Pattern Synthesizer):** Multi-indicator structural pattern aggregation (pattern_synthesizer.py).
   - **pipeline_d/ (Recipe Maker):** Composite macro-geopolitical recipe construction (ecipe_maker.py).
   - **pipeline_e/ (Trends Analyzer):** Regime shift and structural break detection (	rends_analyzer.py).
2. **Storage Migration (data_prep/):** Migrated raw World Bank time series from local relational tables (mysql_setup.py, populate_mysql.py) to columnar Parquet (migrate_mysql_to_parquet.py) and early S3 staging (upload_to_s3.py).
3. **Outreach Automation (x_bot/):** First automated chart rendering (chart_generator.py), insight synthesis (content_generator.py), and scheduled social publishing (scheduler.py, 	witter_publisher.py).

---

## QGI Generational Lineage (1.0 – 7.0)

| Generation | Era | Canonical GitHub Repository | Original Repository Name(s) | Key Milestone |
| :--- | :--- | :--- | :--- | :--- |
| **QGI 1.0** | 2023 | `AminAlAit/qgi-1.0` | `trojan`, `trojan-streamlit` | Initial pairwise country correlation engine (countriesIndex.py, patternEngine.py) & first Streamlit prototype |
| **QGI 2.0** | 2023 | `AminAlAit/qgi-2.0` | `trojan-2.0` | Poetry-managed Soft Power & composite institutional index prototype |
| **QGI 3.0** | 2024 | `AminAlAit/qgi-3.0` | `qgi` (2024), `qgi-streamlit`, `qgi-local` | First public Quantitative Geopolitical Intelligence Streamlit prototype & World Bank notebook workflows |
| **QGI 4.0** | 2025 | `AminAlAit/qgi-4.0` | `qgi-4.0` | Formalization of SCDI (Spearman + Cross-Correlation + DTW), local MySQL-to-Parquet engine (Pipelines A–E), and X Bot v1 |
| **QGI 5.0** | 2025–2026 | `AminAlAit/qgi-5.0` | `qgi-pipeline-a-indicator-extractor`, `qgi-pipeline-b-scdi-finder`, `qgi-pipeline-c-pattern-synthesizer` | First AWS cloud migration: Serverless Lambda indicator extractor, Dockerized AWS Batch SCDI compute worker, and Lambda pattern synthesizer |
| **QGI 6.0** | 2026 | `AminAlAit/qgi-6.0` | `qgi-aws-tf`, `qgi-infra`, `qgi-pipeline`, `qgi-data`, `qgi-ml`, `qgi-frontend`, `qgi-docs` | 7-repo multi-account AWS cloud architecture (14.3B FFT correlations), Trajectory Engine (42 event models), and Next.js 16 terminal |
| **QGI 7.0 (Active)** | 2026–Present | [**AminAlAit/qgi**](https://github.com/AminAlAit/qgi) & `AminAlAit/qgi-docs` | `qgi`, `qgi-docs` | Unified polyglot production monorepo & live analytical terminal at [qgintelligence.com](https://qgintelligence.com) |