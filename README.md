# Retail Analysis PySpark Pipeline

This repository contains a robust, end-to-end PySpark ETL pipeline built using the **Medallion Architecture** (Bronze, Silver, Gold). The pipeline processes retail e-commerce data (customers, products, and orders), ensures data quality through strict schema enforcement and quarantining, and produces aggregated business KPIs.

## Architecture

The pipeline follows the standard Medallion Architecture:

- **Bronze (Ingestion)**
  - Ingests raw data (CSV, JSON, Excel) into Delta tables.
  - Handles parsing errors and corrupt records by routing them to a `quarantine` directory (using Spark's `PERMISSIVE` mode).
  - Appends ingestion metadata (e.g., `_ingested_at`).

- **Silver (Enrichment & Cleansing)**
  - Cleanses string fields (trimming, casing).
  - Enforces strict schemas and data quality rules. Rows violating non-nullable constraints are automatically isolated into quarantine tables.
  - Masks PII (Personally Identifiable Information) such as emails and phone numbers.
  - Normalizes columns and deduplicates records.

- **Gold (Business Value)**
  - **Master Orders:** Joins cleansed orders with enriched customers and products to create a comprehensive, wide fact table.
  - **Aggregations:** Computes profit aggregations by year, category, sub-category, and customer.
  - Employs Delta Lake `Z-ORDER` optimization for highly performant query execution on high-cardinality columns.

- **Reporting (KPIs)**
  - Reads the Gold aggregations to output final KPI metrics (Profit by Year, Profit by Category, Profit by Customer, etc.).

## Project Structure

```text
.
├── pyproject.toml            # Project dependencies and pytest configuration
├── src/
│   └── retail_analysis/
│       ├── configs/              # etl configs , spark configs
│       └── databricks/
│           ├── notebooks/        # Pipeline orchestration and execution stages
│           │   ├── ingestion.py
│           │   ├── enrich_customers_products.py
│           │   ├── master_orders.py
│           │   ├── aggregate.py
│           │   ├── kpis.py
│           │   └── run_pipeline.py  # End-to-end orchestrator
│           └── utils/            # Shared utilities (schema, DQ, transforms, logger)
└── tests/                    # Comprehensive Pytest test suite (unit, edge cases)
```

## Key Features

- **Robust Data Quality:** String standardization, duplicate handling, and schema enforcement.
- **Config-Driven Ingestion:** Source paths and table configurations are dynamically read from `etl.yml`.
- **Fault-Tolerant Pipelines:** "Bad" data doesn't fail the pipeline. Violations are automatically quarantined into Delta tables for downstream analysis, while valid records continue flowing.
- **Delta Lake Integration:** Utilizes Delta format natively for ACID transactions, upserts (`MERGE`), and `Z-ORDER` performance tuning.
- **Test-Driven:** High test coverage using Pytest, parametrized fixtures, and mocked Spark sessions handling both happy paths and tricky edge cases.

## Setup and Execution

### Prerequisites

- Python 3.11+
- Apache Spark (with Delta Lake and Crelytics Spark Excel dependencies)
- Required Python packages (defined in `pyproject.toml`)

### Installation

1. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install the project and dependencies:
   ```bash
   pip install -e .[dev]
   ```

### Running the Pipeline

To execute the entire end-to-end pipeline (Bronze → Silver → Gold → Reporting):

```bash
python -m src.retail_analysis.databricks.notebooks.run_pipeline
```

### Running Tests

The project uses `pytest` with custom markers for each layer of etl. 

To run the complete test suite:
```bash
pytest tests/ -v
```

To run only specific markers:
```bash
pytest -m unit -v
```
