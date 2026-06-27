# Azure Databricks E-Commerce Lakehouse

A production-grade **Medallion Architecture** pipeline built on Azure Databricks and Delta Lake, using the Brazilian Olist E-Commerce dataset (100K+ orders, 9 source tables). Demonstrates end-to-end Data Engineering patterns — ingestion, cleaning, modelling, and data quality enforcement — with Unity Catalog governance across all three layers.

---

## Architecture

```
Raw CSVs (ADLS Gen2 Bronze Container)
        |
        v
  [ Bronze Layer ]   — Raw Delta tables, schema-on-read, no transformations
        |
        v
  [ Silver Layer ]   — Cleaned, deduplicated, MERGE INTO upserts, Delta constraints
        |
        v
  [ Gold Layer ]     — Star schema (fact + 4 dims), optimized for analytics
```

All three layers are governed by **Unity Catalog** (`ecommerce_dev` catalog → `bronze` / `silver` / `gold` schemas), with external Delta tables backed by ADLS Gen2.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Compute | Azure Databricks (Unity Catalog cluster) |
| Storage | ADLS Gen2 |
| Table Format | Delta Lake |
| Governance | Unity Catalog + External Locations |
| Language | PySpark, SQL |
| Source Data | Brazilian Olist E-Commerce (Kaggle) |

---

## Project Structure

```
azure-databricks-ecommerce-lakehouse/
├── notebooks/
│   ├── 01_bronze_ingest.py          # Raw CSV → Bronze Delta tables
│   ├── 02_silver_clean.py           # Cleaning, MERGE INTO, Delta constraints
│   ├── 03_gold_model.py             # Star schema fact + dimension tables
│   └── 04_unity_catalog_ops.py      # Unity Catalog setup and validation
├── docs/
│   └── screenshots/                 # Dashboard and pipeline run evidence
└── README.md
```

---

## Pipeline Detail

### Bronze Layer — 9 Tables

Ingests all 9 raw Olist CSVs from ADLS Gen2 into Delta tables with zero transformation. Schema drift is handled via `mergeSchema` option to avoid pipeline failures when upstream columns change.

**Key decision:** Store raw data as Delta (not CSV) from the first touch. This enables time travel for debugging, audit trails, and schema evolution without re-ingesting from source.

Tables: `orders`, `order_items`, `order_payments`, `order_reviews`, `customers`, `products`, `sellers`, `geolocation`, `product_category_name_translation`

### Silver Layer — 7 Tables

Applies business-level cleaning: null removal, type casting, duplicate elimination, and standardised column names. Uses **MERGE INTO** for all incremental loads instead of `overwrite`.

**Why MERGE instead of overwrite?**
`overwrite` deletes everything and rewrites — if the pipeline fails mid-write, data is lost. MERGE is idempotent: running it once or ten times produces the same correct result. Run 1 inserts new rows; Run 2 finds the same rows, updates in place, no duplicates, no data loss.

**Delta constraints** enforce data quality at table level — any write violating a rule is rejected by the engine itself:

```sql
ALTER TABLE ecommerce_dev.silver.order_payments
  ADD CONSTRAINT chk_payment_value CHECK (payment_value >= 0);

ALTER TABLE ecommerce_dev.silver.order_reviews
  ADD CONSTRAINT chk_review_score CHECK (review_score BETWEEN 1 AND 5);
```

This creates two-level quality enforcement: cleaning at ingestion time + hard rejection for any future bad data.

**Late-arriving payments** handled via composite key MERGE (`order_id + payment_sequential`) — a single order can have multiple payment rows, so a single-column key would cause incorrect merges.

### Gold Layer — Star Schema (5 Tables)

Aggregates Silver into an analytics-ready star schema:

| Table | Type | Description |
|---|---|---|
| `fact_orders` | Fact | One row per order with all revenue metrics |
| `dim_customers` | Dimension | Customer location and profile |
| `dim_products` | Dimension | Product category and attributes |
| `dim_sellers` | Dimension | Seller location and profile |
| `dim_date` | Dimension | Date spine for time-based analysis |

**OPTIMIZE and ZORDER** applied at Gold layer for query performance:

```sql
OPTIMIZE ecommerce_dev.gold.fact_orders ZORDER BY (order_date, customer_id);
```

This compacts small Delta files and co-locates related data so date-range queries and customer joins skip unnecessary file reads.

---

## Key Engineering Decisions

**1. Unity Catalog over Hive Metastore**
Unity Catalog provides fine-grained access control, cross-workspace data sharing, and audit lineage — necessary for any production multi-team environment. All three layer schemas live under a single `ecommerce_dev` catalog, making governance and discovery consistent.

**2. External Tables over Managed Tables**
Used external Delta tables backed by ADLS Gen2 external locations (`ext_bronze`, `ext_silver`, `ext_gold`). Dropping a table does not delete the underlying data — critical for production environments where the storage layer outlives any single pipeline or workspace.

**3. Idempotent Pipeline Design**
Every notebook can be re-run without side effects. Bronze uses `mergeSchema` with overwrite for raw snapshots. Silver and Gold use MERGE INTO — the pipeline produces the same correct output whether it runs once or is re-triggered ten times after a failure.

**4. Two-Level Data Quality**
Cleaning logic in PySpark catches nulls and type issues at ingestion time. Delta constraints at table level catch any future writes that violate business rules — even from ad hoc queries or other pipelines writing to the same table.

---

## Dataset

Brazilian Olist E-Commerce Public Dataset — 100K+ orders from 2016–2018, covering orders, payments, reviews, products, sellers, and customer geolocation across Brazil.

Source: [Kaggle — Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## Setup

### Prerequisites
- Azure Databricks workspace with Unity Catalog enabled
- ADLS Gen2 storage account
- Access Connector for Azure Databricks (for Unity Catalog ADLS auth)

### Infrastructure
```
Storage Account : stlakehousedev225
Resource Group  : data-engineering-dev
Databricks WS   : dbw-enterprise-lakehouse
UC Metastore    : metastore-lakehouse-dev (Central India)
Catalog         : ecommerce_dev
Schemas         : bronze | silver | gold
```

### Run Order
```
1. 04_unity_catalog_ops.py   — Create catalog, schemas, external locations
2. 01_bronze_ingest.py       — Ingest raw CSVs into Bronze Delta tables
3. 02_silver_clean.py        — Clean and MERGE into Silver tables
4. 03_gold_model.py          — Build Gold star schema
```

---

## Dashboard

Databricks SQL dashboard with:
- Monthly revenue trend (2016–2018)
- Top products by revenue

> Screenshots in `docs/screenshots/`

---

## What This Demonstrates

- Medallion Architecture (Bronze / Silver / Gold) on Databricks
- Unity Catalog setup: metastore, storage credentials, external locations
- Delta Lake: MERGE INTO, constraints, OPTIMIZE, ZORDER, Time Travel
- Production patterns: idempotency, schema drift handling, composite key upserts
- Star schema modelling for analytics workloads
- Data quality enforcement at both pipeline and table level

---

*Built as part of an Azure Data Engineering portfolio. See also: [NYC Taxi Lakehouse](https://github.com/AlvinDavid225/nyc-taxi-lakehouse) | [Retail ADF Pipeline](https://github.com/AlvinDavid225/retail-adf-pipeline)*
