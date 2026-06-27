# Azure Databricks E-Commerce Lakehouse Pipeline

End-to-end Medallion Architecture (Bronze/Silver/Gold) data pipeline built on Azure Databricks, processing 1.5M+ Brazilian e-commerce records into governed, business-ready analytics tables.

---

## Architecture

Olist CSVs (Raw Data)

↓

ADLS Gen2 — raw/ container

↓

Databricks Bronze Layer

(Delta tables, audit columns, _ingest_ts, _source_file)

↓

Databricks Silver Layer

(Cleaned, typed, deduplicated, MERGE INTO, Delta constraints)

↓

Databricks Gold Layer

(Star schema — fact_orders + 4 dimensions)

↓

Databricks AI/BI Dashboard

↓

Databricks Workflow (Scheduled daily at 2 AM IST)
---

## Business Problem

An e-commerce company has raw transactional data landing in cloud storage as messy CSV files. Analysts cannot trust the data — duplicates, nulls, inconsistent schemas, and no single source of truth. Every team builds its own extracts, creating conflicting numbers in reports.

---

## Solution

A governed Bronze/Silver/Gold Lakehouse on Delta Lake that delivers clean, deduplicated, business-ready Gold tables with Unity Catalog governance.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Storage | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks |
| Processing | PySpark |
| Table Format | Delta Lake |
| Governance | Unity Catalog |
| Orchestration | Databricks Workflows |
| Dashboard | Databricks AI/BI Dashboard |

---

## Dataset

**Brazilian E-Commerce Public Dataset (Olist)**
- Source: Kaggle
- 9 related CSV files
- ~1.5M rows across all tables

---

## Data Volume

| Layer | Tables | Total Rows |
|---|---|---|
| Bronze | 9 | 1,556,460 |
| Silver | 7 | 553,646 |
| Gold | 5 | 248,546 |

---

## Gold Layer — Star Schema

fact_orders (113,425 rows)

├── dim_customer  (99,441 rows)

├── dim_product   (32,951 rows)

├── dim_seller    (3,095 rows)

└── dim_date      (634 rows)

---

## Key Challenges Solved

**1. Schema Drift**
Added `mergeSchema=true` to Bronze writes — handles new columns automatically without breaking the pipeline.

**2. Late-Arriving Payments**
Replaced `mode("overwrite")` with Delta `MERGE INTO` on orders and payments tables — idempotent upserts handle late arrivals without duplicates.

**3. Data Quality Enforcement**
Enforced at two levels:
- Cleaning during ingestion (invalid review scores removed, wrong types cast)
- Delta constraints as hard rules:
  - `order_id IS NOT NULL`
  - `payment_value >= 0`
  - `review_score BETWEEN 1 AND 5`
  - `price > 0`

**4. Joining 9 Normalized Tables**
Built a star schema Gold layer joining orders, items, payments, and reviews into `fact_orders` with aggregated payment totals and average review scores per order.

**5. Idempotent Re-runs**
Validated end-to-end by dropping all tables, clearing ADLS containers, and rerunning the full pipeline — all 21 tables passed row count validation.

---

## Unity Catalog Governance

- Metastore: `metastore-lakehouse-dev` (Central India)
- Catalog: `ecommerce_dev`
- Schemas: `bronze`, `silver`, `gold`
- External Locations: `ext_bronze`, `ext_silver`, `ext_gold`
- Storage Credential: Access Connector (Managed Identity)
- Table Comments on all Gold tables
- OPTIMIZE + ZORDER on `fact_orders` (order_date, customer_id)
- Time Travel demonstrated across all Delta versions

---

## Project Structure

azure-databricks-ecommerce-lakehouse/

├── notebooks/

│   ├── 01_bronze_ingest.ipynb

│   ├── 02_silver_clean.ipynb

│   ├── 03_gold_model.ipynb

│   └── 04_unity_catalog_operations.ipynb

└── README.md

---

## How to Run

1. Upload Olist CSVs to ADLS `raw/` container
2. Configure Unity Catalog metastore and external locations
3. Run `01_bronze_ingest` — ingests 9 CSVs into Bronze Delta tables
4. Run `02_silver_clean` — cleans and writes to Silver
5. Run `03_gold_model` — builds star schema in Gold
6. Run `04_unity_catalog_operations` — applies governance

Or run automatically via Databricks Workflow job `ecommerce_medallion_pipeline`.

---

## Dashboard Insights

Built on Databricks AI/BI Dashboard querying Gold tables directly:

- Revenue Trend by Month (2016-2018)
- Top 10 Product Categories by Revenue
- Order Status Distribution (97% delivered)
- Top 10 Sellers by State (SP dominant)
- Customer Distribution by State

---

## Resume Bullets

- Designed and built an end-to-end Lakehouse on Azure Databricks using Medallion Architecture (Bronze/Silver/Gold), processing 1.5M+ e-commerce records into governed, business-ready tables
- Implemented idempotent incremental loads with Delta MERGE INTO, schema enforcement, and OPTIMIZE/ZORDER, ensuring pipeline reliability through full end-to-end rerun validation
- Established Unity Catalog governance (3-level namespace, external locations, storage credentials, table comments) and delivered AI/BI dashboards consumed by analytics stakeholders
- Enforced data quality at two levels — cleaning during ingestion and Delta constraints as hard rules rejecting future bad data automatically

---

## Author

**Alvin David**
Data Engineer | Kochi, Kerala
GitHub: github.com/AlvinDavid225
