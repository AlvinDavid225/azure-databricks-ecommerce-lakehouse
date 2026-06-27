# Azure Databricks E-Commerce Lakehouse

> End-to-end Medallion Architecture pipeline on Azure Databricks — processing 1.5M+ Brazilian e-commerce records into governed, business-ready analytics tables with Unity Catalog, Delta Lake, and automated orchestration.

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apache-spark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-blue)
![Azure Databricks](https://img.shields.io/badge/Azure_Databricks-Premium-red?logo=databricks)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-purple)
![ADLS Gen2](https://img.shields.io/badge/ADLS-Gen2-blue?logo=microsoft-azure)

---

## Architecture

![Architecture](architecture/architecture.svg)

---

## Business Problem

An e-commerce company has raw transactional data landing in cloud storage as messy CSV files. Analysts cannot trust the data:

- Duplicates and nulls across order and payment records
- Inconsistent schemas and data types
- No single source of truth for reporting
- Every team building its own one-off extracts
- Conflicting numbers across reports

**This project solves all of that** — a governed Bronze/Silver/Gold Lakehouse on Delta Lake that delivers clean, deduplicated, business-ready Gold tables. Every team consumes one trusted source, data quality is enforced at each layer, and the pipeline runs automatically every night.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Storage | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks |
| Processing | PySpark |
| Table Format | Delta Lake |
| Governance | Unity Catalog |
| Orchestration | Databricks Workflows |
| Dashboard | Databricks AI/BI Dashboard |
| Version Control | GitHub |

---

## Dataset

**Brazilian E-Commerce Public Dataset (Olist)**

- Source: [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- 9 related CSV files across orders, customers, products, payments, reviews, sellers
- ~1.5M rows across all tables
- Date range: October 2016 – August 2018

---

## Azure Infrastructure

![Resource Groups](docs/screenshots/azure_resource_groups.png)

![ADLS Containers](docs/screenshots/adls_containers.png)

| Resource | Name |
|---|---|
| Storage Account | stlakehousedev225 |
| Resource Group | data-engineering-dev |
| Databricks Workspace | dbw-enterprise-lakehouse |
| UC Metastore | metastore-lakehouse-dev (Central India) |
| Catalog | ecommerce_dev |
| Schemas | bronze · silver · gold |

---

## Data Volume

| Layer | Tables | Total Rows |
|---|---|---|
| Bronze | 9 | 1,556,460 |
| Silver | 7 | 553,646 |
| Gold | 5 | 248,546 |

---

## Bronze Layer

Raw ingestion layer. Reads 9 CSV files from ADLS `raw/` container and writes as Delta tables with audit columns.

**Key decisions:**
- Store raw data as Delta (not CSV) from first touch — enables time travel, audit trails, schema evolution
- `mergeSchema=True` handles schema drift automatically without breaking the pipeline
- Audit columns `_ingest_ts` and `_source_file` added to every table for lineage

![Raw Files](docs/screenshots/adls_raw_files.png)

![Bronze Tables](docs/screenshots/uc_bronze_tables.png)

---

## Silver Layer

Data quality and cleaning layer. Reads from Bronze, applies business rules, and writes cleaned Delta tables.

**What Silver does:**
- Removes invalid data (review scores outside 1–5)
- Casts data types (string → integer, string → timestamp)
- MERGE INTO on `orders` and `payments` — idempotent upserts handle late-arriving data without duplicates
- Delta constraints enforce hard data quality rules at table level

**Delta constraints applied:**
```sql
order_id IS NOT NULL
payment_value >= 0
review_score BETWEEN 1 AND 5
price > 0
```

![Silver Tables](docs/screenshots/uc_silver_tables.png)

![MERGE Output](docs/screenshots/merge_output_orders.png)

---

## Gold Layer — Star Schema

Business analytics layer. Joins 7 Silver tables into a dimensional model optimized for reporting.

```
fact_orders (113,425 rows)
├── dim_customer  (99,441 rows)
├── dim_product   (32,951 rows)
├── dim_seller     (3,095 rows)
└── dim_date         (634 rows)
```

**Aggregations applied:**
- Payments aggregated per order (`SUM payment_value`)
- Reviews aggregated per order (`AVG review_score`)

**Performance optimizations:**
- `OPTIMIZE` — compacted 4 files into 1 on `fact_orders`
- `ZORDER BY (order_date, customer_id)` — speeds up date range and customer join queries

![Gold Tables](docs/screenshots/uc_gold_tables.png)

![ADLS Gold Container](docs/screenshots/adls_gold_container.png)

---

## Unity Catalog Governance

![Catalog Explorer](docs/screenshots/uc_catalog_explorer.png)

![External Locations](docs/screenshots/external_locations.png)

**Governance setup:**
- Metastore: `metastore-lakehouse-dev` (Central India)
- Catalog: `ecommerce_dev` with `bronze`, `silver`, `gold` schemas
- External locations: `ext_bronze`, `ext_silver`, `ext_gold` backed by ADLS Gen2
- Storage credential: Access Connector (Managed Identity) — no keys or secrets
- Table comments on all Gold tables
- Full data lineage tracked automatically

**Unity Catalog Lineage — fact_orders:**

![Lineage 1](docs/screenshots/uc_lineage_fact_orders_1.png)

![Lineage 2](docs/screenshots/uc_lineage_fact_orders_2.png)

---

## Delta Time Travel

```python
# Query any previous version of a Delta table
spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .table("ecommerce_dev.gold.fact_orders")
```

![Delta History](docs/screenshots/delta_history_fact_orders.png)

`fact_orders` has 3 versions tracked: initial write, OPTIMIZE run, and table comment added.

---

## Dashboard

Databricks AI/BI Dashboard built on Gold layer queries via Unity Catalog.

![Dashboard](docs/screenshots/dashboard.png)

| Chart | Type | Insight |
|---|---|---|
| Revenue Trend by Month | Line | Growth from Oct 2016 → peak ~1M/month in late 2017 |
| Top 10 Product Categories by Revenue | Donut | `beleza_saude` and `relogios_presentes` lead |
| Sellers by State | Bar | SP (São Paulo) dominates — ~9M total revenue |
| Customer Distribution by State | Bar | SP leads with ~40K customers |
| Order Status Distribution | Bar | 97%+ delivered — pipeline health confirmed |

SQL queries for all 5 charts available in [`sql/`](sql/).

---

## Orchestration — Databricks Workflows

Automated pipeline running daily at **2:00 AM IST**.

![Job Run Graph](docs/screenshots/job_run_graph.png)

![Job Run Success](docs/screenshots/job_run_success.png)

**Job:** `ecommerce_medallion_pipeline`

| Task | Notebook | Duration |
|---|---|---|
| bronze_ingest | 01_bronze_ingest | 8m 33s |
| silver_clean | 02_silver_clean | 1m 7s |
| gold_model | 03_gold_model | 34s |

**Total runtime: 10m 16s · Launched by scheduler · Status: Succeeded**

Workflow definition available in [`workflows/ecommerce_medallion_pipeline.yml`](workflows/ecommerce_medallion_pipeline.yml).

---

## Engineering Features

| Feature | Implemented |
|---|---|
| Medallion Architecture (Bronze/Silver/Gold) | ✅ |
| Delta Lake table format | ✅ |
| Unity Catalog governance | ✅ |
| Star schema modelling | ✅ |
| Databricks Workflows orchestration | ✅ |
| Delta Time Travel | ✅ |
| MERGE INTO (idempotent upserts) | ✅ |
| Delta constraints (data quality) | ✅ |
| Idempotent pipeline design | ✅ |
| OPTIMIZE + ZORDER (performance tuning) | ✅ |
| Audit columns (_ingest_ts, _source_file) | ✅ |
| Schema drift handling (mergeSchema) | ✅ |
| External tables (ADLS-backed) | ✅ |
| Managed Identity authentication | ✅ |
| Table comments and lineage tracking | ✅ |

---

## Performance Optimization

| Optimization | Applied To | Result |
|---|---|---|
| `OPTIMIZE` | fact_orders | Compacted 4 files → 1 file |
| `ZORDER BY (order_date, customer_id)` | fact_orders | Faster date range and customer join queries |
| External Delta tables | All layers | Storage persists independently of workspace |
| Serverless SQL Warehouse | Dashboard | Instant query startup, no cluster warm-up |

---

## Challenges and Solutions

| Challenge | Solution |
|---|---|
| Duplicate records on re-runs | MERGE INTO — idempotent upserts, no duplicates |
| Late-arriving payments | Composite key MERGE (order_id + payment_sequential) |
| Invalid review scores | `filter(isin("1","2","3","4","5"))` + Delta constraint |
| Schema drift from upstream CSVs | `mergeSchema=True` on Bronze writes |
| Query performance on 113K fact rows | OPTIMIZE + ZORDER on order_date and customer_id |
| Storage–compute coupling | External Delta tables backed by ADLS Gen2 |
| Governance across 3 layers | Unity Catalog with external locations and managed identity |

---

## Project Metrics

| Metric | Value |
|---|---|
| Source CSV files | 9 |
| Bronze tables | 9 |
| Silver tables | 7 |
| Gold tables | 5 |
| Total records processed | 1.5M+ |
| fact_orders rows | 113,425 |
| Dashboard charts | 5 |
| Workflow tasks | 3 |
| Pipeline runtime | ~10 minutes |
| Data quality constraints | 4 |
| Unity Catalog external locations | 3 |

---

## Key Learnings

1. **MERGE INTO vs overwrite** — overwrite deletes everything and rewrites; MERGE is idempotent and safe for incremental production loads
2. **Two-level data quality** — cleaning at ingestion time catches known issues; Delta constraints catch any future bad data automatically
3. **External tables matter** — dropping a table should never delete your data; external tables decouple storage from compute
4. **Composite keys for multi-row entities** — a single order can have multiple payment rows; matching on `order_id` alone causes incorrect merges
5. **Unity Catalog lineage is automatic** — no manual documentation needed; the engine tracks Silver → Gold flows without any extra code
6. **Schema drift is inevitable** — building `mergeSchema` in from the start prevents pipeline failures when upstream adds columns

---

## Project Structure

```
azure-databricks-ecommerce-lakehouse/
├── architecture/
│   └── architecture.svg
├── dashboard/
│   └── dashboard.png
├── docs/
│   └── screenshots/
├── notebooks/
│   ├── 01_bronze_ingest.ipynb
│   ├── 02_silver_clean.ipynb
│   ├── 03_gold_model.ipynb
│   └── 04_unity_catalog_operations.ipynb
├── sql/
│   ├── 01_revenue_trend_by_month.sql
│   ├── 02_top_product_categories_by_revenue.sql
│   ├── 03_order_status_distribution.sql
│   ├── 04_seller_revenue_by_state.sql
│   └── 05_customer_distribution_by_state.sql
├── workflows/
│   └── ecommerce_medallion_pipeline.yml
├── LICENSE
└── README.md
```

---

## How to Run

**Prerequisites:**
- Azure subscription with Databricks workspace (Premium tier)
- ADLS Gen2 storage account
- Unity Catalog metastore configured with Access Connector

**Steps:**
1. Upload 9 Olist CSVs to ADLS `raw/` container
2. Run `04_unity_catalog_operations` — creates catalog, schemas, external locations
3. Run `01_bronze_ingest` — ingests CSVs into 9 Bronze Delta tables
4. Run `02_silver_clean` — cleans and writes to 7 Silver tables
5. Run `03_gold_model` — builds star schema in Gold

Or schedule automatically via `workflows/ecommerce_medallion_pipeline.yml`.

---

## Resume Bullets

- Designed and built an end-to-end Lakehouse on Azure Databricks using Medallion Architecture (Bronze/Silver/Gold), processing 1.5M+ e-commerce records into governed, business-ready analytics tables
- Implemented idempotent incremental loads with Delta MERGE INTO, schema enforcement via Delta constraints, and OPTIMIZE/ZORDER for query performance
- Established Unity Catalog governance with 3-level namespace, external locations backed by ADLS Gen2, managed identity authentication, and automatic data lineage tracking
- Orchestrated end-to-end pipeline using Databricks Workflows with 3-task dependency chain scheduled daily at 2 AM IST — validated end-to-end by full pipeline rerun

---

## Author

**Alvin David**  
Data Engineer | Kochi, Kerala  
GitHub: [github.com/AlvinDavid225](https://github.com/AlvinDavid225)
