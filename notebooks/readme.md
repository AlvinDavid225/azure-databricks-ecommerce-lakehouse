
# 🏗️ Azure Databricks E-Commerce Lakehouse Pipeline

> **Production-grade Enterprise Lakehouse** built on Azure Databricks — transforming 1.5M+ messy e-commerce records into a governed, trusted, analytics-ready platform using Medallion Architecture, Unity Catalog, Delta Lake MERGE, and automated Databricks Workflows.

| What | Detail |
|---|---|
| Dataset | Olist Brazilian E-Commerce — 9 CSV files |
| Total Records | 1,556,460 rows ingested |
| Layers | Bronze (9 tables) → Silver (7 tables) → Gold (5 tables) |
| Key Tech | PySpark, Delta Lake, Unity Catalog, Databricks Workflows |
| Governance | Metastore, External Locations, Managed Identity, Lineage |
| Automation | Scheduled pipeline — daily at 2 AM IST |
| Dashboard | 5 business charts — Revenue, Products, Sellers, Customers |
| Challenges Solved | Schema drift, Late payments, Data quality, Idempotency, Star schema |

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apache-spark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-blue)
![Azure Databricks](https://img.shields.io/badge/Azure_Databricks-Premium-red?logo=databricks)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-purple)
![ADLS Gen2](https://img.shields.io/badge/ADLS-Gen2-blue?logo=microsoft-azure)

---

## Architecture Overview

![Architecture Simple](docs/screenshots/architecture_simple.png)

---

## Detailed Architecture

![Architecture Detailed](docs/screenshots/architecture_detailed.png)

---

## Business Problem

An e-commerce company has raw transactional data landing in cloud storage as messy CSV files. Analysts cannot trust the data:

- Duplicates and nulls across order and payment records
- Inconsistent schemas and data types
- No single source of truth
- Every team building its own one-off extracts
- Conflicting numbers in reports

**This project solves all of that.**

---

## Solution

A governed Bronze/Silver/Gold Lakehouse on Delta Lake that delivers clean, deduplicated, business-ready Gold tables. Every team consumes one trusted source, data quality is enforced at each layer, and the pipeline runs automatically every night.

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
- 9 related CSV files
- ~1.5M rows across all tables
- Domain: Orders, Customers, Products, Payments, Reviews, Sellers

---

## Azure Infrastructure

![Resource Groups](docs/screenshots/azure_resource_groups.png)

![ADLS Containers](docs/screenshots/adls_containers.png)

![Raw Files](docs/screenshots/adls_raw_files.png)

---

## Data Volume

| Layer | Tables | Total Rows |
|---|---|---|
| Bronze | 9 | 1,556,460 |
| Silver | 7 | 553,646 |
| Gold | 5 | 248,546 |

---

## Bronze Layer

Raw ingestion layer. Reads CSV files from ADLS `raw/` container and writes as Delta tables with audit columns.

**Audit columns added:**
- `_ingest_ts` — timestamp of when data arrived
- `_source_file` — source file path for lineage

![Bronze Tables](docs/screenshots/uc_bronze_tables.png)

---

## Silver Layer

Data quality and cleaning layer.

**What Silver does:**
- Removes invalid data (review scores outside 1-5)
- Casts data types (string → integer, string → timestamp)
- MERGE INTO on orders and payments (idempotent upserts)
- Delta constraints enforce hard data quality rules

**Delta Constraints applied:**

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
