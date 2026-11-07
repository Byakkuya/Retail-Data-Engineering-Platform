::: {align="center"}
# Retail Data Engineering Platform

### End-to-end retail analytics with Databricks, PySpark, Delta Lake & Medallion Architecture

[![Databricks](https://img.shields.io/badge/Platform-Databricks-EA3E3E?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Apache
Spark](https://img.shields.io/badge/Processing-Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Language-Python-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/Language-SQL-4479A1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Delta
Lake](https://img.shields.io/badge/Storage-Delta%20Lake-00ADD8?logo=delta&logoColor=white)](https://delta.io/)

**A unified retail data platform that transforms data from multiple
source systems into trusted, analytics-ready insights.**
:::

------------------------------------------------------------------------

## Overview

This project builds a retail analytics platform on **Databricks**,
integrating data from three source systems:

  -----------------------------------------------------------------------
  Source                              Data
  ----------------------------------- -----------------------------------
  **PostgreSQL**                      Product catalog and inventory

  **Salesforce**                      Customer accounts and sales
                                      opportunities

  **Azure Blob Storage**              Retail transaction files, including
                                      incremental data
  -----------------------------------------------------------------------

The platform follows the **Medallion Architecture** to ingest, clean,
validate, and model data across Bronze, Silver, and Gold layers. A
Databricks Metric View centralizes business definitions, while a SQL
dashboard presents retail performance indicators.

### What the platform delivers

-   Multi-source data ingestion into a centralized Lakehouse.
-   Incremental transaction-file processing with **Databricks Auto
    Loader**.
-   Data cleansing, standardization, and quality checks in the Silver
    layer.
-   Analytics-ready fact and dimension datasets in the Gold layer.
-   Reusable business metrics through **Databricks Metric Views**.
-   A **Retail Sales Performance** dashboard for business analysis.

------------------------------------------------------------------------

## Architecture

The solution uses a layered data architecture, moving from raw source
records to curated business metrics and dashboards.

``` mermaid
flowchart TD
    subgraph Sources["Source Systems"]
        PG["PostgreSQL<br/>Products · Inventory"]
        SF["Salesforce<br/>Accounts · Opportunities"]
        BS["Azure Blob Storage<br/>Transaction CSV files"]
    end

    subgraph Bronze["Bronze Layer — Raw"]
        BPG["PostgreSQL Bronze"]
        BSF["Salesforce Bronze"]
        BBS["Blob Bronze"]
    end

    subgraph Silver["Silver Layer — Clean & Validated"]
        S1["Account"]
        S2["Product Catalog"]
        S3["Inventory"]
        S4["Opportunity"]
        S5["Transactions"]
    end

    subgraph Gold["Gold Layer — Analytics Ready"]
        G1["Fact Sales"]
        G2["Customer Dimension"]
        G3["Product Dimension"]
        G4["Inventory Fact/View"]
        G5["Calendar"]
    end

    M["Databricks Metric View<br/>retail_metrics"]
    D["Databricks SQL Dashboard<br/>Retail Sales Performance"]

    PG --> BPG
    SF --> BSF
    BS --> BBS

    BPG --> S2
    BPG --> S3
    BSF --> S1
    BSF --> S4
    BBS --> S5

    S1 --> G2
    S2 --> G3
    S3 --> G4
    S4 --> G1
    S5 --> G1
    G1 --> M
    G2 --> M
    G3 --> M
    G5 --> M
    M --> D
```

### Medallion layers

  -----------------------------------------------------------------------
  Layer                   Role                    Examples
  ----------------------- ----------------------- -----------------------
  🥉 **Bronze**           Land source data with   PostgreSQL, Salesforce,
                          minimal transformation. and Blob Storage tables

  🥈 **Silver**           Clean, standardize,     Accounts, products,
                          validate, and enrich    inventory,
                          records.                opportunities,
                                                  transactions

  🥇 **Gold**             Publish curated         `fact_sales`,
                          datasets for analytical `dim_customer`,
                          use.                    `dim_product`,
                                                  `fact_inventory`

  **Semantic**            Define reusable         `retail_metrics` Metric
                          business metrics and    View
                          dimensions.             

  **Analytics**           Present business        Databricks SQL
                          insights.               dashboard
  -----------------------------------------------------------------------

All datasets are organized under the `retail_q` Unity Catalog.

------------------------------------------------------------------------

## Repository Structure

``` text
Retail-Data-Engineering/
│
├── 01_Source_data/
│   ├── 01_postgres_product_history.sql
│   ├── 02_postgres_inventory_history.sql
│   ├── 03_salesforce_accounts_history.csv
│   ├── 04_salesforce_opportunities_history.csv
│   ├── 05_blob_transactions_history.csv
│   └── 06_blob_transactions_incremental_100.csv
│
├── 02_Notebooks/
│   ├── 01_blob_to_bronze.py
│   ├── 02_Gold_Views.sql
│   ├── 03_calendar.py
│   └── 04_Metric View.py
│
├── 03_ETL_Pipeline/
│   ├── bronze_to_silver/
│   │   ├── account.py
│   │   ├── product_catalog.py
│   │   ├── inventory.py
│   │   ├── opportunity.py
│   │   └── transactions.py
│   │
│   └── silver_to_gold/
│       └── fact_sales.py
│
└── 05_Dashboard/
    └── Retail Sales Performance Dashboard.lvdash.json
```

------------------------------------------------------------------------

## Data Pipeline

### 1. Ingestion --- Bronze

The Bronze layer is the landing zone for source data.

**Transaction ingestion with Auto Loader**

The notebook `02_Notebooks/01_blob_to_bronze.py` uses Databricks Auto
Loader to ingest CSV transaction files from:

``` text
/Volumes/retail_q/volumes/blob_source/transactions_source/
```

The ingestion process:

1.  Reads CSV files and infers the incoming schema.
2.  Stores schema-evolution metadata in `_schema`.
3.  Writes records to `retail_q.blob_bronze.transactions`.
4.  Maintains ingestion state through a checkpoint in `_checkpoint`.
5.  Uses `availableNow=True` to process currently available files and
    stop.

This supports scheduled incremental ingestion without reprocessing
previously tracked files.

PostgreSQL and Salesforce datasets are expected to be loaded into their
Bronze tables through the configured Databricks ingestion/DLT pipeline:

``` text
retail_q.postgres_bronze.product_catalog
retail_q.postgres_bronze.inventory
retail_q.salesforce_bronze.account
retail_q.salesforce_bronze.opportunity
```

### 2. Transformation --- Silver

Silver transformations use PySpark and `pyspark.pipelines`. Data-quality
expectations are applied to validate records:

-   `expect` --- reports records that fail a constraint without dropping
    them.
-   `expect_or_drop` --- removes records that fail a constraint.

  -----------------------------------------------------------------------
  Dataset                             Main transformations
  ----------------------------------- -----------------------------------
  **Account**                         Standardizes Salesforce column
                                      names, trims and uppercases
                                      customer names, fills missing
                                      industry with `UNKNOWN`, preserves
                                      deletion metadata, derives
                                      `is_active` from CDC metadata, and
                                      validates IDs and names.

  **Product Catalog**                 Standardizes product identifiers
                                      and descriptive fields, fills
                                      missing subcategories and brands,
                                      rounds prices, classifies products
                                      into `PREMIUM`, `MID_RANGE`, and
                                      `BUDGET`, retains CDC validity
                                      fields, and adds `processed_at`.

  **Inventory**                       Validates inventory/product/store
                                      identifiers and positive stock
                                      quantities; derives `LOW_STOCK` or
                                      `HEALTHY` from stock quantity and
                                      reorder level.

  **Opportunity**                     Standardizes Salesforce fields,
                                      validates amount, probability, and
                                      stage, classifies deal size, and
                                      preserves ownership, forecast, and
                                      audit information.

  **Transactions**                    Casts quantity, selling price, and
                                      discount fields to integers;
                                      calculates gross amount; parses
                                      timestamps; validates transaction
                                      fields; and retains channel and
                                      opportunity information.
  -----------------------------------------------------------------------

```{=html}
<details>
```
```{=html}
<summary>
```
`<strong>`{=html}Silver implementation notes`</strong>`{=html}
```{=html}
</summary>
```
-   The account and product transformations use CDC metadata, including
    `__END_AT`, to derive active-record status.
-   The transaction transformation currently uses `spark.read.table`
    rather than `spark.readStream.table`. It therefore performs a batch
    read of the managed Bronze table.
-   The transaction timestamp format used by the transformation is
    `dd-MMM-yyyy hh.mm.ss a`.

```{=html}
</details>
```
### 3. Modeling --- Gold

The Gold layer publishes curated datasets for analysis.

#### Fact Sales

`03_ETL_Pipeline/silver_to_gold/fact_sales.py` creates:

``` text
retail_q.retail_gold.fact_sales
```

The fact is built by joining transactions to opportunities using a
case-insensitive, trimmed match between `opportunity_name` and
opportunity `name`.

A **LEFT JOIN** preserves transactions even when no matching opportunity
is found.

The resulting dataset includes transaction identifiers, product/store
keys, quantity, selling price, discounts, payment mode, sales channel,
transaction date, and related opportunity/customer attributes.

#### Analytical views

The notebook `02_Notebooks/02_Gold_Views.sql` exposes:

  -----------------------------------------------------------------------
  Dataset                             Description
  ----------------------------------- -----------------------------------
  `dim_customer`                      Active, non-deleted accounts with
                                      customer and geographic attributes.

  `dim_product`                       Active products with category,
                                      brand, segment, price, and supplier
                                      attributes.

  `fact_inventory`                    Stock quantities, reorder levels,
                                      inventory status, warehouse, and
                                      last-update timestamp.
  -----------------------------------------------------------------------

#### Calendar dimension

`02_Notebooks/03_calendar.py` generates a calendar dataset for a
parameterized `:start_date` to `:end_date` range.

It includes year, quarter, month, week, day, weekday/weekend flags,
year-quarter, year-month, and month-boundary attributes.

------------------------------------------------------------------------

## Semantic Layer --- Databricks Metric Views

The notebook `02_Notebooks/04_Metric View.py` defines:

``` text
retail_q.retail_semantic.retail_metrics
```

The Metric View is built over `retail_q.retail_gold.fact_sales` and
joins product, calendar, and customer dimensions.

### Business dimensions

  Category      Dimensions
  ------------- ----------------------------------------
  Time          Transaction Date, Year, Quarter, Month
  Product       Product Category, Brand
  Sales         Payment Mode, Sales Channel
  Opportunity   Opportunity Stage
  Customer      Customer Type, Customer Name
  Geography     Billing City, State, Country
  Industry      Customer Industry

### Business measures

  Measure                         Definition
  ------------------------------- ----------------------------------------
  **Transaction Count**           `COUNT(1)`
  **Total Revenue**               `SUM(amount)`
  **Total Quantity Sold**         `SUM(quantity)`
  **Total Discount**              `SUM(discount_amount)`
  **Average Transaction Value**   `SUM(amount) / COUNT(1)`
  **Unique Customers**            `COUNT(DISTINCT customer.customer_id)`

The YAML metadata also defines display formats, descriptions, and
synonyms. This provides a consistent semantic layer for Databricks SQL
and natural-language analytics.

------------------------------------------------------------------------

## Dashboard --- Retail Sales Performance

The dashboard is stored at:

``` text
05_Dashboard/Retail Sales Performance Dashboard.lvdash.json
```

It uses the `retail_metrics` dataset, which maps to the Databricks
Metric View.

### Dashboard insights

  Visualization                          What it helps analyze
  -------------------------------------- -------------------------------
  Revenue over time                      Revenue trends
  Revenue by product category            Category performance
  Revenue by brand                       Brand-level performance
  Revenue by billing state and country   Geographic distribution
  Revenue by customer type               Customer-segment contribution
  Revenue by sales channel               Channel performance
  Revenue by payment mode                Payment-method contribution
  Revenue by opportunity stage           Opportunity-stage performance

### Key performance indicators

-   **Total Revenue**
-   **Transaction Count**
-   **Unique Customers**
-   **Average Transaction Value**

> The dashboard queries the semantic Metric View rather than individual
> tables, keeping business definitions centralized and reusable.

------------------------------------------------------------------------

## Execution Workflow

Run the components in dependency order:

  -----------------------------------------------------------------------
                                      Step Action
  ---------------------------------------- ------------------------------
                                         1 Create the `retail_q` catalog,
                                           schemas, volumes, and required
                                           permissions.

                                         2 Load PostgreSQL and Salesforce
                                           history into their Bronze
                                           tables.

                                         3 Upload initial and incremental
                                           transaction CSV files to the
                                           Blob volume.

                                         4 Run `01_blob_to_bronze.py`.

                                         5 Run the Bronze-to-Silver DLT
                                           pipeline.

                                         6 Run the Silver-to-Gold
                                           pipeline to create
                                           `fact_sales`.

                                         7 Execute the Gold SQL views and
                                           generate the calendar
                                           dimension.

                                         8 Create or refresh the
                                           Databricks Metric View.

                                         9 Import/open the dashboard and
                                           validate KPI totals against
                                           Gold data.
  -----------------------------------------------------------------------

For production use, schedule ingestion and transformation pipelines
according to their dependencies. Monitor Auto Loader checkpoints and DLT
expectation results.

------------------------------------------------------------------------

## Deployment Checks

Before running the project in a new workspace, verify the following
implementation dependencies.

```{=html}
<details>
```
```{=html}
<summary>
```
`<strong>`{=html}1. Calendar table naming`</strong>`{=html}
```{=html}
</summary>
```
The Metric View references:

``` text
retail_q.retail_gold.dim_calendar
```

The calendar notebook creates:

``` text
retail_q.retail_gold.calendar
```

Use one name consistently or create a `dim_calendar` view/table.

```{=html}
</details>
```
```{=html}
<details>
```
```{=html}
<summary>
```
`<strong>`{=html}2. Revenue field consistency`</strong>`{=html}
```{=html}
</summary>
```
The Metric View uses:

``` sql
SUM(amount)
```

However, the current `fact_sales` selection exposes `selling_price` and
does not explicitly select an `amount` column.

If `amount` is not present in the deployed fact schema, define the
intended revenue field explicitly (for example, a net transaction
amount) and use it consistently in the Gold model and semantic measures.

```{=html}
</details>
```
```{=html}
<details>
```
```{=html}
<summary>
```
`<strong>`{=html}3. CDC metadata availability`</strong>`{=html}
```{=html}
</summary>
```
Confirm that the deployed ingestion pipeline supplies the CDC columns
`__START_AT` and `__END_AT`, which are used by the account and product
transformations.

```{=html}
</details>
```
```{=html}
<details>
```
```{=html}
<summary>
```
`<strong>`{=html}4. Opportunity stage expectations`</strong>`{=html}
```{=html}
</summary>
```
The sample source data contains a `Qualification` stage, while the
current Silver expectation list allows only `Prospecting`, `Closed Won`,
and `Closed Lost`.

Review the accepted values to ensure they match the source data.

```{=html}
</details>
```

------------------------------------------------------------------------

## Technology Stack

  -----------------------------------------------------------------------
  Area                                Technologies
  ----------------------------------- -----------------------------------
  Lakehouse platform                  Databricks

  Distributed processing              Apache Spark, PySpark

  Storage format                      Delta Lake

  Ingestion                           Databricks Auto Loader

  Pipeline framework                  Delta Live Tables (DLT), Lakeflow
                                      Declarative Pipelines APIs

  Source systems                      PostgreSQL, Salesforce, Azure Blob
                                      Storage

  Data modeling                       Medallion Architecture, fact and
                                      dimension tables

  Semantic modeling                   Databricks Metric Views, YAML

  Analytics                           Databricks SQL Dashboards

  Languages                           Python, SQL
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## Project Highlights

  Capability                   Implementation
  ---------------------------- --------------------------------------
  Multi-source ingestion       PostgreSQL, Salesforce, Blob Storage
  Incremental file ingestion   Databricks Auto Loader
  Layered data processing      Bronze, Silver, and Gold
  Data quality                 DLT expectations
  Analytical modeling          Fact and dimension datasets
  Business metrics             Databricks Metric Views
  Business intelligence        Databricks SQL Dashboard

------------------------------------------------------------------------

::: {align="center"}
**Built with Databricks · PySpark · Delta Lake**

*From raw retail data to reusable business insights.*
:::
