FMCG Data Engineering Project

Databricks lakehouse pipeline for consolidating FMCG sales data from a parent company and a child company. The pipeline uses PySpark, Delta Lake, and Unity Catalog-style three-layer processing to create analytics-ready customer, product, pricing, and order data.

## What this project does

The project:

- Loads parent-company reference and order data into the `fmcg` catalog.
- Loads child-company CSV files from landing locations.
- Applies data-quality rules to customers, products, prices, and orders.
- Stores raw and transformed data in Bronze, Silver, and Gold schemas.
- Merges child-company dimensions into the parent-company dimensions.
- Aggregates child-company daily orders to the parent company's monthly grain.
- Merges child-company facts into the parent-company fact table.
- Creates an enriched view for reporting and dashboard consumption.

## Architecture

```text
Parent company CSV / Volume
        |
        v
fmcg.gold.dim_customers
fmcg.gold.dim_products
fmcg.gold.dim_gross_price
fmcg.gold.fact_orders
        ^
        | merge dimensions and monthly facts
        |
Child company landing CSV files
        |
        v
Bronze Delta tables
        |
        v
Silver cleaned and standardized tables
        |
        v
Gold child-company tables
        |
        v
fmcg.gold.vw_fact_orders_enriched
        |
        v
Dashboard / reporting
```

The child-company processing uses the following schemas:

| Layer | Purpose |
| --- | --- |
| `fmcg.bronze` | Raw CSV records plus ingestion metadata such as `read_timestamp`, `file_name`, and `file_size`. |
| `fmcg.silver` | Deduplicated, validated, standardized, and joined records. |
| `fmcg.gold` | Business-ready dimensions, facts, and reporting views. |

All pipeline tables are Delta tables, and Change Data Feed is enabled by the notebooks where applicable.

## Repository structure

```text
0_data/
├── 1_parent_company/
│   ├── full_load/
│   └── incremental_load/
└── 2_child_company/
    └── full_load/

1_codes/
├── 1_setup/
│   ├── setup_catalog.ipynb
│   ├── utilities.ipynb
│   └── dim_date_table_creation.ipynb
├── 2_dimension_data_processing/
│   ├── 1_customers_data_processing.ipynb
│   ├── 2_products_data_processing.ipynb
│   └── 3_pricing_data_processing.ipynb
└── 3_fact_data_processing/
    ├── 1_full_load_fact.ipynb
    └── 2_incremental_load_fact.ipynb

2_dashboarding/
└── denormalise_table_query_fmcg.txt

resources/
├── project_architecture.png
└── databricks_project.excalidraw
```

## Data model

### Parent-company Gold tables

The parent-company tables are the conformed targets that receive child-company data:

- `fmcg.gold.dim_date`
- `fmcg.gold.dim_customers`
- `fmcg.gold.dim_products`
- `fmcg.gold.dim_gross_price`
- `fmcg.gold.fact_orders`

### Child-company processing tables

The child-company notebooks create tables using the configured `catalog`, `bronze`, `silver`, and `gold` values:

- Bronze: `fmcg.bronze.customers`, `fmcg.bronze.products`, `fmcg.bronze.gross_price`, `fmcg.bronze.orders`
- Silver: `fmcg.silver.customers`, `fmcg.silver.products`, `fmcg.silver.gross_price`, `fmcg.silver.orders`
- Gold staging/output: `fmcg.gold.sb_dim_customers`, `fmcg.gold.sb_dim_products`, `fmcg.gold.sb_dim_gross_price`, `fmcg.gold.sb_fact_orders`

The fact-processing notebooks also use `fmcg.bronze.staging_orders` during incremental processing.

## End-to-end execution flow

Run the notebooks in this order.

### 1. Create the catalog and schemas

Run `1_codes/1_setup/setup_catalog.ipynb`.

This creates:

```sql
fmcg
fmcg.bronze
fmcg.silver
fmcg.gold
```

The notebook also switches the active catalog to `fmcg`.

### 2. Create the date dimension

Run `1_codes/1_setup/dim_date_table_creation.ipynb`.

The notebook generates one row per month from January 2024 through December 2025 and derives:

- `date_key`
- `year`
- `month_name`
- `month_short_name`
- `quarter`
- `year_quarter`

The result is written to `fmcg.gold.dim_date`. The pipeline uses a monthly date grain because the parent-company fact data is monthly.

### 3. Load parent-company data

Load the parent-company full-load data into the parent Gold tables before processing child-company data:

- `dim_customers.csv` into `fmcg.gold.dim_customers`
- `dim_products.csv` into `fmcg.gold.dim_products`
- `dim_gross_price.csv` into `fmcg.gold.dim_gross_price`
- `fact_orders.csv` into `fmcg.gold.fact_orders`

The parent-company incremental fact file can be loaded with `0_data/1_parent_company/incremental_load/incremental_data_parent_company_query.txt`. That query uses `COPY INTO` to append typed records into `fmcg.gold.fact_orders`.

### 4. Process customers

Run `1_codes/2_dimension_data_processing/1_customers_data_processing.ipynb`.

Flow:

1. Read customer CSV files.
2. Add ingestion metadata.
3. Write raw data to `fmcg.bronze.customers`.
4. Remove duplicate `customer_id` values.
5. Trim customer names.
6. Correct known city spelling variations.
7. Null out cities outside the accepted list.
8. Fill selected missing cities from a correction mapping.
9. Convert `customer_id` to string.
10. Build the conformed `customer` value as `customer_name-city`.
11. Add parent-compatible attributes:
    - `market = India`
    - `platform = Sports Bar`
    - `channel = Acquisition`
12. Write cleaned records to `fmcg.silver.customers`.
13. Write the Gold projection to `fmcg.gold.sb_dim_customers`.
14. Merge child customers into `fmcg.gold.dim_customers` using `customer_code`.

### 5. Process products

Run `1_codes/2_dimension_data_processing/2_products_data_processing.ipynb`.

Flow:

1. Read product CSV files into Bronze.
2. Remove duplicate `product_id` values.
3. Normalize category capitalization.
4. Correct the `Protien` spelling in product names and categories.
5. Map categories to business divisions.
6. Extract the product variant from parentheses in the product name.
7. Generate a deterministic SHA-256 `product_code` from the product name.
8. Replace invalid product IDs with `999999`.
9. Rename `product_name` to `product`.
10. Write the Silver table and Gold projection.
11. Merge the child products into `fmcg.gold.dim_products` using `product_code`.

### 6. Process gross prices

Run `1_codes/2_dimension_data_processing/3_pricing_data_processing.ipynb` after products have been processed.

Flow:

1. Read price CSV files into Bronze.
2. Parse the `month` column from supported date formats.
3. Convert valid `gross_price` values to numeric values.
4. Convert negative prices to positive values.
5. Replace non-numeric prices with `0`.
6. Join prices to `fmcg.silver.products` to obtain `product_code`.
7. Write the Silver table and Gold projection.
8. Select the latest non-zero price for each product and year.
9. Merge the yearly price into `fmcg.gold.dim_gross_price` as `price_inr`.

The product notebook must run before the pricing notebook because pricing uses the product mapping from Silver.

### 7. Run the child-company full fact load

Run `1_codes/3_fact_data_processing/1_full_load_fact.ipynb`.

Flow:

1. Read all files from the orders landing path.
2. Add ingestion metadata and append records to `fmcg.bronze.orders`.
3. Move processed landing files to the processed directory.
4. Remove rows with missing `order_qty`.
5. Replace invalid customer IDs with `999999`.
6. Remove weekday text from order dates.
7. Parse order dates from multiple supported formats.
8. Remove duplicate order lines.
9. Join orders to `fmcg.silver.products` to obtain `product_code`.
10. Merge the cleaned daily orders into `fmcg.silver.orders`.
11. Create or merge the child Gold fact table `fmcg.gold.sb_fact_orders`.
12. Convert daily orders to monthly grain by truncating dates to the first day of the month.
13. Sum `sold_quantity` by month, product, and customer.
14. Merge the monthly child facts into `fmcg.gold.fact_orders`.

### 8. Run the child-company incremental fact load

Run `1_codes/3_fact_data_processing/2_incremental_load_fact.ipynb` whenever new order files arrive.

The incremental flow is similar to the full load, but it first writes the newly arrived batch to `fmcg.bronze.staging_orders`. That staging table is then used to:

1. Process only the current batch.
2. Merge new or changed rows into the Silver orders table.
3. Merge new or changed rows into the child Gold fact table.
4. Recalculate the affected monthly product/customer combinations.
5. Merge those monthly recalculations into `fmcg.gold.fact_orders`.
6. Remove the temporary staging data.

The notebook also contains logic to merge the relevant child-company months with the parent-company monthly fact data.

## Reporting view

Run the SQL in `2_dashboarding/denormalise_table_query_fmcg.txt` after the Gold tables are available.

It creates:

```sql
fmcg.gold.vw_fact_orders_enriched
```

The view joins:

- `fact_orders` to `dim_date`
- `fact_orders` to `dim_customers`
- `fact_orders` to `dim_products`
- `fact_orders` to `dim_gross_price`

It exposes the reporting attributes and calculates:

```text
total_amount_inr = sold_quantity * price_inr
```

The view is the recommended source for dashboards and analysis.

## Databricks configuration

The notebooks use widgets with these defaults:

| Widget | Default |
| --- | --- |
| `catalog` | `fmcg` |
| `data_source` | `customers`, `products`, `gross_price`, or `orders` depending on the notebook |

The notebooks currently construct external paths such as:

```text
s3://sportsbar-final/customers/*.csv
s3://sportsbar-final/products/*.csv
s3://sportsbar-final/gross_price/*.csv
s3://sportsbar-final/orders/landing/
```

Before running in another workspace, update the source paths or provide equivalent mounted/volume paths. The notebooks also use:

```text
%run /Workspace/consolidated_pipeline/1_setup/utilities
```

If the notebooks are stored under a different Databricks workspace folder, update this `%run` path. The local CSV files under `0_data/` are the project input samples; they are not automatically uploaded to Databricks by the notebooks.

## Running the project

1. Create or select a Databricks workspace with a cluster that supports PySpark and Delta Lake.
2. Upload or import the notebooks under `1_codes/`.
3. Upload the required CSV files to the configured cloud storage, volume, or mounted path.
4. Update the `base_path`, `landing_path`, and `%run` utility path if required.
5. Run the setup notebook.
6. Create the date dimension.
7. Load the parent-company Gold tables.
8. Run customer and product processing.
9. Run pricing processing.
10. Run the full fact load for the initial child-company load.
11. Run the incremental fact load for later order batches.
12. Create the enriched reporting view.
13. Connect the dashboard to `fmcg.gold.vw_fact_orders_enriched`.

## Data-quality rules at a glance

| Dataset | Main rules |
| --- | --- |
| Customers | Deduplicate IDs, trim names, standardize cities, repair selected missing cities, standardize casing. |
| Products | Deduplicate IDs, normalize category casing, correct spelling, derive division and variant, generate product codes. |
| Prices | Parse multiple date formats, validate numeric values, normalize negative prices, map product IDs to product codes, select yearly latest non-zero price. |
| Orders | Remove missing quantities, standardize customer IDs, parse multiple date formats, deduplicate order lines, map products, aggregate to monthly grain. |

## Notes and limitations

- The implementation is designed for a monthly reporting grain, not a daily parent-company fact grain.
- Invalid customer and product identifiers are replaced with fallback values so that fact records are not silently discarded.
- The fallback and correction rules should be reviewed before production use.
- Several notebooks use `mode("overwrite")` for Bronze/Silver/Gold child-company tables. Use care when rerunning them against shared production tables.
- The pricing merge condition currently matches by `product_code`; if multiple years per product must be retained in the target, the merge condition should also include `year`.
- The dashboard SQL assumes that order dates are aligned to the first day of the month, as produced by the fact aggregation.
