CREATE OR REFRESH MATERIALIZED VIEW dim_date AS
WITH days AS (
  SELECT explode(sequence(DATE'2025-01-01', DATE'2026-11-30', INTERVAL 1 DAY)) AS full_date
)
SELECT
  CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
  full_date,
  YEAR(full_date) AS year,
  QUARTER(full_date) AS quarter,
  MONTH(full_date) AS month,
  date_format(full_date, 'MMMM') AS month_name,
  trunc(full_date, 'week') AS week_start,
  date_format(full_date, 'EEEE') AS day_name,
  DAY(full_date) AS day_of_month,
  dayofweek(full_date) IN (1, 7) AS is_weekend,
  DAY(full_date) >= 25 OR DAY(full_date) <= 3 AS is_payday_window
FROM days;

CREATE OR REFRESH MATERIALIZED VIEW dim_product (
  CONSTRAINT product_id_present EXPECT (product_id IS NOT NULL) ON VIOLATION FAIL UPDATE
) AS
SELECT
  TRIM(product_id) AS product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  TRIM(brand) AS brand,
  TRIM(unit) AS unit,
  TRIM(pack_size) AS pack_size,
  CAST(current_price_kes AS DECIMAL(10,2)) AS current_price_kes
FROM read_files(
  'abfss://sokoni@sokoniadls.dfs.core.windows.net/bronze_layer/products/',
  format => 'csv',
  header => true
);

CREATE OR REFRESH MATERIALIZED VIEW dim_store (
  CONSTRAINT store_id_present EXPECT (store_id IS NOT NULL) ON VIOLATION FAIL UPDATE
) AS
SELECT
  TRIM(store_id) AS store_id,
  TRIM(store_name) AS store_name,
  TRIM(city) AS city,
  TRIM(region) AS region,
  TRIM(store_tier) AS store_tier
FROM read_files(
  'abfss://sokoni@sokoniadls.dfs.core.windows.net/bronze_layer/stores/',
  format => 'csv',
  header => true
)
UNION ALL
SELECT 'UNKNOWN', 'Unknown store', 'Unknown', 'Unknown', 'Unknown';