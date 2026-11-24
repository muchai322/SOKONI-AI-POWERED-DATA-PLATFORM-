CREATE OR REFRESH MATERIALIZED VIEW fact_sales (
  CONSTRAINT customer_version_found EXPECT (customer_sk IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT date_found EXPECT (date_key IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT store_found EXPECT (store_name IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT product_found EXPECT (product_name IS NOT NULL) ON VIOLATION FAIL UPDATE
) AS
SELECT
  s.sale_id,
  d.date_key,
  s.sale_date,
  s.store_id,
  st.store_name,
  s.product_id,
  p.product_name,
  c.customer_sk,
  s.customer_id,
  s.quantity,
  s.unit_price_kes,
  ROUND(s.quantity * s.unit_price_kes, 2) AS revenue_kes,
  s.payment_method
FROM sales_silver s
LEFT JOIN dim_date d
  ON s.sale_date = d.full_date
LEFT JOIN dim_store st
  ON s.store_id = st.store_id
LEFT JOIN dim_product p
  ON s.product_id = p.product_id
LEFT JOIN sokoni_retail.gold.dim_customer c
  ON s.customer_id = c.customer_id
 AND s.sale_date BETWEEN c.valid_from AND c.valid_to;

CREATE OR REFRESH MATERIALIZED VIEW fact_inventory_weekly (
  CONSTRAINT date_found EXPECT (date_key IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT store_found EXPECT (store_name IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT product_found EXPECT (product_name IS NOT NULL) ON VIOLATION FAIL UPDATE
) AS
WITH inv AS (
  SELECT
    TRIM(store_id) AS store_id,
    TRIM(product_id) AS product_id,
    CAST(week_start AS DATE) AS week_start,
    CAST(quantity_on_hand AS INT) AS quantity_on_hand
  FROM read_files(
    'abfss://sokoni@sokoniadls.dfs.core.windows.net/bronze_layer/inventory/',
    format => 'csv',
    header => true
  )
)
SELECT
  d.date_key,
  i.week_start,
  i.store_id,
  st.store_name,
  i.product_id,
  p.product_name,
  i.quantity_on_hand,
  i.quantity_on_hand = 0 AS is_out_of_stock
FROM inv i
LEFT JOIN dim_date d
  ON i.week_start = d.full_date
LEFT JOIN dim_store st
  ON i.store_id = st.store_id
LEFT JOIN dim_product p
  ON i.product_id = p.product_id;