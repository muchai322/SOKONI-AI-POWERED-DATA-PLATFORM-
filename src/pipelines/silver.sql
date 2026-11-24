CREATE OR REFRESH MATERIALIZED VIEW products_silver AS
SELECT
  TRIM(product_id) AS product_id,
  CAST(current_price_kes AS DECIMAL(10,2)) AS unit_price_kes
FROM read_files(
  'abfss://sokoni@sokoniadls.dfs.core.windows.net/bronze_layer/products/',
  format => 'csv',
  header => true
);

CREATE OR REFRESH MATERIALIZED VIEW sales_silver (
  CONSTRAINT keys_present EXPECT (sale_id IS NOT NULL AND customer_id IS NOT NULL AND product_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_quantity EXPECT (quantity BETWEEN 1 AND 5) ON VIOLATION DROP ROW,
  CONSTRAINT payment_known EXPECT (payment_method IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT price_present EXPECT (unit_price_kes IS NOT NULL) ON VIOLATION DROP ROW
) AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY sale_id
      ORDER BY CASE WHEN quantity BETWEEN 1 AND 5 THEN 0 ELSE 1 END,
               _source_file, product_id, sale_date
    ) AS rn
  FROM sales_bronze
)
SELECT
  r.sale_id,
  r.customer_id,
  COALESCE(r.store_id, 'UNKNOWN') AS store_id,
  r.product_id,
  CAST(r.sale_date AS DATE) AS sale_date,
  CAST(r.quantity AS INT) AS quantity,
  CASE UPPER(TRIM(r.payment_method))
    WHEN 'M-PESA' THEN 'M-Pesa'
    WHEN 'MPESA' THEN 'M-Pesa'
    WHEN 'CASH' THEN 'Cash'
    WHEN 'CARD' THEN 'Card'
  END AS payment_method,
  CAST(COALESCE(r.unit_price_kes, p.unit_price_kes) AS DECIMAL(10,2)) AS unit_price_kes,
  r._source_file
FROM ranked r
LEFT JOIN products_silver p ON r.product_id = p.product_id
WHERE r.rn = 1;