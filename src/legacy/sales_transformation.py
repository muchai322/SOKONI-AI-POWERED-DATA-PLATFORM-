from pyspark.sql import functions as F
from databricks.connect import DatabricksSession

BRONZE = "sokoni_retail.bronze.sales_ingested"
SILVER = "sokoni_retail.silver.sales_clean"
REJECTED = "sokoni_retail.silver.sales_rejected"
PRODUCTS_PATH = "abfss://sokoni@sokoniadls.dfs.core.windows.net/bronze_layer/products/"


def load_products(spark):
    (spark.read.option("header", True).csv(PRODUCTS_PATH)
          .select("product_id", F.col("current_price_kes").cast("double").alias("unit_price_kes"))
          .createOrReplaceTempView("products"))


def build_normalised(spark):
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW sales_normalised AS
        WITH ranked AS (
            SELECT s.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY s.sale_id
                       ORDER BY s._source_file, s.product_id, s.sale_date
                   ) AS rn
            FROM {BRONZE} s
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
        LEFT JOIN products p ON r.product_id = p.product_id
        WHERE r.rn = 1
    """)


def write_outputs(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER} AS
        SELECT
            sale_id, customer_id, store_id, product_id, sale_date, quantity,
            payment_method, unit_price_kes,
            ROUND(quantity * unit_price_kes, 2) AS revenue_kes,
            _source_file
        FROM sales_normalised
        WHERE quantity BETWEEN 1 AND 5
          AND payment_method IS NOT NULL
          AND unit_price_kes IS NOT NULL
    """)

    spark.sql(f"""
        CREATE OR REPLACE TABLE {REJECTED} AS
        SELECT *,
            CASE
                WHEN quantity NOT BETWEEN 1 AND 5 THEN 'invalid_quantity'
                WHEN payment_method IS NULL THEN 'unknown_payment_method'
                WHEN unit_price_kes IS NULL THEN 'missing_price'
            END AS reject_reason
        FROM sales_normalised
        WHERE NOT (quantity BETWEEN 1 AND 5
                   AND payment_method IS NOT NULL
                   AND unit_price_kes IS NOT NULL)
    """)


def transform_sales(spark):
    load_products(spark)
    build_normalised(spark)
    write_outputs(spark)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    transform_sales(spark)

    print("silver rows:", spark.table(SILVER).count())
    print("rejected rows:", spark.table(REJECTED).count())