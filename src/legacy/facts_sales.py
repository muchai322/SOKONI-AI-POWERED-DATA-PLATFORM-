from databricks.connect import DatabricksSession

FACT_SALES = "sokoni_retail.gold.fact_sales"


def build_fact_sales(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {FACT_SALES} AS
        SELECT
            s.sale_id,
            CAST(date_format(s.sale_date, 'yyyyMMdd') AS INT) AS date_key,
            s.sale_date,
            s.store_id,
            s.product_id,
            c.customer_sk,
            s.customer_id,
            s.quantity,
            s.unit_price_kes,
            s.revenue_kes,
            s.payment_method
        FROM sokoni_retail.silver.sales_clean s
        LEFT JOIN sokoni_retail.gold.dim_customer c
            ON s.customer_id = c.customer_id
           AND s.sale_date BETWEEN c.valid_from AND c.valid_to
    """)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    build_fact_sales(spark)

    f = spark.table(FACT_SALES)
    print("fact_sales rows:", f.count())
    print("sales with no customer version:", f.filter("customer_sk IS NULL").count())