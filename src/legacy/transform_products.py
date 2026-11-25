from databricks.connect import DatabricksSession

BRONZE_PATH = "abfss://sokoni@sokoniadls.dfs.core.windows.net/bronze_layer/products/"
SILVER = "sokoni_retail.silver.products_clean"


def transform_products(spark):
    spark.read.option("header", True).csv(BRONZE_PATH).createOrReplaceTempView("products_bronze")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER} AS
        SELECT
            TRIM(product_id) AS product_id,
            TRIM(product_name) AS product_name,
            TRIM(category) AS category,
            TRIM(brand) AS brand,
            TRIM(unit) AS unit,
            TRIM(pack_size) AS pack_size,
            CAST(current_price_kes AS DECIMAL(10,2)) AS current_price_kes
        FROM products_bronze
    """)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    transform_products(spark)
    print("products_clean rows:", spark.table(SILVER).count())