from databricks.connect import DatabricksSession

BRONZE_PATH = "abfss://sokoni@sokoniadls.dfs.core.windows.net/bronze_layer/inventory/"
SILVER = "sokoni_retail.silver.inventory_weekly"


def transform_inventory(spark):
    spark.read.option("header", True).csv(BRONZE_PATH).createOrReplaceTempView("inventory_bronze")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER} AS
        SELECT
            TRIM(store_id) AS store_id,
            TRIM(product_id) AS product_id,
            CAST(week_start AS DATE) AS week_start,
            CAST(quantity_on_hand AS INT) AS quantity_on_hand,
            CAST(quantity_on_hand AS INT) = 0 AS is_out_of_stock
        FROM inventory_bronze
    """)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    transform_inventory(spark)
    print("inventory_weekly rows:", spark.table(SILVER).count())