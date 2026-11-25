from databricks.connect import DatabricksSession

BRONZE_PATH = "abfss://sokoni@sokoniadls.dfs.core.windows.net/bronze_layer/stores/"
SILVER = "sokoni_retail.silver.stores_clean"


def transform_stores(spark):
    spark.read.option("header", True).csv(BRONZE_PATH).createOrReplaceTempView("stores_bronze")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER} AS
        SELECT
            TRIM(store_id) AS store_id,
            TRIM(store_name) AS store_name,
            TRIM(city) AS city,
            TRIM(region) AS region,
            TRIM(store_tier) AS store_tier
        FROM stores_bronze
    """)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    transform_stores(spark)
    print("stores_clean rows:", spark.table(SILVER).count())

    from databricks.connect import DatabricksSession

DIM_STORE = "sokoni_retail.gold.dim_store"


def build_dim_store(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DIM_STORE} AS
        SELECT store_id, store_name, city, region, store_tier
        FROM sokoni_retail.silver.stores_clean
        UNION ALL
        SELECT 'UNKNOWN', 'Unknown store', 'Unknown', 'Unknown', 'Unknown'
    """)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    build_dim_store(spark)
    print("dim_store rows:", spark.table(DIM_STORE).count())