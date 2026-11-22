from databricks.connect import DatabricksSession

BRONZE_PATH = "abfss://sokoni@sokoniadls.dfs.core.windows.net/bronze_layer/customers/"
SILVER = "sokoni_retail.silver.customers_clean"


def transform_customers(spark):
    spark.read.option("header", True).csv(BRONZE_PATH).createOrReplaceTempView("customers_bronze")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER} AS
        SELECT
            TRIM(customer_id) AS customer_id,
            TRIM(name) AS name,
            LOWER(TRIM(email)) AS email,
            TRIM(city) AS city,
            TRIM(region) AS region
        FROM customers_bronze
    """)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    transform_customers(spark)
    print("customers_clean rows:", spark.table(SILVER).count())