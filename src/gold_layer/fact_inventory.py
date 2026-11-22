from databricks.connect import DatabricksSession

FACT_INVENTORY = "sokoni_retail.gold.fact_inventory_weekly"


def build_fact_inventory(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {FACT_INVENTORY} AS
        SELECT
            store_id,
            product_id,
            CAST(date_format(week_start, 'yyyyMMdd') AS INT) AS date_key,
            week_start,
            quantity_on_hand,
            is_out_of_stock
        FROM sokoni_retail.silver.inventory_weekly
    """)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    build_fact_inventory(spark)

    f = spark.table(FACT_INVENTORY)
    print("fact_inventory_weekly rows:", f.count())