from databricks.connect import DatabricksSession

DIM_PRODUCT = "sokoni_retail.gold.dim_product"


def build_dim_product(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DIM_PRODUCT} AS
        SELECT
            product_id,
            product_name,
            category,
            brand,
            unit,
            pack_size,
            current_price_kes,
            CASE pack_size
                WHEN 'Small' THEN 'Small'
                WHEN 'Regular' THEN 'Regular'
                WHEN 'Family' THEN 'Family'
                ELSE 'Other'
            END AS pack_group
        FROM sokoni_retail.silver.products_clean
    """)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    build_dim_product(spark)
    print("dim_product rows:", spark.table(DIM_PRODUCT).count())