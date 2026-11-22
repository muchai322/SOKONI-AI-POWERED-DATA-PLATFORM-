from databricks.connect import DatabricksSession

DIM_CUSTOMER = "sokoni_retail.gold.dim_customer"


def build_dim_customer(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DIM_CUSTOMER} AS
        SELECT customer_sk, customer_id, name, email, city, region,
               valid_from, valid_to, is_current
        FROM sokoni_retail.silver.customers_scd2
    """)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    build_dim_customer(spark)

    d = spark.table(DIM_CUSTOMER)
    print("dim_customer rows:", d.count())
    print("current rows:", d.filter("is_current").count())
    print("duplicate surrogate keys:", d.count() - d.select("customer_sk").distinct().count())