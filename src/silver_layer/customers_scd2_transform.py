from databricks.connect import DatabricksSession

CHANGES_PATH = "abfss://sokoni@sokoniadls.dfs.core.windows.net/bronze_layer/customer_changes/"
SILVER = "sokoni_retail.silver.customers_scd2"


def transform_customers_scd2(spark):
    spark.read.option("header", True).csv(CHANGES_PATH).createOrReplaceTempView("changes_bronze")

    # Remove exact duplicate moves and cast the date
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW changes_clean AS
        SELECT DISTINCT
            TRIM(customer_id) AS customer_id,
            TRIM(city) AS city,
            TRIM(region) AS region,
            CAST(change_date AS DATE) AS valid_from
        FROM changes_bronze
    """)

    # Version 0 is the snapshot, valid from the start of the data.
    # Each move becomes a new version with the customer's name and email.
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW versions_raw AS
        SELECT customer_id, name, email, city, region,
               DATE'2025-01-01' AS valid_from, 0 AS src
        FROM sokoni_retail.silver.customers_clean
        UNION ALL
        SELECT c.customer_id, cu.name, cu.email, c.city, c.region,
               c.valid_from, 1 AS src
        FROM changes_clean c
        JOIN sokoni_retail.silver.customers_clean cu ON c.customer_id = cu.customer_id
    """)

    # If two versions start on the same day, keep one deterministically.
    # A move beats the snapshot, then city order breaks any remaining tie.
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW versions AS
        SELECT customer_id, name, email, city, region, valid_from
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY customer_id, valid_from
                                      ORDER BY src DESC, city) AS rn
            FROM versions_raw
        ) t
        WHERE rn = 1
    """)

    # Each version ends the day before the next one starts.
    # The latest version is open-ended and flagged as current.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER} AS
        WITH ordered AS (
            SELECT *,
                   LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) AS next_from
            FROM versions
        )
        SELECT
            sha2(concat_ws('|', customer_id, CAST(valid_from AS STRING)), 256) AS customer_sk,
            customer_id, name, email, city, region, valid_from,
            COALESCE(DATE_SUB(next_from, 1), DATE'9999-12-31') AS valid_to,
            next_from IS NULL AS is_current
        FROM ordered
    """)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    transform_customers_scd2(spark)

    t = spark.table(SILVER)
    print("versions:", t.count())
    print("current versions:", t.filter("is_current").count())
    print("distinct customers:", t.select("customer_id").distinct().count())