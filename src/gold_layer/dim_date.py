from databricks.connect import DatabricksSession

DIM_DATE = "sokoni_retail.gold.dim_date"


def build_dim_date(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DIM_DATE} AS
        WITH days AS (
            SELECT explode(sequence(DATE'2025-01-01', DATE'2026-11-30', INTERVAL 1 DAY)) AS full_date
        )
        SELECT
            CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
            full_date,
            YEAR(full_date) AS year,
            QUARTER(full_date) AS quarter,
            MONTH(full_date) AS month,
            date_format(full_date, 'MMMM') AS month_name,
            trunc(full_date, 'week') AS week_start,
            date_format(full_date, 'EEEE') AS day_name,
            DAY(full_date) AS day_of_month,
            dayofweek(full_date) IN (1, 7) AS is_weekend,
            DAY(full_date) >= 25 OR DAY(full_date) <= 3 AS is_payday_window
        FROM days
    """)


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")
    build_dim_date(spark)
    print("dim_date rows:", spark.table(DIM_DATE).count())