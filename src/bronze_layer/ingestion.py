from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "sokoni_retail"
ROOT = "abfss://sokoni@sokoniadls.dfs.core.windows.net"

SOURCE = f"{ROOT}/bronze_layer/sales/"
CHECKPOINT = f"{ROOT}/_checkpoints/sales/"
SCHEMA_LOC = f"{ROOT}/_schemas/sales/"
TARGET = f"{CATALOG}.bronze.sales_ingested"


def ingest_sales(spark):
    raw = (spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "csv")
           .option("header", "true")
           .option("cloudFiles.inferColumnTypes", "true")
           .option("cloudFiles.schemaLocation", SCHEMA_LOC)
           .load(SOURCE)
           .withColumn("_ingested_at", F.current_timestamp())
           .withColumn("_source_file", F.col("_metadata.file_path")))

    query = (raw.writeStream.format("delta")
             .option("checkpointLocation", CHECKPOINT)
             .trigger(availableNow=True)
             .toTable(TARGET))

    query.awaitTermination()


if __name__ == "__main__":
    import os
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("USE CATALOG sokoni_retail")

    ingest_sales(spark)
    print("sales_ingested rows:",
          spark.table("sokoni_retail.bronze.sales_ingested").count())