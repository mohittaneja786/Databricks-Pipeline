from pyspark import pipelines as dp
import pyspark.sql.functions as F

source_path = "/Volumes/project_tp/trips/project_tp_data/Full Load"

@dp.table(
    name = "project_tp.bronze.trips",
    comment = "Streaming ingestion pf raw orders data with Auto Loader",
    table_properties = {
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

def orders_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(source_path)
    )

    #rename the problematic column
    df = df.withColumnRenamed(
        "distance_travelled(km)",
        "distance_travelled_km"
        )

    df = df.withColumn("file_name", F.col("_metadata.file_path")).withColumn("ingest_datetime", F.current_timestamp())

    return df




