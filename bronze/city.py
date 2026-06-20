from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, sha2

source_path = "/Volumes/project_tp/trips/project_tp_data/City"


@dp.materialized_view(
    name = "project_tp.bronze.city",
    comment = "City raw data processing",
    table_properties = {
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("mode", "PERMISSIVE").option("mergeSchema", "true").option("columnNameofCorruptRecord", "_corrupt_record").load(source_path)

    df = df.withColumn("file_name", col("_metadata.file_path")).withColumn("ingest_datetime", current_timestamp())

    return df
