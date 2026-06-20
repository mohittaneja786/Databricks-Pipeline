
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="project_tp.silver.city",
    comment = "cleaned and standardized products dimension with business transformations",
    table_properties={
        "quality" : "silver",
        "layer" : "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)


def city_silver():
    df_bronze = spark.read.table("project_tp.bronze.city")
    df_silver = df_bronze.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("ingest_datetime").alias("bronze_ingest_datetime")
    )

    df_silver = df_silver.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )

    return df_silver
