from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(
    name="trips_silver_staging", comment = "Tramsformed trips ready for GDC upsert"
)

@dp.expect("valid_date", "year(business_date) >= 2020")
@dp.expect("valid_driver_rating", "driver_rating Between 1 And 100")
@dp.expect("valid_passenger_rating", "passenger_rating Between 1 And 10")


def trips_silver():
    df_bronze = spark.readStream.table("project_tp.bronze.trips")
    
    df_silver = df_bronze.select(
        F.col("trip_id").alias("id"),
        F.col("date").cast("date").alias("business_date"),
        F.col("city_id").alias("city_id"),
        F.col("passenger_type").alias("passenger_category"),
        F.col("distance_travelled_km").alias("distance_kms"),
        F.col("fare_amount").alias("sales_amt"),
        F.col("passenger_rating").alias("passenger_rating"),
        F.col("driver_rating").alias("driver_rating"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp"),
    )

    df_silver = df_silver.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )

    return df_silver

dp.create_streaming_table(
    name="project_tp.silver.trips",
    comment = "cleaned and validated orders with CDC Upsert capability",
    table_properties={
        "quality" : "silver",
        "layer" : "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

dp.create_auto_cdc_flow(
    target="project_tp.silver.trips",
    source="trips_silver_staging",
    keys = ["id"],
    sequence_by = F.col("silver_processed_timestamp"),
    stored_as_scd_type = 1,
    except_column_list = []
)








