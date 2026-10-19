"""Etapa 2 — ETL distribuido con PySpark: RAW → BRONZE → SILVER → GOLD.

* BRONZE: esquema explícito, Parquet + Snappy particionado por year/month.
* SILVER: 5 reglas de calidad, enriquecimiento con zonas (broadcast join) y variables derivadas.
* GOLD:   agregados de negocio para BI.
Uso:  python src/02_etl.py
"""
import json

from pyspark.sql import functions as F, types as T

from common import BRONZE, GOLD, OUT, RAW, SILVER, get_spark

SCHEMA = T.StructType([
    T.StructField("VendorID", T.IntegerType()),
    T.StructField("tpep_pickup_datetime", T.TimestampType()),
    T.StructField("tpep_dropoff_datetime", T.TimestampType()),
    T.StructField("passenger_count", T.IntegerType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("RatecodeID", T.IntegerType()),
    T.StructField("store_and_fwd_flag", T.StringType()),
    T.StructField("PULocationID", T.IntegerType()),
    T.StructField("DOLocationID", T.IntegerType()),
    T.StructField("payment_type", T.IntegerType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("extra", T.DoubleType()),
    T.StructField("mta_tax", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()),
    T.StructField("tolls_amount", T.DoubleType()),
    T.StructField("improvement_surcharge", T.DoubleType()),
    T.StructField("total_amount", T.DoubleType()),
    T.StructField("congestion_surcharge", T.DoubleType()),
])


def to_bronze(spark):
    df = (spark.read.csv(str(RAW / "yellow_tripdata_*.csv.gz"), header=True, schema=SCHEMA)
          .withColumn("year", F.year("tpep_pickup_datetime"))
          .withColumn("month", F.month("tpep_pickup_datetime"))
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn("source_file", F.input_file_name()))
    (df.repartition("year", "month").write.mode("overwrite")
       .partitionBy("year", "month").parquet(str(BRONZE / "yellow_trips")))
    return spark.read.parquet(str(BRONZE / "yellow_trips"))


def to_silver(spark, bronze):
    zones = (spark.read.csv(str(RAW / "taxi_zone_lookup.csv"), header=True, inferSchema=True)
             .select(F.col("LocationID").cast("int"), "Borough", "Zone", "service_zone"))
    pu = zones.select(F.col("LocationID").alias("PULocationID"), F.col("Borough").alias("pu_borough"),
                      F.col("Zone").alias("pu_zone"), F.col("service_zone").alias("pu_service_zone"))
    do = zones.select(F.col("LocationID").alias("DOLocationID"), F.col("Borough").alias("do_borough"),
                      F.col("Zone").alias("do_zone"), F.col("service_zone").alias("do_service_zone"))
    df = (bronze
          .withColumn("trip_duration_min",
                      (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
          .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
          .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
          .withColumn("pickup_dow", F.dayofweek("tpep_pickup_datetime"))
          .withColumn("is_weekend", F.col("pickup_dow").isin(1, 7).cast("int")))
    rules = {
        "fecha_fuera_de_periodo": ~F.col("pickup_date").between("2021-01-01", "2021-03-31"),
        "distancia_invalida": ~F.col("trip_distance").between(0.1, 100),
        "tarifa_invalida": ~F.col("fare_amount").between(2.5, 500),
        "duracion_invalida": ~F.col("trip_duration_min").between(1, 180),
        "zona_desconocida": F.col("PULocationID").isin(264, 265) | F.col("DOLocationID").isin(264, 265),
    }
    dq = df.select([F.sum(F.coalesce(c.cast("int"), F.lit(0))).alias(k) for k, c in rules.items()]).first().asDict()
    valid = None
    for c in rules.values():
        valid = ~c if valid is None else valid & ~c
    silver = (df.filter(valid)
              .join(F.broadcast(pu), "PULocationID", "left")
              .join(F.broadcast(do), "DOLocationID", "left")
              .withColumn("avg_speed_mph", F.col("trip_distance") / (F.col("trip_duration_min") / 60))
              .filter(F.col("avg_speed_mph") < 80)
              .withColumn("tip_pct", F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount")))
              .withColumn("is_airport", F.coalesce(((F.col("pu_service_zone") == "Airports")
                                                    | (F.col("do_service_zone") == "Airports")
                                                    | F.col("RatecodeID").isin(2, 3)).cast("int"), F.lit(0)))
              .withColumn("same_borough", F.coalesce((F.col("pu_borough") == F.col("do_borough")).cast("int"), F.lit(0)))
              .drop("ingest_ts", "source_file"))
    (silver.repartition("year", "month").write.mode("overwrite")
           .partitionBy("year", "month").parquet(str(SILVER / "yellow_trips")))
    return spark.read.parquet(str(SILVER / "yellow_trips")), dq


def to_gold(silver):
    (silver.groupBy("pickup_date", "pu_borough")
           .agg(F.count("*").alias("trips"), F.round(F.sum("total_amount"), 2).alias("revenue"),
                F.round(F.avg("trip_distance"), 3).alias("avg_distance"),
                F.round(F.avg("trip_duration_min"), 3).alias("avg_duration"),
                F.round(F.avg("tip_pct"), 4).alias("avg_tip_pct"))
           .write.mode("overwrite").parquet(str(GOLD / "daily_borough")))
    (silver.groupBy("pickup_dow", "pickup_hour")
           .agg(F.count("*").alias("trips"), F.avg("avg_speed_mph").alias("avg_speed_mph"))
           .write.mode("overwrite").parquet(str(GOLD / "hourly_profile")))


if __name__ == "__main__":
    spark = get_spark("etl")
    bronze = to_bronze(spark)
    n_raw = bronze.count()
    silver, dq = to_silver(spark, bronze)
    n_silver = silver.count()
    to_gold(silver)
    report = {"rows_raw": n_raw, "rows_silver": n_silver, "data_quality": dq}
    (OUT / "etl_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
