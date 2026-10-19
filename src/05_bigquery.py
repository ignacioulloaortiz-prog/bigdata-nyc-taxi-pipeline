"""Etapa 5 — Carga al Data Warehouse (BigQuery Sandbox) + BigQuery ML.

Requisitos:
  * Proyecto GCP (Sandbox, sin tarjeta) y variable de entorno GCP_PROJECT_ID.
  * Autenticación: en Colab `google.colab.auth.authenticate_user()`; fuera de Colab
    `gcloud auth application-default login` o una Service Account con los roles
    roles/bigquery.dataEditor (dataset) + roles/bigquery.jobUser (proyecto).
Uso:  GCP_PROJECT_ID=mi-proyecto python src/05_bigquery.py
"""
import os
import sys

from google.cloud import bigquery

from common import LAKE, SILVER, get_spark

COLS = ["pickup_date", "tpep_pickup_datetime", "pickup_hour", "pickup_dow", "is_weekend", "trip_distance",
        "trip_duration_min", "passenger_count", "RatecodeID", "payment_type", "fare_amount", "tip_amount",
        "total_amount", "tip_pct", "pu_borough", "do_borough", "pu_zone", "do_zone", "is_airport", "month"]

if __name__ == "__main__":
    project = os.environ.get("GCP_PROJECT_ID")
    if not project:
        sys.exit("Defina GCP_PROJECT_ID")
    if "google.colab" in sys.modules:
        from google.colab import auth
        auth.authenticate_user()
    bq = bigquery.Client(project=project)
    ds = f"{project}.nyc_taxi"
    bq.create_dataset(bigquery.Dataset(ds), exists_ok=True)

    spark = get_spark("bq-export")
    stage = LAKE / "bq_stage" / "trips"
    spark.read.parquet(str(SILVER / "yellow_trips")).select(COLS).coalesce(4).write.mode("overwrite").parquet(str(stage))

    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        # Rango entero sobre `month`: en el Sandbox las particiones por fecha expiran a los 60 días
        range_partitioning=bigquery.RangePartitioning(field="month",
                                                      range_=bigquery.PartitionRange(start=1, end=13, interval=1)),
        clustering_fields=["pickup_date", "pu_borough", "do_borough"])
    for i, f in enumerate(sorted(stage.glob("*.parquet"))):
        cfg.write_disposition = "WRITE_TRUNCATE" if i == 0 else "WRITE_APPEND"
        with open(f, "rb") as fh:
            bq.load_table_from_file(fh, f"{ds}.trips", job_config=cfg).result()
    print("Filas cargadas:", bq.get_table(f"{ds}.trips").num_rows)

    bq.query(f"""
    CREATE OR REPLACE MODEL `{ds}.tip_model`
    OPTIONS(model_type='logistic_reg', input_label_cols=['good_tip'],
            data_split_method='CUSTOM', data_split_col='is_eval', auto_class_weights=TRUE) AS
    SELECT IF(tip_pct >= 0.2, 1, 0) AS good_tip, trip_distance, pickup_hour, pickup_dow, is_airport,
           pu_borough, do_borough, CAST(passenger_count AS STRING) AS passengers, month = 2 AS is_eval
    FROM `{ds}.trips` WHERE payment_type = 1 AND month IN (1, 2)""").result()
    print(bq.query(f"SELECT * FROM ML.EVALUATE(MODEL `{ds}.tip_model`)").to_dataframe().T)
