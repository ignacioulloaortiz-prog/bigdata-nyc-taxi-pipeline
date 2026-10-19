"""Etapa 3 — Análisis descriptivo distribuido con Spark SQL sobre la zona SILVER.
Uso:  python src/03_eda.py
"""
from common import OUT, SILVER, get_spark

QUERIES = {
    "resumen": """
        SELECT COUNT(*) AS viajes, ROUND(AVG(trip_distance),2) AS dist_media_mi,
               ROUND(PERCENTILE_APPROX(trip_duration_min,0.5),2) AS dur_mediana_min,
               ROUND(AVG(trip_duration_min),2) AS dur_media_min, ROUND(STDDEV(trip_duration_min),2) AS dur_desv_std,
               ROUND(AVG(fare_amount),2) AS tarifa_media_usd, ROUND(SUM(total_amount)/1e6,2) AS ingresos_musd,
               ROUND(CORR(trip_distance, trip_duration_min),3) AS corr_dist_dur
        FROM trips""",
    "mensual": """
        SELECT month, COUNT(*) AS viajes, ROUND(SUM(total_amount)/1e6,2) AS ingresos_musd,
               ROUND(AVG(trip_duration_min),2) AS dur_media_min, ROUND(AVG(tip_pct)*100,2) AS propina_pct
        FROM trips GROUP BY month ORDER BY month""",
    "boroughs": """
        SELECT pu_borough AS borough, COUNT(*) AS viajes,
               ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER (),2) AS pct_viajes,
               ROUND(AVG(trip_distance),2) AS dist_media, ROUND(AVG(fare_amount),2) AS tarifa_media
        FROM trips GROUP BY pu_borough ORDER BY viajes DESC""",
    "top_rutas": """
        SELECT pu_zone, do_zone, COUNT(*) AS viajes, ROUND(AVG(trip_duration_min),1) AS dur_media
        FROM trips WHERE pu_zone <> do_zone GROUP BY pu_zone, do_zone ORDER BY viajes DESC LIMIT 10""",
}

if __name__ == "__main__":
    spark = get_spark("eda")
    spark.read.parquet(str(SILVER / "yellow_trips")).createOrReplaceTempView("trips")
    for name, q in QUERIES.items():
        pdf = spark.sql(q).toPandas()
        pdf.to_csv(OUT / f"eda_{name}.csv", index=False)
        print(f"\n== {name} ==\n{pdf.to_string(index=False)}")
