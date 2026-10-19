"""Configuración compartida del pipeline: rutas del Data Lake y sesión Spark.

En Google Colab el Data Lake vive en Google Drive (almacenamiento replicado por Google);
fuera de Colab usa ./lake o la variable de entorno LAKE_PATH.
"""
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

IN_COLAB = "google.colab" in sys.modules or os.path.exists("/content")
ROOT = Path(__file__).resolve().parents[1]

if IN_COLAB:
    LAKE = Path(os.environ.get("LAKE_PATH", "/content/drive/MyDrive/bigdata_lake"))
    OUT = Path(os.environ.get("RESULTS_PATH", "/content/drive/MyDrive/bigdata_results"))
else:
    LAKE = Path(os.environ.get("LAKE_PATH", ROOT / "lake"))
    OUT = Path(os.environ.get("RESULTS_PATH", ROOT / "results"))

RAW, BRONZE, SILVER, GOLD, MODELS = (LAKE / z for z in ["raw", "bronze", "silver", "gold", "models"])
FIG = OUT / "figures"
for _p in [RAW, BRONZE, SILVER, GOLD, MODELS, FIG]:
    _p.mkdir(parents=True, exist_ok=True)

MONTHS = ["2021-01", "2021-02", "2021-03"]
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
SEED = 42


def get_spark(app: str = "nyc-taxi-bigdata-pipeline") -> SparkSession:
    """Crea (o reutiliza) la sesión Spark con las optimizaciones del proyecto."""
    spark = (SparkSession.builder
             .appName(app)
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "8g"))
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark
