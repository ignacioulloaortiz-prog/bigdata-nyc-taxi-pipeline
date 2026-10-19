"""Etapa 4 — Modelo predictivo distribuido (pyspark.ml): ETA = duración del viaje en minutos.

Validación temporal: entrena con enero–febrero y evalúa con marzo completo.
Pipeline: Imputer → StringIndexer → OneHotEncoder → VectorAssembler → GBTRegressor (hiperparámetros por CV 3-fold).
Uso:  python src/04_modelo_ml.py      (TRAIN_FRACTION=0.25 por defecto)
"""
import json
import os
import time

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import functions as F

from common import MODELS, OUT, SEED, SILVER, get_spark

LABEL = "trip_duration_min"
CATS = ["ratecode", "pu_borough", "do_borough", "PU", "DO"]
NUM = ["trip_distance", "passenger_imp", "pickup_hour", "pickup_dow", "is_weekend", "is_airport", "same_borough"]


def features(silver):
    return (silver
            .withColumn("passenger_count_d", F.col("passenger_count").cast("double"))
            .withColumn("ratecode", F.coalesce(F.col("RatecodeID"), F.lit(99)).cast("string"))
            .withColumn("pu_borough", F.coalesce("pu_borough", F.lit("Unknown")))
            .withColumn("do_borough", F.coalesce("do_borough", F.lit("Unknown")))
            .withColumn("PU", F.col("PULocationID").cast("string"))
            .withColumn("DO", F.col("DOLocationID").cast("string"))
            .select(LABEL, "month", "trip_distance", "passenger_count_d", "pickup_hour", "pickup_dow",
                    "is_weekend", "is_airport", "same_borough", *CATS))


def preprocessing():
    return ([Imputer(inputCols=["passenger_count_d"], outputCols=["passenger_imp"], strategy="median")]
            + [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in CATS]
            + [OneHotEncoder(inputCols=[c + "_idx" for c in CATS], outputCols=[c + "_ohe" for c in CATS],
                             handleInvalid="keep"),
               VectorAssembler(inputCols=NUM + [c + "_ohe" for c in CATS], outputCol="features")])


if __name__ == "__main__":
    spark = get_spark("ml")
    data = features(spark.read.parquet(str(SILVER / "yellow_trips")))
    train = data.filter("month IN (1, 2)").sample(float(os.environ.get("TRAIN_FRACTION", "0.25")), seed=SEED).cache()
    test = data.filter("month = 3")
    evs = {m: RegressionEvaluator(labelCol=LABEL, metricName=m) for m in ["rmse", "mae", "r2"]}
    results = {}

    lr = Pipeline(stages=preprocessing() + [LinearRegression(labelCol=LABEL, regParam=0.01)]).fit(train)
    pred = lr.transform(test)
    results["linear_regression"] = {m: e.evaluate(pred) for m, e in evs.items()}

    prep = Pipeline(stages=preprocessing()).fit(train)
    cv_df = prep.transform(train.sample(0.2, seed=SEED)).select("features", LABEL).cache()
    gbt = GBTRegressor(labelCol=LABEL, seed=SEED, maxBins=64, subsamplingRate=0.8)
    grid = ParamGridBuilder().addGrid(gbt.maxDepth, [4, 6]).addGrid(gbt.maxIter, [20, 40]).build()
    cvm = CrossValidator(estimator=gbt, estimatorParamMaps=grid, evaluator=evs["rmse"],
                         numFolds=3, seed=SEED, parallelism=2).fit(cv_df)
    bp = grid[min(range(len(grid)), key=lambda i: cvm.avgMetrics[i])]
    best = {"maxDepth": bp[gbt.maxDepth], "maxIter": bp[gbt.maxIter]}

    t0 = time.time()
    model = Pipeline(stages=preprocessing() + [GBTRegressor(labelCol=LABEL, seed=SEED, maxBins=64,
                                                            subsamplingRate=0.8, **best)]).fit(train)
    pred = model.transform(test)
    results["gbt"] = {m: e.evaluate(pred) for m, e in evs.items()} | {"best_params": best,
                                                                       "train_s": round(time.time() - t0, 1)}
    model.write().overwrite().save(str(MODELS / f"gbt_eta_{time.strftime('%Y%m%d')}"))
    (OUT / "ml_report.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))
