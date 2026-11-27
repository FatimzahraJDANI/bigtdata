"""ENTRAINEMENT Spark ML (regression du prix de la course, fare_amount en USD).

Scenario de prediction: au moment de la commande, on connait l'heure de prise en charge, les zones de depart
et d'arrivee, le nombre de passagers, le tarif (RatecodeID) et la distance estimee de l'itineraire.
On n'utilise ni pourboire, ni peages, ni total, ni heure d'arrivee (non disponibles au moment de la prediction).

Decoupage TEMPOREL (jamais aleatoire):  train = mois les plus anciens | validation = 1 mois | test = dernier mois.
1. Reference simple: moyenne, et tarif moyen au mile x distance.
2. Trois techniques comparees (parametres par defaut): Regression lineaire, Random Forest, GBT.
3. Tuning de la meilleure (RMSE validation) par recherche sur grille, validation temporelle.
4. Refit sur train+val, evaluation finale sur test, sauvegarde du PipelineModel versionne dans HDFS.
"""
import argparse
import json
import os
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, SQLTransformer, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql import functions as F

from common.cleaning import clean
from common.config import ML_BASE, MODEL_BASE, RAW_PATH
from common.spark_utils import get_spark

LABEL = "fare_amount"
COLS = ["trip_id", "pickup_ts", "pickup_month", "trip_distance", "passenger_count",
        "RatecodeID", "PULocationID", "DOLocationID", "fare_amount"]


def build_pipeline(estimator):
    """Toute la preparation est DANS le modele: le job streaming n'a qu'a appeler transform()."""
    sql = SQLTransformer(statement="""
        SELECT *, hour(pickup_ts) AS pickup_hour, dayofweek(pickup_ts) AS pickup_dow,
               CASE WHEN dayofweek(pickup_ts) IN (1,7) THEN 1.0 ELSE 0.0 END AS is_weekend,
               CAST(RatecodeID AS STRING) AS rate_s, CAST(PULocationID AS STRING) AS pu_s,
               CAST(DOLocationID AS STRING) AS do_s
        FROM __THIS__""")
    # handleInvalid="keep": une zone/un tarif inconnu a l'inference ne fait pas planter le job
    idx = StringIndexer(inputCols=["rate_s", "pu_s", "do_s"], outputCols=["rate_i", "pu_i", "do_i"],
                        handleInvalid="keep")
    ohe = OneHotEncoder(inputCols=["rate_i", "pu_i", "do_i"], outputCols=["rate_v", "pu_v", "do_v"],
                        handleInvalid="keep")
    asm = VectorAssembler(inputCols=["trip_distance", "passenger_count", "pickup_hour", "pickup_dow",
                                     "is_weekend", "rate_v", "pu_v", "do_v"], outputCol="features")
    return Pipeline(stages=[sql, idx, ohe, asm, estimator])


def default_estimators():
    kw = dict(featuresCol="features", labelCol=LABEL, predictionCol="prediction")
    return {
        "LinearRegression": LinearRegression(maxIter=50, **kw),
        "RandomForest": RandomForestRegressor(numTrees=20, maxDepth=8, seed=42, **kw),
        "GBT": GBTRegressor(maxIter=20, maxDepth=5, seed=42, **kw),
    }


def param_grid(name, est):
    g = ParamGridBuilder()
    if name == "LinearRegression":
        g.addGrid(est.regParam, [0.0, 0.01, 0.1]).addGrid(est.elasticNetParam, [0.0, 0.5])
    elif name == "RandomForest":
        g.addGrid(est.numTrees, [20, 40]).addGrid(est.maxDepth, [6, 10])
    else:
        g.addGrid(est.maxIter, [20, 40]).addGrid(est.maxDepth, [4, 6])
    return g.build()


def metrics(df, pred="prediction"):
    return {m: RegressionEvaluator(labelCol=LABEL, predictionCol=pred, metricName=m).evaluate(df)
            for m in ("rmse", "mae", "r2")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train-months", default="2024-01,2024-02")
    ap.add_argument("--val-month", default="2024-03")
    ap.add_argument("--test-month", default="2024-04")
    ap.add_argument("--version", default="v" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M"))
    a = ap.parse_args()

    spark = get_spark("train_fare_model")
    spark.sparkContext.setLogLevel("WARN")

    data = clean(spark.read.parquet(RAW_PATH).dropDuplicates(["trip_id"])).select(*COLS)
    train = data.filter(F.col("pickup_month").isin(a.train_months.split(","))).cache()
    val = data.filter(F.col("pickup_month") == a.val_month).cache()
    test = data.filter(F.col("pickup_month") == a.test_month).cache()
    n_tr, n_va, n_te = train.count(), val.count(), test.count()
    print(f"train={n_tr} val={n_va} test={n_te}")
    assert min(n_tr, n_va, n_te) > 0, "Un des decoupages est vide: verifier les mois archives dans HDFS"

    rows = []   # (stage, model, split, rmse, mae, r2, params)

    def add(stage, model, split, m, params=""):
        rows.append((stage, model, split, float(m["rmse"]), float(m["mae"]), float(m["r2"]), params))
        print(f"{stage:12s} {model:18s} {split:5s} RMSE={m['rmse']:.3f} MAE={m['mae']:.3f} R2={m['r2']:.3f} {params}", flush=True)

    # ---- 1. references simples (apprises sur train uniquement) ----
    mean_fare = train.agg(F.avg(LABEL)).first()[0]
    per_mile = train.agg(F.sum(LABEL) / F.sum("trip_distance")).first()[0]
    for split, df in (("val", val), ("test", test)):
        add("baseline", "Moyenne", split, metrics(df.withColumn("prediction", F.lit(mean_fare))), f"mean={mean_fare:.2f}")
        add("baseline", "Tarif_au_mile", split, metrics(df.withColumn("prediction", F.col("trip_distance") * per_mile)),
            f"usd_per_mile={per_mile:.2f}")

    # ---- 2. comparaison de 3 techniques (defaut), fit sur train ----
    ests = default_estimators()
    val_rmse = {}
    for name, est in ests.items():
        model = build_pipeline(est).fit(train)
        mv, mt = metrics(model.transform(val)), metrics(model.transform(test))
        add("comparaison", name, "val", mv)
        add("comparaison", name, "test", mt)
        val_rmse[name] = mv["rmse"]
    best = min(val_rmse, key=val_rmse.get)
    print(f">>> Technique retenue pour le tuning (RMSE validation minimale): {best}")

    # ---- 3. tuning par grille, evalue sur la validation temporelle ----
    base_est = ests[best]
    best_pm, best_rmse = None, float("inf")
    for pm in param_grid(best, base_est):
        desc = json.dumps({p.name: v for p, v in pm.items()})
        m = metrics(build_pipeline(base_est.copy(pm)).fit(train).transform(val))
        add("tuning", best, "val", m, desc)
        if m["rmse"] < best_rmse:
            best_rmse, best_pm = m["rmse"], pm
    best_desc = json.dumps({p.name: v for p, v in best_pm.items()})
    print(f">>> Meilleurs hyperparametres: {best_desc}")

    # ---- 4. modele final: refit train+val, evaluation sur test, sauvegarde ----
    final = build_pipeline(base_est.copy(best_pm)).fit(train.union(val))
    scored = final.transform(test).cache()
    add("final", best + "_tuned", "test", metrics(scored), best_desc)

    err = (scored.withColumn("bucket", F.when(F.col("trip_distance") < 1, "0-1").when(F.col("trip_distance") < 3, "1-3")
                             .when(F.col("trip_distance") < 6, "3-6").when(F.col("trip_distance") < 10, "6-10")
                             .when(F.col("trip_distance") < 20, "10-20").otherwise("20+"))
           .groupBy("bucket").agg(F.count("*").alias("n"), F.round(F.avg(F.abs(F.col("prediction") - F.col(LABEL))), 3).alias("mae"),
                                  F.round(F.avg(F.col("prediction") - F.col(LABEL)), 3).alias("mean_signed_error"),
                                  F.round(F.avg(LABEL), 2).alias("avg_fare")))
    err.coalesce(1).write.mode("overwrite").parquet(f"{ML_BASE}/error_by_distance")
    err.show(truncate=False)

    path = f"{MODEL_BASE}/{a.version}"
    final.write().overwrite().save(path)
    final.write().overwrite().save(f"{MODEL_BASE}/latest")
    print(f">>> Modele sauvegarde: {path}  (et {MODEL_BASE}/latest)")

    res = spark.createDataFrame(rows, ["stage", "model", "split", "rmse", "mae", "r2", "params"]) \
               .withColumn("model_version", F.lit(a.version))
    res.coalesce(1).write.mode("overwrite").parquet(f"{ML_BASE}/results")
    res.show(100, truncate=False)
    os.makedirs("/app/docs/results", exist_ok=True)
    with open(f"/app/docs/results/ml_results_{a.version}.json", "w") as f:
        json.dump({"version": a.version, "n_train": n_tr, "n_val": n_va, "n_test": n_te, "best_model": best,
                   "best_params": json.loads(best_desc),
                   "rows": [dict(zip(["stage", "model", "split", "rmse", "mae", "r2", "params"], r)) for r in rows]},
                  f, indent=2)
    spark.stop()


if __name__ == "__main__":
    main()
