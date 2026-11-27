"""Controle des predictions ecrites par la voie chaude (reprise, doublons, erreur, delai)."""
from pyspark.sql import functions as F

from common.config import PRED_PATH
from common.spark_utils import get_spark

spark = get_spark("check_predictions")
spark.sparkContext.setLogLevel("WARN")
p = spark.read.parquet(PRED_PATH)
p.agg(F.count("*").alias("lignes"), F.countDistinct("trip_id").alias("ids_distincts"),
      F.round(F.avg("abs_error"), 3).alias("MAE_usd"),
      F.round(F.avg("latency_ms"), 0).alias("delai_moyen_ms"),
      F.expr("percentile_approx(latency_ms, 0.95)").alias("delai_p95_ms"),
      F.min("processed_at").alias("premier_traitement"), F.max("processed_at").alias("dernier_traitement")
      ).show(truncate=False)
p.orderBy(F.col("processed_at").desc()).select("trip_id", "trip_distance", "fare_amount", "prediction",
                                                "abs_error", "latency_ms").show(10, truncate=False)
spark.stop()
