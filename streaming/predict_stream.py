"""VOIE CHAUDE: Kafka -> modele Spark ML (charge au demarrage) -> predictions Parquet dans HDFS.

- Le PipelineModel sauvegarde contient TOUTE la preparation (SQLTransformer, StringIndexer,
  OneHotEncoder, VectorAssembler) + le regresseur: ici on appelle seulement model.transform().
- Sink Parquet + checkpoint HDFS: reprise apres panne sans doublon (sink fichier exactly-once).
- latency_ms = instant de traitement - horodatage du message Kafka (delai de bout en bout).
- --console : affiche aussi les predictions dans le terminal (utile pour la demonstration).
"""
import argparse
import os

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from common.cleaning import clean, parse_events
from common.config import CKPT_PREDICT, KAFKA_BOOTSTRAP, KAFKA_TOPIC, MODEL_BASE, PRED_PATH
from common.spark_utils import get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-path", default=f"{MODEL_BASE}/latest")
    ap.add_argument("--starting-offsets", default="latest", help="latest = uniquement les nouveaux evenements")
    ap.add_argument("--max-offsets-per-trigger", type=int, default=5000)
    ap.add_argument("--console", action="store_true")
    a = ap.parse_args()

    spark = get_spark("predict_stream")
    spark.sparkContext.setLogLevel("WARN")

    model = PipelineModel.load(a.model_path)             # chargement du modele au demarrage
    version = os.path.basename(a.model_path.rstrip("/"))
    print(f"Modele charge: {a.model_path}", flush=True)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", a.starting_offsets)
           .option("maxOffsetsPerTrigger", a.max_offsets_per_trigger)
           .option("failOnDataLoss", "false")
           .load())

    scored = (model.transform(clean(parse_events(raw)))
              .select("trip_id", "pickup_ts", "PULocationID", "DOLocationID", "passenger_count", "RatecodeID",
                      "trip_distance", "fare_amount",
                      F.round("prediction", 2).alias("prediction"),
                      F.round(F.abs(F.col("prediction") - F.col("fare_amount")), 2).alias("abs_error"),
                      "kafka_ts",
                      F.current_timestamp().alias("processed_at"),
                      F.expr("unix_millis(current_timestamp()) - unix_millis(kafka_ts)").alias("latency_ms"),
                      F.lit(version).alias("model_version")))

    (scored.writeStream.format("parquet")
     .option("path", PRED_PATH).option("checkpointLocation", CKPT_PREDICT)
     .outputMode("append").trigger(processingTime="5 seconds").start())

    if a.console:
        (scored.select("trip_id", "trip_distance", "fare_amount", "prediction", "abs_error", "latency_ms")
         .writeStream.format("console").option("numRows", 10).option("truncate", "false")
         .outputMode("append").trigger(processingTime="5 seconds").start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
