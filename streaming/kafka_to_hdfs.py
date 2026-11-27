"""VOIE FROIDE: Kafka -> HDFS (Parquet partitionne par mois), avec checkpoint.

  --once  : consomme tout le backlog Kafka puis s'arrete (construction de l'historique)
  sinon   : tourne en continu (micro-batch toutes les 10 s)
Le checkpoint (HDFS) memorise les offsets: une relance reprend la ou le job s'est arrete.
"""
import argparse

from common.cleaning import parse_events
from common.config import CKPT_ARCHIVE, KAFKA_BOOTSTRAP, KAFKA_TOPIC, RAW_PATH
from common.spark_utils import get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--starting-offsets", default="earliest")
    ap.add_argument("--max-offsets-per-trigger", type=int, default=50000)
    a = ap.parse_args()

    spark = get_spark("archive_kafka_to_hdfs")
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", a.starting_offsets)
           .option("maxOffsetsPerTrigger", a.max_offsets_per_trigger)
           .option("failOnDataLoss", "false")
           .load())

    events = parse_events(raw)   # on archive le flux "utile" (JSON valide + id + horodatage), le nettoyage metier est fait en lecture

    writer = (events.writeStream.format("parquet")
              .option("path", RAW_PATH)
              .option("checkpointLocation", CKPT_ARCHIVE)
              .partitionBy("pickup_month")
              .outputMode("append"))
    writer = writer.trigger(availableNow=True) if a.once else writer.trigger(processingTime="10 seconds")

    q = writer.start()
    q.awaitTermination()


if __name__ == "__main__":
    main()
