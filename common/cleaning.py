"""Parsing des evenements Kafka et regles de qualite (partagees batch + streaming)."""
from pyspark.sql import functions as F

from common.schema import EVENT_SCHEMA


def parse_events(raw):
    """DataFrame Kafka (key, value, timestamp, ...) -> colonnes de l'evenement + pickup_ts + kafka_ts."""
    ev = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
                    F.col("timestamp").alias("kafka_ts")).select("e.*", "kafka_ts")
    return (ev.withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
              .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
              .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
              .filter(F.col("trip_id").isNotNull() & F.col("pickup_ts").isNotNull()))


def quality_rules():
    """Nom de la regle -> condition de VALIDITE (une valeur nulle est consideree invalide)."""
    c = F.col
    rules = {
        "fare_amount_hors_[3,200]": c("fare_amount").between(3.0, 200.0),
        "trip_distance_hors_[0.1,100]": c("trip_distance").between(0.1, 100.0),
        "passenger_count_hors_[1,6]": c("passenger_count").between(1, 6),
        "ratecode_hors_[1,6]": c("RatecodeID").between(1, 6),
        "zone_depart_invalide": c("PULocationID").between(1, 265),
        "zone_arrivee_invalide": c("DOLocationID").between(1, 265),
        "duree_hors_[1min,3h]": (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")).between(60, 10800),
        "date_hors_mois_source": c("pickup_month") == c("source_month"),
    }
    return {k: F.coalesce(v, F.lit(False)) for k, v in rules.items()}


def clean(df):
    valid = F.lit(True)
    for cond in quality_rules().values():
        valid = valid & cond
    return df.filter(valid)
