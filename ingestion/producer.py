#!/usr/bin/env python3
"""Producteur Kafka: rejoue les fichiers NYC TLC Yellow Taxi (Parquet) vers le topic taxi-trips.

Un evenement = une course de taxi (trip_id = hash du contenu, donc rejeu idempotent).
Exemples:
  python producer.py --months 2024-01 2024-02 2024-03 2024-04 --max-rows-per-month 150000 --rate 0
  python producer.py --months 2024-05 --max-rows-per-month 50000 --rate 200
"""
import argparse
import hashlib
import json
import os
import time

import pandas as pd
from kafka import KafkaProducer

COLS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
        "trip_distance", "RatecodeID", "PULocationID", "DOLocationID", "payment_type", "fare_amount"]
INT_COLS = ["VendorID", "passenger_count", "RatecodeID", "PULocationID", "DOLocationID", "payment_type"]
TS_FMT = "%Y-%m-%d %H:%M:%S"


def load_month(path, month, max_rows, seed):
    df = pd.read_parquet(path, columns=COLS).dropna(subset=COLS)
    # les fichiers TLC contiennent quelques dates hors du mois: on garde le mois demande
    df = df[df["tpep_pickup_datetime"].dt.strftime("%Y-%m") == month]
    if max_rows and len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=seed)
    df = df.sort_values("tpep_pickup_datetime").reset_index(drop=True)  # ordre chronologique
    for c in INT_COLS:
        df[c] = df[c].astype("int64")
    for c in ("trip_distance", "fare_amount"):
        df[c] = df[c].astype("float64")
    for c in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        df[c] = df[c].dt.strftime(TS_FMT)
    key = (df["tpep_pickup_datetime"] + "|" + df["tpep_dropoff_datetime"] + "|" + df["PULocationID"].astype(str)
           + "|" + df["DOLocationID"].astype(str) + "|" + df["fare_amount"].astype(str)
           + "|" + df["trip_distance"].astype(str))
    df.insert(0, "source_month", month)
    df.insert(0, "trip_id", key.map(lambda s: hashlib.md5(s.encode()).hexdigest()[:20]))
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="+", required=True, help="ex: 2024-01 2024-02")
    ap.add_argument("--data-dir", default="/data")
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic", default="taxi-trips")
    ap.add_argument("--max-rows-per-month", type=int, default=150000)
    ap.add_argument("--rate", type=float, default=0, help="evenements/seconde (0 = vitesse max)")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=a.bootstrap, acks="all", linger_ms=20, retries=5,
        key_serializer=lambda k: k.encode(), value_serializer=lambda v: json.dumps(v).encode())

    total, start = 0, time.time()
    for month in a.months:
        path = os.path.join(a.data_dir, f"yellow_tripdata_{month}.parquet")
        df = load_month(path, month, a.max_rows_per_month, a.seed)
        print(f"[{month}] {len(df)} evenements a publier", flush=True)
        for rec in df.to_dict("records"):
            producer.send(a.topic, key=rec["trip_id"], value=rec)
            total += 1
            if a.rate > 0 and total % 50 == 0:
                ahead = total / a.rate - (time.time() - start)
                if ahead > 0:
                    time.sleep(ahead)
            if total % 10000 == 0:
                print(f"  {total} envoyes ({total / (time.time() - start):.0f} evt/s)", flush=True)
    producer.flush()
    print(f"Termine: {total} evenements en {time.time() - start:.1f}s", flush=True)


if __name__ == "__main__":
    main()
