"""Configuration commune (variables d'environnement avec valeurs par defaut)."""
import os

HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:9000")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "taxi-trips")

RAW_PATH = f"{HDFS_URL}/data/taxi/raw"                 # archive Parquet (voie froide)
CKPT_ARCHIVE = f"{HDFS_URL}/checkpoints/archive"       # checkpoint Kafka -> HDFS
CKPT_PREDICT = f"{HDFS_URL}/checkpoints/predict"       # checkpoint inference
MODEL_BASE = f"{HDFS_URL}/models/taxi_fare"

PRED_PATH = f"{HDFS_URL}/output/predictions"      # predictions (voie chaude)
KPI_BASE = f"{HDFS_URL}/output/kpi"                  # indicateurs batch
ML_BASE = f"{HDFS_URL}/output/ml"                    # resultats ML
