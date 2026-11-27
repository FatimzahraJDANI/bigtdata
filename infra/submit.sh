#!/usr/bin/env bash
# Soumet un job PySpark au cluster standalone (master + 2 workers).
# Usage: bash /app/infra/submit.sh /app/batch/train.py [args...]
# Variables optionnelles: DRIVER_MEMORY EXECUTOR_MEMORY CORES_MAX SHUFFLE_PARTITIONS
set -euo pipefail
JOB="$1"; shift
exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "$(basename "$JOB" .py)" \
  --driver-memory "${DRIVER_MEMORY:-1g}" \
  --executor-memory "${EXECUTOR_MEMORY:-768m}" \
  --conf spark.cores.max="${CORES_MAX:-2}" \
  --conf spark.executor.cores=1 \
  --conf spark.driver.host=spark-client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS:-8}" \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  --conf spark.hadoop.dfs.replication=2 \
  "$JOB" "$@"
