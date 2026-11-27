#!/usr/bin/env bash
# Telecharge les fichiers NYC TLC Yellow Taxi (Parquet) dans ./data
# Historique (voie froide): 2024-01..2024-04   |   Rejeu streaming (voie chaude): 2024-05
set -euo pipefail
cd "$(dirname "$0")/../data"
BASE="https://d37ci6vzurychx.cloudfront.net/trip-data"
for m in 2024-01 2024-02 2024-03 2024-04 2024-05; do
  f="yellow_tripdata_${m}.parquet"
  [ -f "$f" ] && { echo "deja present: $f"; continue; }
  echo "telechargement $f"
  curl -fL -o "$f" "$BASE/$f"
done
ls -lh
