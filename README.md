# Prédiction en temps réel du prix des courses de taxi à New York
Pipeline Big Data : **Kafka → Spark Structured Streaming → HDFS → Spark SQL / Spark ML → Kafka + modèle → prédictions (HDFS)**.
Données : NYC TLC Trip Record Data, *Yellow Taxi* (open data NYC). Spark en **PySpark 3.5.3**.

## Les quatre couches
| Couche | Fichier | Rôle |
|---|---|---|
| Ingestion | `ingestion/producer.py` | Rejoue les fichiers Parquet TLC dans le topic Kafka `taxi-trips` |
| Stockage (voie froide) | `streaming/kafka_to_hdfs.py` | Structured Streaming : Kafka → HDFS (Parquet partitionné par mois) ; puis `batch/analytics.py` (indicateurs) et `batch/train.py` (modèle ML) lisent cet historique |
| Streaming (voie chaude) | `streaming/predict_stream.py` | Structured Streaming : charge le `PipelineModel` au démarrage, prédit chaque nouvel événement Kafka, écrit dans HDFS |
| Analytique | `batch/analytics.py` + jobs Structured Streaming | Spark SQL sur l'historique + Spark Structured Streaming |

```
 Parquet TLC ─► producer.py ─► Kafka (taxi-trips) ─┬─► kafka_to_hdfs.py ─► HDFS /data/taxi/raw
                                                   │          │
                                                   │          ├─► analytics.py (Spark SQL) ─► HDFS /output/kpi
                                                   │          └─► train.py (Spark ML) ─► HDFS /models/taxi_fare/<version>
                                                   └─► predict_stream.py (charge le modèle) ─► HDFS /output/predictions
```
Cluster Docker : 1 master Spark, 2 workers, 1 NameNode, 2 DataNodes (réplication 2, volumes distincts), Kafka (KRaft), conteneur `spark-client` (driver). Tous les jobs Spark utilisent `spark://spark-master:7077` (`infra/submit.sh`).
Interfaces : Spark http://localhost:8080 · HDFS http://localhost:9870 · jobs http://localhost:4040.

## Prérequis
Docker + Compose v2, **≥ 8-10 Go de RAM pour Docker**, ~6 Go de disque, Internet. Machine limitée : `WORKER_MEMORY=2g` dans `.env` et `--max-rows-per-month 80000`.

## Lancement
```bash
cp .env.example .env
./scripts/download_data.sh                 # 5 fichiers Parquet (2024-01 → 2024-05) dans ./data
docker compose up -d --build
docker compose ps
docker compose exec namenode hdfs dfsadmin -report | grep -E "Live datanodes"    # 2 DataNodes
```
### 1. Historique (ingestion + voie froide)
```bash
docker compose run --rm producer python producer.py \
    --months 2024-01 2024-02 2024-03 2024-04 --max-rows-per-month 150000 --rate 0
docker compose exec spark-client bash /app/infra/submit.sh /app/streaming/kafka_to_hdfs.py --once
docker compose exec namenode hdfs dfs -ls /data/taxi/raw                  # un dossier par mois
docker compose exec namenode hdfs fsck /data/taxi/raw -files -blocks | grep -m3 "repl=2"
```
### 2. Indicateurs (Spark SQL)
```bash
docker compose exec spark-client bash /app/infra/submit.sh /app/batch/analytics.py
```
Les tableaux s'affichent dans le terminal et sont écrits dans HDFS `/output/kpi/*`.
### 3. Entraînement, comparaison, tuning, sauvegarde
```bash
docker compose exec -e CORES_MAX=4 -e EXECUTOR_MEMORY=1g -e DRIVER_MEMORY=1500m \
    spark-client bash /app/infra/submit.sh /app/batch/train.py --version v1
```
Split temporel : train 2024-01/02, validation 2024-03, test 2024-04. Résultats : affichés, HDFS `/output/ml/results`, `docs/results/ml_results_v1.json`. Modèle : HDFS `/models/taxi_fare/v1` (+ `latest`).
### 4. Voie chaude (inférence en continu)
```bash
docker compose exec -d spark-client bash -c "bash /app/infra/submit.sh /app/streaming/kafka_to_hdfs.py > /app/logs/archive.log 2>&1"
docker compose exec -d spark-client bash -c "bash /app/infra/submit.sh /app/streaming/predict_stream.py --model-path hdfs://namenode:9000/models/taxi_fare/v1 > /app/logs/predict.log 2>&1"
# attendre "Modele charge" dans logs/predict.log, puis rejouer des courses (200 evt/s)
docker compose run --rm producer python producer.py --months 2024-05 --max-rows-per-month 50000 --rate 200
```
Pour voir les prédictions défiler dans le terminal pendant la démo : lancer `predict_stream.py` au premier plan avec `--console`
(`docker compose exec spark-client bash /app/infra/submit.sh /app/streaming/predict_stream.py --model-path ... --console`).
### 5. Contrôle des résultats et reprise
```bash
docker compose exec spark-client bash /app/infra/submit.sh /app/batch/check_predictions.py   # lignes = ids distincts, MAE, délai moyen/p95
docker compose restart spark-client           # simule un crash des jobs
docker compose run --rm producer python producer.py --months 2024-05 --max-rows-per-month 60000 --seed 7 --rate 300   # pendant la panne
# relancer les jobs de l'étape 4 (reprise via checkpoints HDFS), puis relancer check_predictions.py
```
Arrêt : `docker compose down` (garde les volumes) ou `down -v` (remise à zéro).

## Choix de conception
- **Cible** : `fare_amount` (USD). **Variables connues à la commande** : distance estimée, heure, jour, week-end, zones de départ/arrivée, passagers, tarif. Exclus (fuite) : pourboire, péages, total, heure d'arrivée.
- **Identifiant** `trip_id` = hash du contenu (rejeu idempotent) ; doublons retirés à la lecture de l'historique.
- **Qualité** (`common/cleaning.py`) : tarif ∈ [3,200], distance ∈ [0,1 ; 100], passagers 1-6, RatecodeID 1-6, zones 1-265, durée 1 min-3 h, date dans le mois source. Rejets comptés dans `/output/kpi/kpi_data_quality`.
- **PipelineModel** = `SQLTransformer` + `StringIndexer`/`OneHotEncoder` (`handleInvalid="keep"` pour valeurs inconnues) + `VectorAssembler` + régresseur : le job streaming appelle seulement `transform()`.
- **Évaluation** : split temporel, références (moyenne, tarif au mile), RMSE/MAE/R², 3 techniques (LinearRegression, RandomForest, GBT), tuning par grille de la meilleure sur la validation, erreur par tranche de distance.
- **Reprise** : checkpoints HDFS (Kafka→HDFS et inférence) ; le sink fichier Spark garantit l'absence de doublons.
- **Limite** : `trip_distance` est la distance réelle de la course, présentée comme « distance estimée de l'itinéraire ». Le rejeu simule le temps réel.

## Dépannage
| Symptôme | Piste |
|---|---|
| Workers absents de l'UI Spark | `docker compose logs spark-worker-1` ; RAM Docker |
| `Initial job has not accepted any resources` | trop de jobs simultanés : réduire `CORES_MAX`/`EXECUTOR_MEMORY` ou arrêter un job |
| `could only be written to 0 of 1 minReplication` | attendre 2 DataNodes `Live` |
| `OutOfMemoryError` à l'entraînement | augmenter la mémoire ou baisser `--max-rows-per-month` |
| Téléchargement TLC en échec | télécharger à la main depuis la page TLC Trip Record Data vers `./data` |

## Arborescence
`ingestion/` producteur · `streaming/` jobs Structured Streaming · `batch/` analytics, entraînement, contrôle · `common/` config, schéma, qualité · `infra/` Dockerfile Spark, config HDFS, `submit.sh` · `scripts/` téléchargement · `docs/` modèle de rapport.
