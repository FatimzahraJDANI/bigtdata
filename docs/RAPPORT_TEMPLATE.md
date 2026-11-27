# Rapport – Prédiction en temps réel du prix des courses de taxi à New York
*(≤ 20 pages, viser ~15 : argumenter, peu de captures/code. Les zones [À COMPLÉTER] sont à remplir avec VOS résultats.)*

## 1. Cas métier et données
- **Problème** : estimer le prix (`fare_amount`, USD) d'une course au moment de la commande.
- **Source** : NYC TLC Trip Record Data – Yellow Taxi, 2024-01 à 2024-05, format Parquet, open data NYC. Volume brut : [À COMPLÉTER] ; utilisé : 150 000 courses/mois (échantillon aléatoire, ordre chronologique conservé).
- **Événement** = une course ; identifiant `trip_id` (hash) ; horodatage `tpep_pickup_datetime`.
- **Schéma** (`common/schema.py`), **variables explicatives** (distance estimée, heure, jour, week-end, zones, passagers, tarif) et **exclusions** justifiées (fuite : pourboire, péages, total, heure d'arrivée).
- **Qualité** : tableau `/output/kpi/kpi_data_quality` (lignes brutes, doublons, lignes rejetées par règle) : [À COMPLÉTER].

## 2. Architecture et lancement
- Schéma des services (reprendre celui du README), rôle de chaque conteneur, versions (Spark 3.5.3, Hadoop 3.3.6, Kafka 3.7.0), ressources (RAM, cœurs par worker).
- Justifier : réplication HDFS 2 + volumes distincts, Kafka 3 partitions, KRaft, Parquet partitionné par mois.
- Guide de lancement : renvoyer au README (étapes 1 à 5) en résumé.

## 3. Analyse batch et ML
- Indicateurs Spark SQL (prix par heure, jour, zone, tarif, tranche de distance) + 3-4 constats interprétés : [À COMPLÉTER].
- Préparation et découpage temporel : train 01-02, validation 03, test 04. Transformations apprises sur le train seulement.
- **Tableau de comparaison** (RMSE / MAE / R²) : Moyenne, Tarif au mile, LinearRegression, RandomForest, GBT, modèle tuné : [À COMPLÉTER depuis `/output/ml/results`].
- **Tuning** : technique retenue, grille, meilleurs hyperparamètres, gain observé : [À COMPLÉTER].
- **Interprétation et limites** : erreur par tranche de distance (`/output/ml/error_by_distance`), cas difficiles (courses aéroport/tarif forfaitaire, embouteillages, distance estimée), dérive possible entre mois.
- Version sauvegardée : `/models/taxi_fare/v1`.

## 4. Streaming et restitution
- Voie chaude : chargement du modèle, micro-batch 5 s, `maxOffsetsPerTrigger`, checkpoints HDFS.
- Sink Parquet dans HDFS + checkpoint : pas de doublon à la reprise (sink fichier exactly-once).
- **Délai observé** (moyenne, p95) : [À COMPLÉTER]. **Débit** supporté : [À COMPLÉTER] evt/s.
- **Vérification de reprise** : procédure (étape 5 du README), `check_predictions.py` : nombre de lignes = nombre d'ids distincts, aucun événement perdu : [À COMPLÉTER].
- Résultats : sortie de `check_predictions.py` (1 capture) et extrait des prédictions.

## 5. Contributions individuelles et bilan
| Étudiant | Nom et prénom | Tâches effectivement réalisées (développements, configs, analyses, vérifications, documentation, livrables) |
|---|---|---|
| Étudiant 1 | [À COMPLÉTER] | [À COMPLÉTER] |
| Étudiant 2 | [À COMPLÉTER] | [À COMPLÉTER] |
| Étudiant 3 | [À COMPLÉTER] | [À COMPLÉTER] |
| Étudiant 4 | [À COMPLÉTER] | [À COMPLÉTER] |

Tâches communes (intégration hebdomadaire, tests, démo, documentation) ; difficultés rencontrées et solutions ; limites et améliorations (Schema Registry, exactly-once, zones géographiques, features météo/événements).

---
### Plan de la vidéo (10 min)
1. Cas métier et données (1 min) 2. Architecture (2 min) 3. Résultats ML : comparaison + tuning (3 min) 4. Démo : producteur → Kafka → Spark  → dashboard (3 min) 5. Reprise après panne + conclusion (1 min).
