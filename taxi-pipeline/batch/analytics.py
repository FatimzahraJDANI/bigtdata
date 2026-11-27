"""ANALYTIQUE BATCH (Spark SQL) sur l'historique HDFS -> indicateurs ecrits dans HDFS (/output/kpi)."""
from pyspark.sql import functions as F

from common.cleaning import clean, quality_rules
from common.config import KPI_BASE, RAW_PATH
from common.spark_utils import get_spark


def write_hdfs(df, name):
    df.coalesce(1).write.mode("overwrite").parquet(f"{KPI_BASE}/{name}")


def main():
    spark = get_spark("batch_analytics")
    spark.sparkContext.setLogLevel("WARN")

    raw = spark.read.parquet(RAW_PATH)
    n_raw = raw.count()
    hist = raw.dropDuplicates(["trip_id"]).cache()
    n_unique = hist.count()

    # ---- rapport qualite: nombre de lignes violant chaque regle ----
    q = [(f"lignes_brutes", n_raw), ("doublons_trip_id", n_raw - n_unique), ("lignes_uniques", n_unique)]
    agg = hist.agg(*[F.sum(F.when(~cond, 1).otherwise(0)).alias(name) for name, cond in quality_rules().items()]).first()
    q += [(k, int(agg[k])) for k in agg.asDict()]
    trips = clean(hist)
    n_clean = trips.count()
    q.append(("lignes_apres_nettoyage", n_clean))
    qdf = spark.createDataFrame(q, ["regle", "nb_lignes"])
    write_hdfs(qdf, "kpi_data_quality")
    qdf.show(30, truncate=False)

    trips.createOrReplaceTempView("trips")

    kpis = {
        "kpi_fare_by_hour": """
            SELECT hour(pickup_ts) AS hour, COUNT(*) AS trips, ROUND(AVG(fare_amount),2) AS avg_fare,
                   ROUND(AVG(trip_distance),2) AS avg_distance_miles,
                   ROUND(SUM(fare_amount)/SUM(trip_distance),2) AS fare_per_mile
            FROM trips GROUP BY 1 ORDER BY 1""",
        "kpi_fare_by_dow": """
            SELECT dayofweek(pickup_ts) AS dow_num, date_format(pickup_ts,'E') AS dow, COUNT(*) AS trips,
                   ROUND(AVG(fare_amount),2) AS avg_fare, ROUND(AVG(trip_distance),2) AS avg_distance_miles
            FROM trips GROUP BY 1, 2 ORDER BY 1""",
        "kpi_top_pickup_zones": """
            SELECT PULocationID AS zone_id, COUNT(*) AS trips, ROUND(AVG(fare_amount),2) AS avg_fare,
                   ROUND(SUM(fare_amount),0) AS total_fare
            FROM trips GROUP BY 1 ORDER BY trips DESC LIMIT 20""",
        "kpi_fare_by_ratecode": """
            SELECT RatecodeID AS ratecode, COUNT(*) AS trips, ROUND(AVG(fare_amount),2) AS avg_fare,
                   ROUND(AVG(trip_distance),2) AS avg_distance_miles
            FROM trips GROUP BY 1 ORDER BY 1""",
        "kpi_daily": """
            SELECT to_date(pickup_ts) AS day, COUNT(*) AS trips, ROUND(SUM(fare_amount),0) AS total_fare,
                   ROUND(AVG(fare_amount),2) AS avg_fare
            FROM trips GROUP BY 1 ORDER BY 1""",
        "kpi_fare_by_distance_bucket": """
            SELECT CASE WHEN trip_distance < 1 THEN '0-1' WHEN trip_distance < 3 THEN '1-3'
                        WHEN trip_distance < 6 THEN '3-6' WHEN trip_distance < 10 THEN '6-10'
                        WHEN trip_distance < 20 THEN '10-20' ELSE '20+' END AS distance_miles,
                   COUNT(*) AS trips, ROUND(AVG(fare_amount),2) AS avg_fare,
                   ROUND(STDDEV(fare_amount),2) AS std_fare
            FROM trips GROUP BY 1 ORDER BY MIN(trip_distance)""",
    }
    for table, sql in kpis.items():
        df = spark.sql(sql)
        write_hdfs(df, table)
        print(f"--- {table}")
        df.show(10, truncate=False)

    print(f"Lignes brutes={n_raw}, uniques={n_unique}, apres nettoyage={n_clean}")
    spark.stop()


if __name__ == "__main__":
    main()
