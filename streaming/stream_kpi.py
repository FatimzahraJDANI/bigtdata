from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("stream_kpi")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "taxi-trips")
       .option("startingOffsets", "latest")
       .load())

events = raw.select(
    F.col("timestamp").alias("ts"),
    F.get_json_object(F.col("value").cast("string"), "$.fare_amount").cast("double").alias("fare"),
    F.get_json_object(F.col("value").cast("string"), "$.trip_distance").cast("double").alias("dist"))

agg = (events.withWatermark("ts", "1 minute")
       .groupBy(F.window("ts", "1 minute"))
       .agg(F.count("*").alias("trips"),
            F.round(F.avg("fare"), 2).alias("avg_fare"),
            F.round(F.avg("dist"), 2).alias("avg_dist")))

(agg.writeStream.outputMode("update").format("console")
    .option("truncate", "false").start().awaitTermination())