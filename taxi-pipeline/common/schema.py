"""Schema JSON des evenements Kafka (une course = un evenement)."""
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

EVENT_SCHEMA = StructType([
    StructField("trip_id", StringType()),                 # identifiant (hash du contenu)
    StructField("source_month", StringType()),            # mois du fichier TLC d'origine
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", StringType()),    # horodatage de l'evenement (prise en charge)
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),           # miles (distance estimee de l'itineraire)
    StructField("RatecodeID", IntegerType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),             # CIBLE (USD)
])
