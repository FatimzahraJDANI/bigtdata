from pyspark.sql import SparkSession


def get_spark(app_name):
    return (SparkSession.builder.appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            # evite que "2024-01" soit interprete comme une date dans les partitions
            .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
            .getOrCreate())
