from pyspark.sql import SparkSession
from imdb_cleaning import ImdbCleaner

if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    cleaner = ImdbCleaner(spark)
    cleaner.clean()
    spark.stop()