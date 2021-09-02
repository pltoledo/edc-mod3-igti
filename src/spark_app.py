from pyspark.sql import SparkSession
from src.imdb_cleaning import ImdbCleaner
import sys

if __name__ == "__main__":
    if len(sys.argv) > 1:
        spark = (
            SparkSession
            .builder
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
        cleaner = ImdbCleaner(spark, sys.argv[1])
        cleaner.clean()
        spark.stop()