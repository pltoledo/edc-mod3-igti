from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession.builder.getOrCreate()
schema = spark.read.option("multiline","true").json('data/btc_stream/sample_schema.json').schema

def foreachBatch_save(df, batch_id):
    
    df_bids = (
        df.select('symbol', f.explode('bids').alias('bids'))
        .withColumn('price', f.col('bids').getField('px'))
        .withColumn('quantity', f.col('bids').getField('qty'))
        .withColumn('number_of_orders', f.col('bids').getField('num'))
        .withColumn('datetime', f.current_timestamp())
        .drop('bids')
    )
    df_asks = (
        df.select('symbol', f.explode('asks').alias('asks'))
        .withColumn('price', f.col('asks').getField('px'))
        .withColumn('quantity', f.col('asks').getField('qty'))
        .withColumn('number_of_orders', f.col('asks').getField('num'))
        .withColumn('datetime', f.current_timestamp())
        .drop('asks')
    )
    
    df_bids.write.mode('append').format('parquet').save('data/btc_stream/trusted/bids')
    df_asks.write.mode('append').format('parquet').save('data/btc_stream/trusted/asks')


if __name__ == "__main__":
    df = (
        spark
        .readStream
        .schema(schema)
        .option("multiline","true")
        .format("json")
        .load('data/btc_stream/raw')
    )
    query = (
        df.writeStream
        .foreachBatch(foreachBatch_save)
        .start()
    )
    query.awaitTermination(300)