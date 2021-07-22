from pyspark.sql import SparkSession
from pyspark.sql.functions import window, from_json, col
from pyspark.sql.types import *
import time

def milli_time_n_days_ago(n):
    return round(time.time() * 1000) - (n * 24 * 60 * 60 * 1000)

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master('local') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
        .getOrCreate()

        # .config('spark.executor.memory', '6g') \
        # .config('spark.streaming.concurrentJobs', '2') \

    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "view_events") \
    .option("startingOffsets", "earliest") \
    .load() 

    events_string_df = df.selectExpr("CAST(value AS STRING)")

    schema = StructType([
        StructField("userId", StringType()),
        StructField("pageId", StringType()),
        StructField("timestamp", LongType())
    ])

    events_df = events_string_df \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
    # events_df = events_df.withColumn("timestamp", events_df.timestamp.cast(TimestampType()))

    user_page_view_windowed_count_df = events_df.filter(events_df.timestamp >= milli_time_n_days_ago(0.005)).groupBy(
        events_df.userId, 
        events_df.pageId
        ).count()

    page_view_windowed_count_df = events_df.filter(events_df.timestamp >= milli_time_n_days_ago(0.005)).groupBy(
        events_df.pageId
        ).count()

    query1 = user_page_view_windowed_count_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()

    query2 = page_view_windowed_count_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()
    
    spark.streams.awaitAnyTermination()
    # query2.awaitTermination()