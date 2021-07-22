from pyspark.sql import SparkSession
from pyspark.sql.functions import window, from_json, col
from pyspark.sql.types import *
import time

def milli_time_n_days_ago(n):
    return round(time.time() * 1000) - (n * 24 * 60 * 60 * 1000)

def foreach_batch_function(df, epoch_id):
    user_page_view_count_df = user_page_view_count_agg(df)
    page_view_count_df = page_view_count_agg(df)

    user_page_view_count_df.write \
            .outputMode("update") \
            .format("console") \
            .save()

    page_view_count_df.write \
            .outputMode("update") \
            .format("console") \
            .save()

def user_page_view_count_agg(df):
    output = df.filter(df.timestamp >= milli_time_n_days_ago(7)).groupBy(
        df.userId, 
        df.pageId
        ).count()
    
    return output

def page_view_count_agg(df):
    output = df.filter(df.timestamp >= milli_time_n_days_ago(7)).groupBy(
        df.pageId
        ).count()
    
    return output

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master('local') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
        .getOrCreate()

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

    user_page_view_count_df = user_page_view_count_agg(events_df)
    page_view_count_df = page_view_count_agg(events_df)

    query1 = user_page_view_count_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()

    query2 = page_view_count_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()

    spark.streams.awaitAnyTermination()

    # query = events_df \
    #     .writeStream \
    #     .outputMode("update") \
    #     .foreachBatch(foreach_batch_function) \
    #     .start() \
    #     .awaitTermination()