from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("UK_TFL_STREAMING_CONSUMER") \
    .getOrCreate()

kafka_servers = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
topic = "ukde011025tfldata"

# ----------------------------
# STREAM FROM KAFKA
# ----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING) AS json_value")

# ----------------------------
# STREAM TO HDFS
# ----------------------------
query = parsed_df.writeStream \
    .format("json") \
    .option("path", "hdfs:///tmp/DE011025/uk/streaming/incoming_batches/") \
    .option("checkpointLocation", "hdfs:///tmp/DE011025/uk/streaming/checkpoints/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
