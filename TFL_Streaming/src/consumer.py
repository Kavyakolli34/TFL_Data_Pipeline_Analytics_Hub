from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
        .appName("UK_TFL_STREAMING_CONSUMER_DEDUPED")
        .getOrCreate()
)

kafka_servers = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
topic = "ukde011025tfldata"

# ----------------------------
# READ STREAM FROM KAFKA
# ----------------------------
df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
)

json_df = df.selectExpr("CAST(value AS STRING)")

# ----------------------------
# PARSE JSON
# ----------------------------
parsed = spark.read.json(json_df.rdd.map(lambda x: x.value))

# Ensure timestamp exists
parsed = parsed.withColumn(
    "event_ts",
    to_timestamp(col("timestamp"))  # TFL provides "timestamp"
)

# ----------------------------
# BUILD UNIQUE DEDUPE KEY
# ----------------------------
dedupe_ready = (
    parsed
        .withColumn("unique_key",
            concat_ws("_",
                col("id"),                 # event id
                col("stationId"),          # location
                col("expectedArrival")     # arrival time
            )
        )
)

# ----------------------------
# WATERMARK + DEDUPE
# ----------------------------
deduped = (
    dedupe_ready
        .withWatermark("event_ts", "30 minutes")
        .dropDuplicates(["unique_key"])
)

# ----------------------------
# ADD DATE/HOUR PARTITIONS
# ----------------------------
final_df = (
    deduped
        .withColumn("date", to_date(col("event_ts")))
        .withColumn("hour", hour(col("event_ts")))
)

# ----------------------------
# WRITE TO HDFS AS PARQUET
# ----------------------------
query = (
    final_df.writeStream
        .format("parquet")
        .option("path", "hdfs:///tfl/stream/deduped/")
        .option("checkpointLocation", "hdfs:///tfl/stream/checkpoints/")
        .partitionBy("line", "date", "hour")
        .outputMode("append")
        .start()
)

query.awaitTermination()
