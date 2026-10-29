"""Spark Structured Streaming job: reads JSON readings from Kafka topic 'counts',
validates them, and writes each 10-second micro-batch to Cassandra (traffic_ks.traffic_counts)."""
import os
from uuid import uuid1

from pyspark.sql import SparkSession, functions as F, types as T

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CHECKPOINT_DIR = os.getenv(
    "CHECKPOINT_DIR",
    os.path.expanduser("~/spark-checkpoints/kafka_to_cassandra_counts"),
)

# Mirrors the producer payload exactly
schema = T.StructType([
    T.StructField("timeuuid_id", T.StringType()),
    T.StructField("lgu_code", T.StringType()),
    T.StructField("sensor_id", T.StringType()),
    T.StructField("date_saved", T.StringType()),
    T.StructField("time_saved", T.StringType()),
    T.StructField("total", T.IntegerType()),
    T.StructField("car", T.IntegerType()),
    T.StructField("bus", T.IntegerType()),
    T.StructField("truck", T.IntegerType()),
    T.StructField("jeepney", T.IntegerType()),
    T.StructField("bike", T.IntegerType()),
    T.StructField("tryke", T.IntegerType()),
    T.StructField("others", T.IntegerType()),
])

spark = (SparkSession.builder
         .appName("KafkaToCassandraCounts")
         .config("spark.cassandra.connection.host", CASSANDRA_HOST)
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", "counts")
       .option("startingOffsets", "latest")
       .load())

parsed = (raw.selectExpr("CAST(value AS STRING) AS json_str")
          .select(F.from_json(F.col("json_str"), schema).alias("d"))
          .select("d.*")
          .filter(F.col("sensor_id").isNotNull() & F.col("total").isNotNull()))

# Keep the producer's timeuuid when it is well-formed; otherwise generate one so the row is not lost
UUID36 = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
make_timeuuid = F.udf(lambda: str(uuid1()), T.StringType())
parsed = parsed.withColumn(
    "timeuuid_id",
    F.when(F.col("timeuuid_id").rlike(UUID36), F.col("timeuuid_id")).otherwise(make_timeuuid()),
)

console_q = (parsed.writeStream
             .format("console")
             .outputMode("append")
             .option("truncate", False)
             .option("numRows", 20)
             .start())

to_cass = parsed.select(
    "sensor_id", "timeuuid_id", "bike", "bus", "car", "date_saved", "jeepney",
    "lgu_code", "others", "time_saved", "total", "truck", "tryke",
)


def write_to_cassandra(df, batch_id: int):
    rows = df.count()
    (df.write.format("org.apache.spark.sql.cassandra")
     .mode("append")
     .options(keyspace="traffic_ks", table="traffic_counts")
     .save())
    print(f"[Spark] wrote micro-batch {batch_id} rows={rows}")


cass_q = (to_cass.writeStream
          .foreachBatch(write_to_cassandra)
          .outputMode("append")
          .trigger(processingTime="10 seconds")
          .option("checkpointLocation", CHECKPOINT_DIR)
          .start())

spark.streams.awaitAnyTermination()
