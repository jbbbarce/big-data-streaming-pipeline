#!/bin/bash
# Submits the streaming job with the Kafka and Cassandra connectors (Spark 4.0.1, Scala 2.13).
set -e
SPARK_HOME="${SPARK_HOME:-/opt/spark}"

"$SPARK_HOME/bin/spark-submit" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.5.1 \
  --conf spark.cassandra.connection.host="${CASSANDRA_HOST:-127.0.0.1}" \
  "$(dirname "$0")/fetch_stream_to_cassandra.py"
