-- Run with: cqlsh -f cassandra/schema.cql
CREATE KEYSPACE IF NOT EXISTS traffic_ks
  WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE traffic_ks;

-- One partition per sensor, newest readings first
CREATE TABLE IF NOT EXISTS traffic_counts (
  timeuuid_id timeuuid,
  lgu_code    text,
  sensor_id   text,
  date_saved  text,
  time_saved  text,
  car         int,
  total       int,
  bus         int,
  truck       int,
  jeepney     int,
  bike        int,
  tryke       int,
  others      int,
  PRIMARY KEY ((sensor_id), timeuuid_id)
) WITH CLUSTERING ORDER BY (timeuuid_id DESC);
