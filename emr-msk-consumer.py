import sys
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro


# Configuration
CATALOG = "glue"
DATABASE = "iceberg_db_v2"
TABLE = "netflow_v2"
WAREHOUSE = "s3://anuawas/iceberg/warehouse/"
bootstrap_server = "b-3.demolarge.bt9x1c.c17.kafka.us-east-1.amazonaws.com:9098,b-2.demolarge.bt9x1c.c17.kafka.us-east-1.amazonaws.com:9098,b-1.demolarge.bt9x1c.c17.kafka.us-east-1.amazonaws.com:9098"
topic = "demo"
checkpoint_location = "s3://anuawas/tmp/checkpoint/"

# Spark session creation
spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE) \
    .appName("emr-msk-consumer") \
    .getOrCreate()

# Avro schema
schema_inline = '''{ 
    "namespace": "netflow.avro",
    "type": "record",
    "name": "netflow",
    "fields": [
      { "name": "event_type", "type": "string"},
      { "name": "peer_ip_src", "type": "string"},
      { "name": "ip_src", "type": "string"},
      { "name": "ip_dst", "type": "string"},
      { "name": "port_src", "type": "long"},
      { "name": "port_dst", "type": "long"},
      { "name": "tcp_flags", "type": "long"},
      { "name": "ip_proto", "type": "string"},
      { "name": "timestamp_start", "type": "string"},
      { "name": "timestamp_end", "type": "string"},
      { "name": "timestamp_arrival", "type": "string"},
      { "name": "export_proto_seqno", "type": "long"},
      { "name": "export_proto_version", "type": "long"},
      { "name": "packets", "type": "long"},
      { "name": "flows", "type": "long"},
      { "name": "bytes", "type": "long"},
      { "name": "writer_id", "type": "string"}
    ]
  }'''

# Create database if not exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}")

# Create Iceberg table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{DATABASE}.{TABLE} (
      event_type STRING,
      peer_ip_src STRING,
      ip_src STRING,
      ip_dst STRING,
      port_src BIGINT,
      port_dst BIGINT,
      tcp_flags BIGINT,
      ip_proto STRING,
      timestamp_start STRING,
      timestamp_end STRING,
      timestamp_arrival STRING,
      export_proto_seqno BIGINT,
      export_proto_version BIGINT,
      packets BIGINT,
      flows BIGINT,
      bytes BIGINT,
      writer_id STRING
    )
    USING iceberg
    TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd')
""")

# Read stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_server) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
    .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;") \
    .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger",5000000) \
    .load() \
    .select(from_avro("value", schema_inline).alias("data")) \
    .select("data.*")

# Write stream to Iceberg table
data = df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime='60 seconds' ) \
    .option("checkpointLocation", checkpoint_location) \
    .toTable(f"{CATALOG}.{DATABASE}.{TABLE}") 

data.awaitTermination()