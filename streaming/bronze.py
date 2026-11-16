"""BRONZE : données brutes telles que reçues de Kafka, sans perte, + horodatage d'ingestion et source.
usage : bronze.py [once|stream]
"""
import sys

from pyspark.sql import functions as F

from common import KAFKA, ckpt, get_spark, table, trig

mode = sys.argv[1] if len(sys.argv) > 1 else "once"
spark = get_spark("bronze")


def ingest(topic, name, source):
    df = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", KAFKA)
          .option("subscribe", topic)
          .option("startingOffsets", "earliest")
          .option("maxOffsetsPerTrigger", 200000)
          .option("failOnDataLoss", "false")
          .load())
    out = df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingestion_ts"),
        F.lit(source).alias("source"),
    )
    return (out.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", ckpt(f"bronze_{name}"))
            .trigger(**trig(mode)).start(table("bronze", name)))


queries = [ingest("sensors.raw", "telemetry", "mqtt-bridge"),
           ingest("pg.public.engines", "engines_cdc", "debezium-postgres")]
for q in queries:
    q.awaitTermination()