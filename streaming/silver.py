"""SILVER : nettoyé, typé, dédupliqué, conforme. Great Expectations à l'entrée de la couche.
Lignes invalides -> table de quarantaine. Écriture par MERGE (idempotent, rejouable).
usage : silver.py [once|stream]
"""
import json
import sys
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField, StructType)

import rules
from common import OPS, SENSORS, ckpt, degradation_expr, get_spark, table, trig, upsert, wait_for

mode = sys.argv[1] if len(sys.argv) > 1 else "once"
spark = get_spark("silver")

TEL_SCHEMA = StructType(
    [StructField("engine_id", StringType()), StructField("cycle", IntegerType()), StructField("event_ts", StringType())]
    + [StructField(o, DoubleType()) for o in OPS] + [StructField(s, DoubleType()) for s in SENSORS])

ENG_SCHEMA = StructType([
    StructField("engine_id", StringType()), StructField("model", StringType()), StructField("site", StringType()),
    StructField("operator_name", StringType()), StructField("operator_email", StringType()),
    StructField("status", StringType()), StructField("failure_cycle", IntegerType()),
    StructField("__op", StringType()), StructField("__source_ts_ms", LongType()), StructField("__deleted", StringType()),
])

QUALITY_SCHEMA = ("dataset string, batch_id long, checked_at timestamp, n_rows long, success boolean, "
                  "failed_expectations int, total_expectations int, details string")


def log_quality(name, bid, n, results):
    row = [(name, int(bid), datetime.utcnow(), int(n), all(r["success"] for r in results),
            sum(1 for r in results if not r["success"]), len(results), json.dumps(results))]
    spark.createDataFrame(row, QUALITY_SCHEMA).write.format("delta").mode("append").save(table("quality", "results"))
    failed = [r["expectation"] for r in results if not r["success"]]
    print(f"[GE] {name} batch={bid} rows={n} failed={failed}", flush=True)


def quarantine(bad, name):
    if bad.limit(1).count() > 0:
        (bad.withColumn("rejected_at", F.current_timestamp())
         .write.format("delta").mode("append").option("mergeSchema", "true").save(table("silver", name)))


def process_tel(batch, bid):
    if batch.isEmpty():
        return
    df = (batch.select(F.from_json("raw_value", TEL_SCHEMA).alias("j")).select("j.*")
          .withColumn("event_ts", F.to_timestamp("event_ts")).cache())
    n = df.count()
    log_quality("telemetry", bid, n, rules.run_ge(df, rules.TEL_CHECKS))
    valid = rules.spark_valid(rules.TEL_CHECKS)
    quarantine(df.filter(~valid), "quarantine_telemetry")
    good = (df.filter(valid).dropDuplicates(["engine_id", "cycle"])
            .withColumn("degradation_idx", degradation_expr())
            .withColumn("event_date", F.to_date("event_ts")))
    upsert(good, table("silver", "telemetry"), ["engine_id", "cycle"])
    df.unpersist()


def process_eng(batch, bid):
    if batch.isEmpty():
        return
    df = (batch.select(F.from_json("raw_value", ENG_SCHEMA).alias("j"))
          .select("j.engine_id", "j.model", "j.site",
                  F.col("j.operator_email").alias("operator_pseudo"),   # déjà pseudonymisé par Debezium
                  "j.status", "j.failure_cycle",
                  F.col("j.__op").alias("op"), F.col("j.__source_ts_ms").alias("src_ts_ms")).cache())
    log_quality("engines", bid, df.count(), rules.run_ge(df, rules.ENG_CHECKS))
    valid = rules.spark_valid(rules.ENG_CHECKS)
    quarantine(df.filter(~valid), "quarantine_engines")
    w = Window.partitionBy("engine_id").orderBy(F.col("src_ts_ms").desc())
    latest = (df.filter(valid).withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn"))
    upsert(latest, table("silver", "engines"), ["engine_id"], update_cond="s.src_ts_ms >= t.src_ts_ms")
    df.unpersist()


def start(src, fn, name):
    path = table("bronze", src)
    if not wait_for(spark, path, mode):
        print(f"[skip] {path} absent")
        return None
    stream = (spark.readStream.format("delta").option("skipChangeCommits", "true")
              .option("maxFilesPerTrigger", 100).load(path))
    return (stream.writeStream.foreachBatch(fn).option("checkpointLocation", ckpt(name))
            .trigger(**trig(mode)).start())


queries = [q for q in (start("telemetry", process_tel, "silver_telemetry"),
                       start("engines_cdc", process_eng, "silver_engines")) if q]
for q in queries:
    q.awaitTermination()