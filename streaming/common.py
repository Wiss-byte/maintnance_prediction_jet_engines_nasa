import os
import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from schema import OPS, SENSORS  # noqa: F401

LAKE = os.getenv("LAKE", "s3a://lakehouse")
KAFKA = os.getenv("KAFKA", "kafka:29092")
FEATURE_VERSION = os.getenv("FEATURE_VERSION", "v1")
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/pm")
PG_PROPS = {"user": os.getenv("PG_USER", "postgres"), "password": os.getenv("PG_PASSWORD", "postgres"),
            "driver": "org.postgresql.Driver"}


def table(layer, name):
    return f"{LAKE}/{layer}/{name}"


def ckpt(name):
    return f"{LAKE}/_checkpoints/{name}"


def trig(mode):
    """once = traite le retard puis s'arrête (Airflow) ; stream = continu."""
    return {"availableNow": True} if mode == "once" else {"processingTime": "10 seconds"}


def get_spark(app):
    b = (SparkSession.builder.appName(app)
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false"))
    if LAKE.startswith("s3a://"):
        b = (b.config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
             .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "minio"))
             .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "minio12345"))
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def exists(spark, path):
    return DeltaTable.isDeltaTable(spark, path)


def wait_for(spark, path, mode, timeout=900):
    t0 = time.time()
    while not exists(spark, path):
        if mode == "once" or time.time() - t0 > timeout:
            return False
        print(f"[wait] {path} n'existe pas encore...", flush=True)
        time.sleep(5)
    return True


def upsert(df, path, keys, update_cond=None):
    """MERGE idempotent : un rejeu ne crée jamais de doublon."""
    spark = df.sparkSession
    if exists(spark, path):
        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        m = DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), cond)
        if update_cond:
            m = m.whenMatchedUpdateAll(condition=update_cond)
        m.whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").save(path)


def degradation_expr():
    """Indice de dégradation : 0 = sain, ~1 = proche de la panne."""
    terms = [((F.col(s) / nom) - 1.0) / drift for s, (nom, drift) in SENSORS.items()]
    total = terms[0]
    for t in terms[1:]:
        total = total + t
    return total / float(len(terms))