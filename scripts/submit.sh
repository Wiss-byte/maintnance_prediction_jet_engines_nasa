#!/usr/bin/env bash
set -e
SCRIPT=$1; shift
exec /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 3g \
  --packages "$SPARK_PACKAGES" --conf spark.jars.ivy=/tmp/ivy \
  --conf "spark.driver.extraJavaOptions=-Duser.timezone=UTC" \
  "/app/$SCRIPT" "$@"