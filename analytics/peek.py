import sys

from common import exists, get_spark, table

spark = get_spark("peek")
for p in sys.argv[1:]:
    layer, name = p.split("/")
    path = table(layer, name)
    if exists(spark, path):
        df = spark.read.format("delta").load(path)
        print(f"== {p} : {df.count()} lignes")
        df.show(5, truncate=60)
    else:
        print(f"== {p} : absent")