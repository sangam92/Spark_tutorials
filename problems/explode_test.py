from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode,lit

spark=SparkSession.builder.appName("sangam_test_explode").getOrCreate()

data = [
    (1, ["apple", "banana", "cherry"]),
    (2, ["grape", "orange"]),
    (3, [])
]

df=spark.createDataFrame(data,["id","fruits"])
df.show(truncate=False)


df_explode=df.withColumn("fruits",explode(col("fruits")))
df_explode.show(truncate=False)