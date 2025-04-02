from pyspark.sql import SparkSession
from delta import *



builder = SparkSession.builder.appName("DeltaLakeACID") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
 
spark = configure_spark_with_delta_pip(builder).getOrCreate()
# Sample DataFrame
data = [(1, "Alice", 1000), (2, "Bob", 1500)]
df = spark.createDataFrame(data, ["id", "name", "balance"])

# Write DataFrame as Delta Table
df.write.format("delta").mode("overwrite").save("/target")

# Read and show Delta table
df_read = spark.read.format("delta").load("/target")
df_read.show()

