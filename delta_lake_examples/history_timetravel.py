from pyspark.sql import SparkSession
from delta import *



builder = SparkSession.builder.appName("DeltaLakeACID") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    
spark = configure_spark_with_delta_pip(builder).getOrCreate()
# Sample DataFrame
data = [(1, "Virat", 10000), (2, "Rohit", 9000)]
df = spark.createDataFrame(data, ["id", "name", "Runs"])

# Write DataFrame as Delta Table
df.write.format("delta").mode("overwrite").save("/target/timetrvl")

# Read and show Delta table
df_read = spark.read.format("delta").load("/target/timetrvl")

delta_table = DeltaTable.forPath(spark, "/target/timetrvl")
delta_table.delete("name = 'Virat'")

#updating the condition
delta_table.update(condition="name = 'Rohit'", set={"name": "'Sachin'"})

#Cheking the history
spark.sql("DESCRIBE HISTORY delta.`/target/timetrvl`").show(truncate=False)

#Time Travel to older verison
df_older = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/target/timetrvl")

df_older.show()
