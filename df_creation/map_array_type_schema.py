from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,ArrayType,MapType,StructField
from pyspark.sql.functions import col,lit,create_map

spark=SparkSession.builder.appName("map and array type").getOrCreate()

schema=StructType([
                StructField("Player No",IntegerType()),
                StructField("Player Name",StringType()),
                StructField("Matches Played",StringType()),
                StructField("Batting Averages",StringType())
])

df=spark.read.csv('../data/map_array_type_data.csv',schema=schema)
df.printSchema()
df.show(truncate=False)

df= df.withColumn("Map_Example",create_map(
    lit("Test Matches Played"),col()
))