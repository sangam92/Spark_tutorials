from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("empty_df").getOrCreate() 
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)