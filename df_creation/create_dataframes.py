from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("create df").getOrCreate() 
"""
Creating the datframe from the csv file and displaying the records
"""
df=spark.read.csv("..\data\employee.csv",header=True)
print(df.show(10))



