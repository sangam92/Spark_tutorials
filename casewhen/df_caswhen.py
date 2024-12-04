from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark=SparkSession.builder.appName("casewhen").getOrCreate()

df=spark.read.csv('../data/employee.csv',header=True)


caseDF= df.withColumn('casevalue',when(df.Count < 50,'low_value').when(df.Count > 50 ,'high_value').otherwise('too_high')).show()