from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("load json").getOrCreate()

#Reading Single Line JSON
df=spark.read.json("../data/employee.json")
df.show()


#Reading Multiline JSON

df=spark.read.option("multiline",True).json("../data/employee_multiline.json")
df.show()