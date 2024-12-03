from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

spark=SparkSession.builder.appName("new column addition").getOrCreate()

readDF=spark.read.csv('../data/employee.csv',header=True)

#printing the dataset
readDF.show()

#printing the schema
readDF.printSchema()

#adding an extra column
readDF=readDF.withColumn("count_added",col("Count")+1)
readDF.show()

#changing the datatype of the existing column
readDF=readDF.withColumn("Count",col("Count").cast("Integer"))

#printing the schema
readDF.printSchema()

#Adding a new column and giving it 's value
readDF=readDF.withColumn("status",lit("test"))
readDF.show()

#Renaming a column
readDF=readDF.withColumnRenamed("status","new_status")
readDF.printSchema()

#Dropping the new column
readDF=readDF.drop("count_added")
readDF.printSchema()
