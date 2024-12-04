from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

spark=SparkSession.builder.appName("topn rows").getOrCreate()
df=spark.read.csv('../data/employee.csv',header=True)

#Creating the format for window function
windowCount= Window.partitionBy("State").orderBy(col("Count").desc())

#adding the row column that holds the row number
nrowdDF= df.withColumn("row",row_number().over(windowCount))

#filtering the column row which is having value 1
nrowdDF.filter(col("row")==1).show()



"""
Creating the same output with the help of the Temp table
"""

df.createOrReplaceTempView("Data")
spark.sql( "select State, Count, Color from (select  *, row_number()  over (partition by State order by Count) as rn  from Data) tmp where tmp.rn ==1").show()
