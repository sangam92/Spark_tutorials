from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,col,avg,min,max
from pyspark.sql.types import IntegerType


spark=SparkSession.builder.appName("groupby").getOrCreate()
df=spark.read.csv('../data/employee.csv',header=True)

##grouping the data 
df=df.withColumn("Count",col("Count").cast(IntegerType()))
df.printSchema()
df1=df.groupBy("Color").agg(sum(col("Count"))).select(col("sum(Count)"),col("Color"))
df.show()


##Multiple groupby conditions
df2=df.groupBy("State","Color").agg(sum("Count")).select("State","sum(Count)")
df2.show()


##Multiple aggregates at a time 
df3= df.groupBy("State").agg(sum("Count"),avg("Count"),min("Count"),max("Count"))
df3.show()


##using filter on aggregate data
df4=df.groupBy("State").agg(sum("Count")).where(col("sum(Count)") == 546812).select("State","sum(Count)")
df4.show()