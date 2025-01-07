from pyspark.sql import SparkSession
from pyspark.sql.functions import min,row_number,col
from pyspark.sql.window import Window




spark=SparkSession.builder.appName("duplicate").getOrCreate()

df=spark.read.csv('../data/employee_duplicate.csv',header=True)
df.show()

windowCount= Window.partitionBy('State','Color').orderBy(col('State'),col('Color').desc())
df1=df.withColumn('row',row_number().over(windowCount)).filter(col('row')==1).drop(col('row'))
df1.show()




