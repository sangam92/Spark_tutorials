from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col,row_number


spark=SparkSession.builder.appName('second salary').getOrCreate()

df=spark.read.csv('../data/employee_salary.txt',header=True)
df.show()
windowcount=Window.partitionBy('dept').orderBy(col('Salary').desc())

df1=df.withColumn('rank',row_number().over(windowcount)).filter(col('rank')==2)
df1.show()