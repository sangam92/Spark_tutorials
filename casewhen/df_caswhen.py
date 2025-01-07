from pyspark.sql import SparkSession
from pyspark.sql.functions import when,expr

spark=SparkSession.builder.appName("casewhen").getOrCreate()

df=spark.read.csv('../data/employee.csv',header=True)

##using the dataframe 
caseDF= df.withColumn('casevalue',when(df.Count < 50,'low_value').when(df.Count > 50 ,'high_value').otherwise('too_high')).show()

##using the SQL option
caseDF1 = df.withColumn('casevalue',expr("case when Count < 50 then 'low value' " +
                        "case when Count > 50 then 'high_value'" +
                        "ELSE 'too_high' END"))
caseDF1.show()