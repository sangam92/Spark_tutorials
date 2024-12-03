from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,StructField,IntegerType
from pyspark.sql.functions import col,struct,when


spark=SparkSession.builder.appName("create df").getOrCreate() 
schema= StructType(
        [StructField("Name",StructType(
            [
                StructField("FirstName",StringType()),
                StructField("LastName",StringType()),
                StructField("MiddleName",StringType())
            ]
        )),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
        ]
)


data = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]


df=spark.createDataFrame(data=data,schema=schema)
df.show(truncate=False)

### Adding and Changing the Struct of the data

dfStructNew= df.withColumn("otherdetails",
                           struct(col("id").alias("identifier"),
                                  col("gender").alias("gender"),
                                  col("salary").alias("salary"),
                                  when(col("salary").cast(IntegerType()) < 2000,"low")
                                    .when(col("salary").cast(IntegerType())<4000,"Medium")
                                    .otherwise("High").alias("SalaryGrade"))).drop("id","gender","salary")

dfStructNew.printSchema()
dfStructNew.show(truncate=False)