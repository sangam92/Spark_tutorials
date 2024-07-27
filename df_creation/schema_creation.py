from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,StructField

spark = SparkSession.builder.appName("schema creation").getOrCreate()

"""
Read the CSV file and provide a suitable schema.
"""

schema=StructType(
        [
        StructField("State",StringType()),
        StructField("Country",StringType()),
        StructField("Region",StringType())
        ]
)

df=spark.read.csv('..\data\employee_without_schema.csv',header=False,schema=schema)
print(df.show())

df.printSchema()