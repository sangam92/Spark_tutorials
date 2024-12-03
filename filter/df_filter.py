from pyspark.sql  import SparkSession
from pyspark.sql.functions import col

spark=SparkSession.builder.appName("df_filter").getOrCreate()

df = spark.read.csv('../data/employee.csv',header=True)
df.show()

##List of the Color
li=["Blue","Red"]

##Filter condition
df.filter(df.Color=="Blue").show()

##No filtercondition
df.filter(df.Color!="Blue").show()

##No filter with tilde expression
df.filter(~(df.Color=="Blue")).show()

##Using col function
df.filter(col("Color")=="Blue").show()

##using the SQL 
df.filter("Color=='Blue'").show()

##Multiple filter condition
df.filter((df.Color=="Blue")& (df.Count==29)).show()

## Filter based upon list
df.filter(df.Color.isin(li))

## Startswith and endswith
df.filter(df.Color.startswith('B')).show()

##Endswith and drop duplicates
print("endwith clause output")
df.filter(df.Color.endswith('n')).dropDuplicates().show()

##filter the dataframe based upon like reglikeular expression
df.filter(df.Color.like('%ee%')).show()

## filter the dataframe with regular expression
df.filter(df.Color.rlike("^Gr")).show()