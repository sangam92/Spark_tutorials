from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("schema creation").getOrCreate()
read_file=spark.read.csv('../data/test.csv',header='true')
read_file.show()

print("The number of rows in the file are ",read_file.count())
read_file.head(2)


#Below command describe the no of columns in the dataframe and the respective columns.
print("no of columns and name of the columns",len(read_file.columns),read_file.columns)


#provides the complete statistics of the numerical columns available in the dataframe
read_file.describe().show()


#Provides the statistics of a particular column
read_file.describe('salary').show()


#Select specific column from the dataframe
read_file.select('salary','age').show()

#saving the dataframes in the default location
read_file.select("name","age").write.save("../target/dataframe_save.csv",format="csv",mode="overwrite")

