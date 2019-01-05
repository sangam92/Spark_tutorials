


from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
conf=SparkConf().setAppName("dataframe")
sc=SparkContext(conf=conf)
sqlcontext= SQLContext(sc)
read_file=sqlcontext.read.csv('/home/hduser/sangam/test.csv',header='true')
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
read_file.select("name","age").write.save("dataframe_save.csv",format="csv")

