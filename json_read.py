from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext


conf = SparkConf().setAppName("read_json")
sc = SparkContext(conf=conf)


sqlcontext = SQLContext(sc)

read_json = sqlcontext.read.json('/home/hduser/sangam/employee.json')

read_json.show()

read_json.printSchema()


read_json.select("id","age").show()


read_json.max("id").show()

