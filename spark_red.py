
from pyspark import SparkContext,SparkConf
conf=SparkConf().setAppName("test")


sc = SparkContext(conf=conf)



def sum_red(x,y):
	return x+y
read_data = sc.textFile('country.txt')
read_data = read_data.map(lambda x : (x,1))
read_data2 = read_data.reduceByKey(sum_red)


print(read_data2.collect())
