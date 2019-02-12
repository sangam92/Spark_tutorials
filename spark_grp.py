
from pyspark import SparkContext,SparkConf
conf=SparkConf().setAppName("test")


sc = SparkContext(conf=conf)


read_data = sc.textFile('country.txt')
read_data = read_data.map(lambda x : (x,1))
read_data2 = read_data.groupByKey()

for i in read_data2.collect():
	print(i[0],list(i[1]))
