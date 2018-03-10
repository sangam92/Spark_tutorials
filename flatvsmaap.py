from pyspark import SparkContext,SparkConf

conf =SparkConf().setAppName("flatmap").setMaster("local")

sc = SparkContext(conf=conf)

read_file = sc.textFile('flat_map.txt')

print('The no of lines ',read_file.count())

file_map = read_file.map(lambda x : x.split(" "))

flat_map = read_file.flatMap(lambda x : x.split(" "))

print(file_map.collect())
print(flat_map.collect())

