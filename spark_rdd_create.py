from pyspark import SparkContext,SparkConf
conf =SparkConf().setAppName("Count").setMaster("local")
sc =SparkContext(conf=conf)
rdd_create = sc.textFile('test.txt')
rdd_count = rdd_create.first()
print(rdd_count)
#rdd_count.saveAsTextFile('count.txt')







