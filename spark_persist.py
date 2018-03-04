from pyspark import SparkContext,SparkConf
conf =SparkConf().setAppName("Count").setMaster("local")
sc =SparkContext(conf=conf)
rdd_create = sc.textFile('test.txt')
rdd_create.persist
rdd_count = rdd_create.first()
rdd_count2= rdd_create.count()
print('The first line is' ,rdd_count)
print('The count of lines is ',rdd_count2)








