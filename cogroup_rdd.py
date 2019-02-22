from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("cogroup_example")
sc = SparkContext(conf=conf)
rdd_batsman = sc.textFile('batsman_rank.txt')
rdd_bowler = sc.textFile('bowler_rank.txt')

final_rdd=rdd_batsman.cogroup(rdd_bowler)
print(final_rdd.collect())
