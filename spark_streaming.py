
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]","test")
ssc = StreamingContext(sc,1)

lines = ssc.socketTextStream("localhost",9999)


words = lines.flatMap(lambda line:line.split(" "))
pairs = words.map(lambda word : (word,1))
wordcounts = pairs.reduceByKey(lambda a,b : a + b)

wordcounts.pprint()

ssc.start()

ssc.awaitTermination()




