from pyspark import SparkContext,SparkConf
import numpy as np

from pyspark.mllib.stat import Statistics
conf=SparkConf().setAppName("summary")


sc=SparkContext(conf=conf)


mat = sc.parallelize(
    [np.array([1.0, 10.0, 100.0]), np.array([2.0, 20.0, 200.0]), np.array([3.0, 30.0, 300.0])]
)  # an RDD of Vectors

# Compute column summary statistics.
summary = Statistics.colStats(mat)
print("The summary mean",summary.mean())  # a dense vector containing the mean value for each column
print("The summary variance",summary.variance())  # column-wise variance
print("The number of non-zeroes",summary.numNonzeros())  # number of nonzeros in each column


