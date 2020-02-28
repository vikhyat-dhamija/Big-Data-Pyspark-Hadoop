import pyspark
from pyspark import SparkContext
from operator import add
sc = SparkContext('local[*]')
f=sc.textFile('sample_12.txt')
data = f.flatMap(lambda line : line.split()).map(lambda x : (x,1)).reduceByKey(add)
data2 = f.flatMap(lambda line: line.split(".")).map(lambda line: line.strip().split(" ")).flatMap(lambda xs: (tuple(x) for x in zip(xs, xs[1:]))).map(lambda x : (x,1)).reduceByKey(add)
yahoo=data.cartesian(data2).filter(lambda x : x[0][0]==x[1][0][0]).map(lambda x : (x[0][0],x[1][0],x[1][1]/x[0][1])).collect()

print(yahoo)