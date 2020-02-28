import pyspark
from pyspark import SparkContext
from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry
from operator import add
from pyspark.sql import SparkSession

sc = SparkContext()
r=sc.textFile("part-00000")
m=r.flatMap(lambda x: x.split('\n')).filter(lambda x : "A" in x).map(lambda x : (x.strip("A, ")).split(' ')).map(lambda x: tuple(list(map(int, x))))
#n=m.map(lambda x : MatrixEntry(tuple(x)))

spark = SparkSession(sc)
#m.toDF().show()
print(hasattr(m,"toDF"))

cmat=CoordinateMatrix(m)
#mat = CoordinateMatrix(n)
#o=mat.take(5)
print(cmat.numRows()) # 3
print(cmat.numCols())

rowmat = cmat.toRowMatrix()

print(rowmat.numRows()) # 3
print(rowmat.numCols())

