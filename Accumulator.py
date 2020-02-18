
from pyspark import SparkContext


sc = SparkContext("local" ,"Accumulator app")
num = sc.accumulator(1)
def f(x):
    global num
    num+=x
rd = sc.parallelize([12,13,14,5])
rd.foreach(f)
final = num.value
print("Accumulated value is --> %i" % (final))