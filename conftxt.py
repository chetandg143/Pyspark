import os

from pyspark import SparkConf ,SparkContext

from pyspark import BasicProfiler

class MyCstomProfiler(BasicProfiler):
    def show(self, id):
       print("My custom profiles are RDD : %s " % id)

conf = SparkConf().set("spark.python.profile" , "true")
sc = SparkContext('local','conftxt', conf=conf ,profiler_cls=MyCstomProfiler)

print(sc.parallelize(range(1000)).map(lambda x: 2 * x).take(10))
print(sc.parallelize(range(1000)).map(lambda y: 3 * y).take(10))
print(sc.parallelize(range(1000)).count())

"""def profile(self , func):
        raise NotImplemented

path = "/home/tibil/Downloads/Chetan"
def dump(self ,id , path):
    if not os.path.exists(path):
        os.makedir(path)
    stats = self.stats()
    if stats:
        p = os.path.join(path ,"rdd_%d.pstats" %id)
        print(stats.dump_stats(p))
"""
"""def show(self , id):
    stats = self.stats()
    if stats:
        print("=" * 60)
        print("Profile of RDD <id=%d>" % id)
        print("=" * 60)
        stats.sort_stats("time",
"cumulative").print_stats()
"""