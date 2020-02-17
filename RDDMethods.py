from pyspark import  SparkContext
from operator import add
from operator import mul
sc = SparkContext("local" , "RDDMethods")
words =sc.parallelize(
    ["Scala" ,
    "spark" ,
    "java ",
    "hadoop" ,
    "python" ,
    "Spark vs hadoop ",
    "pyspark vs spark" ,
    "AKKA" ]
)
#counting the number of elements present in words using count() method
counts = words.count()
print("Number of elements in RDD --> %i " % counts)

#is used to retrive elements from words using collect() method
collect = words.collect()
print("elements are : %s " %collect)

#print elements using foreach loop
def f(x) : print(x)
fore = words.foreach(f)

#filter by giving condition
words_filter = words.filter(lambda x : 'spark' in x)
filtered = words_filter.collect()
print("collected spark name items : %s" %filtered)

#used to map to set(key ,value) pairs

words_map = words.map(lambda x: (x, 1))
maps = words_map.collect()
print("key value pair --> %s" % (maps))



nums = sc.parallelize([10,20,45,55,60,70])
adding = nums.reduce(mul)
print("added items are %i " % (adding))


x =sc.parallelize([("spark" , 1) , ("pyspark" ,6)])
y =sc.parallelize([("spark" , 7) , ("pyspark" ,2)])
joined = x.join(y)
final = joined.collect()
print("join RDD --> %s" % (final))
x.cache()
catching  = x.persist().is_cached
print("x has catched --> %s "% (catching))

words.cache()
wor = words.persist().is_cached
print("words list is --> %s" % (wor))