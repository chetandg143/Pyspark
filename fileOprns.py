from pyspark import SparkContext as sc

sc =sc("local" ,"fileOprns")
rdd = sc.textFile("/home/tibil/PycharmProjects/Pyspark/blogtexts")
rdd.take(5)
def func(lines):
    lines = lines.lower()
    lines = lines.split()
    return  lines

rdd1 = rdd.map(func)
print(rdd1.take(5))

rdd2 = rdd.flatMap(func)
rdd2.take(5)

stopwords = ['is', 'am' ,'are','the' ,'for' , 'a']
rdd3 =rdd.filter(lambda a: a  not in stopwords)
rdd3.take(5)

#GroupBy
rdd4 = rdd3.groupBy(lambda w:w[0:3])
print([(k,list(v)) for (k, v) in rdd4.take(4)])

#reduce_key
rdd3_mapped = rdd3.map(lambda x: (x,1))
rdd3_grouped = rdd3_mapped.groupByKey()
print(list((j[0], list(j[1])) for j in rdd3_grouped.take(5)))




rdd3.filter(lambda  x :x == 'manager,').collect()

num_rdd = sc.parallelize(range(1,1000))
num_rdd.reduce(lambda x,y: x+y)

# num_rdd = (12,15,4,54,54,56,65)
print("Sum is =" ,num_rdd.sum())
print("Maximum = " ,num_rdd.max())
print("Minimum = " ,num_rdd.min())
print("Variance = " ,num_rdd.variance())

