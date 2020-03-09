
from pyspark import SparkContext as sc, Row
from pyspark.python.pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
spark = SparkSession \
     .builder \
      .appName("Python Spark SQL basic example") \
      .config("spark.some.config.option", "some-value") \
     .getOrCreate()
# df = spark.read.format("jdbc") \
#     .options(url="jdbc:mysql://localhost:3306/pysparkDB") \
#     .options(driver="com.mysql.jdbc.Driver") \
#     .options(dbtable='Task_List') \
#     .options(user="root") \
#     .options(password="MYSQL123").load().show()
# l  = [('Chetan' , 23)]
# sc = spark.createDataFrame(l,['name','age']).collect()
# print(sc)
#
# s = spark.range(1,7,5).collect()
# print(s)
########################################################################################
# df =spark.createDataFrame([('a',1), ('b',2),('c',3),('d',4),('e',5)])
# df.select(df.colRegex("`(col1)?+.+`")).show()
# # df.select(df.colRegex("`(Col1)?+.+`")).show()
#
# print(df.collect())
# print(df.columns)
# print(df.count())
#
#
# People = spark.createDataFrame([('chetan',23),('Ashok',22),('Shoiab',24),('Uday',12),('Guru',25),('Pralhad',100)],['Name','Age'])
# People.show()
# df2 =People.filter(People['age']  == 23).show()
#
# print(People.select('Name','Age').collect())
# # People.createGlobalTempView('People')
# # df2 =spark.sql("select * from global_temp.People")
#
# People.cube('Name','Age').count().orderBy('Name','Age').fillna('abc').show()
# print(People.describe().collect())
#
# dist = People.distinct().count()
# print(dist)

##########################################################################
# df3 = sc.parallelize([ Row(name='Alice', age=5, height=80 ) ,Row(name='Alice', age=5, height=80),Row(name='Alice', age=10, height=80)]).collect()
# df3.dropDuplicates().show()

df3 = spark.createDataFrame([ ('alice',5,80),('bharath',15,180),('Dinga',30,150),('Ashok',23,165),('Chetan',24,145)],['Name','Age','Height'])
# df3.show()
# df3.dropDuplicates().show()
df3.filter(df3['Age'] > 6 ).show()
df3.where(df3['Name'] == 'Alice' ).show()

print(df3.take(2))

def f(df3):
    print(df3['Name'])
df3.foreach(f)

df3.groupBy('Name','Age').agg(func.sum('Age')).show()
df3.groupBy().avg().show()
df3.printSchema()
print(df3.repartition(10).rdd.getNumPartitions())


df3.na.replace(5,35).show()
df3.na.replace(80,135).show()

df3.rollup("name", df3['age']).count().orderBy("name", "age").show()

print(df3.storageLevel)
df3.summary().show()

df3.summary('count','min','25%','75%','max').show()
df3.select('Name','Age').summary("count").show()

print(df3.toJSON().first())
print(list(df3.toLocalIterator()))

print(df3.toPandas())

df3.withColumn('Age',df3['Age']+5).show()


print(df3.groupBy().avg('age', 'height').collect())
df3.select('Name').orderBy(df3['Name'].asc()).show()
df3.select('Name').orderBy(df3['Name'].desc()).show()

df3.select('Name','Age' , df3['Age'].between(10,25)).show()

df3.filter(df3['Height'].isNotNull()).show()
df3.na.fill({'Name':"XYZ", "Age":10 ,"Height":101}).show()



