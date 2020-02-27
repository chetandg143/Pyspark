from pyspark import rdd
from pyspark.sql import Row

from Pysql import spark

Person = Row('name', 'age')

person = rdd.map(lambda r: Person(*r))

df2 = spark.createDataFrame(person)

df2.collect()
[Row(name='Alice', age=1)]
