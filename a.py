from statistics import mean

from numpy.core.defchararray import lower
from pyspark.sql import SparkSession, Window



import pyspark.sql.functions as func
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType
from self import self

spark = SparkSession.builder.appName("Pyspark Query Task").getOrCreate()

df = spark.read.csv("/home/tibil/Chetan/Tasks/20191218.export.CSV", header=True , inferSchema=True)
# print(df.count())
# # print(df.show())
# # stmt =df.select('ActionGeo_CountryCode','EventCode').distinct().show()
# #print(stmt)
#
# stmt1= df.sort('SQLDATE').groupBy('SQLDATE').agg(func.count('SQLDATE').alias("#_of_events")).show()
# # st = df.select("ActionGeo_CountryCode","SQLDATE").distinct().select(stmt1["#_of_events"]).show()
#
# # print(stmt1)
# print("q1")
# q1 = df.select("ActionGeo_CountryCode","SQLDATE").sort(func.col("ActionGeo_CountryCode").desc()).show()
#
# # df3 = stmt1.join(q1, on=['SQLDATE'], how='inner')
# # df3.show()
# # stmt2 =df.groupby('ActionGeo_CountryCode').count('ActionGeo_CountryCode').select(func.col("count('ActionGeo_CountryCode')").alias('# of events'))
# # print(stmt2)
#
# #Create a report which shows Month-wise, ActionCountry_Code wise, # of events
# stmt2 = df.select("MonthYear","ActionGeo_CountryCode").distinct().show()
# join = df.groupBy('SQLDATE').agg(func.count('SQLDATE').alias("#_of_events")).show()
# # print(stmt2)
# # print(join)

# q1 = df.sort("ActionGeo_CountryCode").distinct().groupBy('ActionGeo_CountryCode').agg(func.count('ActionGeo_CountryCode').alias('# of events')).show()
# q2 = df.sort("MonthYear").groupBy('MonthYear','ActionGeo_CountryCode').agg(func.count('ActionGeo_CountryCode').alias("# of events")).show()
# q3 = df.sort('SQLDATE').distinct().groupBy("SQLDATE").agg(func.count('SQLDATE').alias('# of events'))
# # q3.sort(func.col('# of events').desc()).show()
# #
#
# q4 = df.sort('MonthYear').distinct().groupBy("MonthYear").agg(func.count('MonthYear').alias('# of events'))
# window = Window.orderBy("MonthYear")
# lagCol = func.lag(func.col("# of events"), 1).over(window)
# sum = q4.withColumn("Sum", lagCol).fillna(0)
# # sum.show()
#
# result = sum.withColumn('% change ',
#                         ((sum['# of events'] / sum['Sum'] * 100 ) - 100 ).cast(IntegerType())).fillna(0)
#
# result.show()

# res = result.withColumn('% change',func.concat(func.col('% change'),func.lit("%")))
# res.show()







# qq5= df.sort('ActionGeo_CountryCode').distinct().groupBy('ActionGeo_CountryCode').agg(func.count('ActionGeo_CountryCode').alias("Events")).show()
# ss = df.sort('MonthYear').distinct().groupBy('MonthYear','ActionGeo_CountryCode').count().filter((func.col('MonthYear') == '201912')).show()
# df.sort('MonthYear').distinct().groupBy('MonthYear','ActionGeo_CountryCode').agg(func.count("MonthYear") > '100').show()
# ce = df.groupBy('ActionGeo_CountryCode','MonthYear').count().filter(func.countDistinct('MonthYear') >= 100 ).show()


# q5 = df.groupBy('MonthYear').count().filter(func.col('MonthYear') == '201912').show()
# q6 = df.groupBy('Actor1Geo_CountryCode').count().filter(func.col('Actor1Geo_CountryCode') =='US').show()
# q7 = df.groupBy('EventCode').count().filter(func.col('EventCode') == '51').show()
# q8 = df.groupBy('QuadClass').count().filter(func.col('QuadClass') == '1').show()

# Q=df.groupBy('EventCode','MonthYear','Actor1Geo_CountryCode','QuadClass').count().\
#      filter((func.col('EventCode') =='51' )  & (func.col('Actor1Geo_CountryCode') =='US') & (func.col('MonthYear') =='201912') & (func.col('QuadClass') == '1')).show()

# df.groupBy('NumArticles','MonthYear').agg(func.mean('NumArticles')).show()
# df.sort('NumArticles').distinct().groupBy('NumArticles','MonthYear').agg(func.min('NumArticles')).show(1)

# df.sort('GLOBALEVENTID').distinct().groupBy('GLOBALEVENTID','SQLDATE').count().filter(func.col('SQLDATE')=='20181218').alias('# of events').show(2)
# df.sort("MonthYear").distinct().groupBy('ActionGeo_CountryCode','MonthYear','NumArticles').count().show(5)

#joins

# y1 = df.groupBy('ActionGeo_CountryCode','NumArticles','MonthYear').agg(func.count('Numarticles').alias('201812')).show(5)

# a1= df.sort('MonthYear').groupBy('ActionGeo_CountryCode','NumArticles').count().filter((func.col('MonthYear')=='201812').alias('201812')).show()
# r1 = df.select('ActionGeo_CountryCode').show(20)

# co =spark.read.csv("/home/tibil/Chetan/ip2location-country-information-basic/IP2LOCATION-COUNTRY-INFORMATION-BASIC/IP2LOCATION-COUNTRY-INFORMATION-BASIC.CSV", header=True , inferSchema=True)
# r2 = co.sort('Population').distinct().groupBy('country_code','capital','Population','ActionGeo_CountryCode').agg(func.count("Population").alias('# of events')).show(20)


# 3
# df.sort('MonthYear').distinct().groupBy('MonthYear','NumArticles').agg(func.sum('NumArticles').alias('sum of Articles')).show()
#
# df.agg(func.avg('NumArticles').alias('AVg of Articles')).show()


# df.agg(func.min('NumArticles')).show()
# df.agg(func.max('NumArticles')).show()
# print(df.head(1))

# DataQuality
# df.select('SOURCEURL').distinct().show()


# bb  = df.where(func.col('Actor1Name') == None)
# bb.drop().show()
# bb.drop(subset=['Actor1Name'])
# df.select("Actor1Name").show()
# df.dropna(how  = "any").show()

# df.select('Actor1EthnicCode').show()
# df.dropna(how = 'any').show()


# print(df.columns,df.dtypes)

#6
# df.groupBy('Actor2EthnicCode').count().orderBy(func.col("count"),ascending=False).fillna('idg').show()
# 7
# df.groupBy('GoldsteinScale','AvgTone').count().filter((func.col('GoldsteinScale') > 0) & (func.col('AvgTone') > 0 )).show()

# 8

# df.select("*" ,lower(func.col('Actor1Geo_Fullname'))).show()
# ll = df.select('Actor1Name')
# ll.withColumn('cc',func.initcap('Actor1Name')).show()

# df.fillna('99').show()

#11
# dq = df.withColumn("float",func.col('NumArticles').cast('float'))
# dq.select('NumArticles').show()


df.select('NumArticles','AvgTone').filter((func.col('NumArticles') > 100 ) & (func.col('AvgTone') > 10) & (func.col('AvgTone') < 25)).show()

df.select('Actor1Name').filter((func.col('Actor1Name').contains('YER')) | (func.col('Actor1Name').contains('COM'))).show()
df.agg(func.avg('NumArticles').alias('AVg of Articles')).show()

fill = df.select('EventCode').fillna('143')
fill.show()


