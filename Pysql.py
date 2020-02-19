
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df =spark.read.csv('/home/tibil/Downloads/Chetan/Fire_Department_Calls.csv' ,inferSchema=True , header=True)
df1= df.select('Call Type').distinct()

#
# df1.write.format('jdbc').options(url ='jdbc:mysql://localhost:3306/pysparkDB',
#                                  driver="com.mysql.cj.jdbc.Driver",
#                                  dbtable ="Example",
#                                  user ='root' ,
#                                  password ='MYSQL123').mode('append').save()

spark.read.format("jdbc")\
.options(url="jdbc:mysql://localhost:3306/pysparkDB") \
 .options(driver="com.mysql.jdbc.Driver") \
 .options(dbtable='Example') \
 .options(user="root") \
 .options(password="MYSQL123").load().show()

print("success")



