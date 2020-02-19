
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("Spark.some.config.option","Some-value") \
    .getOrCreate()

# df=spark.read.csv("//home/tibil/Downloads/Fire_Department_Calls.csv", inferSchema=True, header=True)
# df1=df.select('Call Type').distinct()

spark.read.format('jdbc').options(url='jdbc:mysql://localhost:3306/pysparkDB',
                                 driver='com.mysql.jdbc.Driver',
                                 dbtable="employee",
                                 user='root',
                                 password='MYSQL123').load().show()
print("sucess")