
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


spark.read.format("jdbc") \
    .options(url="jdbc:mysql://localhost:3306/pysparkDB") \
    .options(driver="com.mysql.jdbc.Driver") \
    .options(dbtable='employee') \
    .options(user="root") \
    .options(password="MYSQL123").load().show()

print("success")



