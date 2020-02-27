from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spar_demo") \
    .config("spark.mongodb.input.uri","mongodb://192.168.1.96/test.ask") \
    .config("spark.mongodb.output.uri","mongodb://192.168.1.96/test.ask").getOrCreate()

abc = spark.read.csv("/home/tibil/Chetan/Tasks/Fire_Department_Calls.csv", header=True)
abc.write.format("com.mongodb.spark.sql.DefaultSource").option("database","test").option("collection","ask").save()

