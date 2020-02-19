from pyspark.sql import SparkSession

spark = SparkSession \
    .builder\
    .appName("Pyspark for Write and read data from sql") \
    .config("spark.Some.config.option" , "some-value") \
    .getOrCreate()

csvfile = spark.read.csv('/home/tibil/Downloads/Chetan/Fire_Incidents_Data.csv' , header =True , inferSchema =True)
csvfile.select('Incident Number','Address').distinct().show(10)


csvfile.write.format('jdbc').options(url = 'jdbc:mysql://localhost:3306/pysparkDB',
                                     driver = 'com.mysql.cj.jdbc.Driver',
                                     dbtable = 'District_list' ,
                                     user ='root' ,
                                     password ='MYSQL123').mode('append').save()

print("Success for writing operation!")

spark.read.format('jdbc')\
    .options(url ='jdbc:mysql://localhost:3306/pysparkDB')\
    .options(driver = 'com.mysql.jdbc.Driver')\
    .options(dbtable = 'District_list') \
    .options(user ='root')\
    .options(password ='MYSQL123').load().show()

print("Success of READ operation")