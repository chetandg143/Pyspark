import pyspark
from pyspark.python.pyspark.shell import spark

f1 = spark.read.format('csv').options(header='true', inferSchema='true').load('/home/tibil/Downloads/Chetan/Fire_Department_Calls.csv')

#Find out the distinct values present in the field Call Type.
res = f1.select('Call Type').distinct().show()
print("result --> %s " % res)

#2 Top 10 Call Types by the number of incidents reported.
print(f1.groupBy('Call Type').count().orderBy('count',ascending =False).show())
print(f1.groupBy('Call Type').count().orderBy('count').show())

# 3 What are the distinct number of years in this dataset for which fire incidents were reported. Please use Call Date field for this operation.
print(f1.select('Call Date', 'Call Type').distinct().filter((f1['Call Type']).endswith('Fire')).show())


#Fire incident.csv
df =spark.read.format('csv').options(header='true', inferSchema='true').load('/home/tibil/Downloads/Chetan/Fire_Incidents_Data.csv')

#4  Find the top 10 Call Type Group (Fire_Department_Calls dataset) for the incidents that were reported from Nob Hill district (Fire_Incidents_Data.csv, field name: Neighborhood  District). You need to join these data sets on the Incident Number field

print(df.select('Incident Number', 'Neighborhood  District').distinct().filter((df["Neighborhood  District"]).contains('Nob Hill')).show(10))

df2 =df.select('Incident Number', 'Neighborhood  District').distinct().filter((df["Neighborhood  District"]).contains('Nob Hill'))

res = f1.join(df, on=['Incident Number'])
print(res.select('Call Type Group','Neighborhood  District','Incident Number').show(10))