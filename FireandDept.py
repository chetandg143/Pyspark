from pyspark.python.pyspark.shell import spark

f1 = spark.read.format('csv').options(header ='true',inferSchema='true').load('/home/tibil/Downloads/Chetan/Fire_Department_Calls.csv')

#1 Find out the distinct values present in the field Call Type.

print(f1.select('Call Type').distinct().show(10))

#2 Top 10 Call Types by the number of incidents reported.

print(f1.select('Call Type').groupBy('Call TYpe').count().orderBy('count', ascending = False).show(10))

#3 What are the distinct number of years in this dataset for which fire incidents were reported. Please use Call Date field for this operation.

print(f1.select('Call Date' , 'Call Type').distinct().filter((f1['Call Type']).endswith('Fire')).show())


#Find the top 10 Call Type Group (Fire_Department_Calls dataset) for the incidents that were reported from Nob Hill district (Fire_Incidents_Data.csv, field name: Neighborhood  District).
# You need to join these data sets on the Incident Number field

f2 =spark.read.format('csv').options(header='true', inferschema ='true').load('/home/tibil/Downloads/Chetan/Fire_Incidents_Data.csv')


#print(f2.select('Incident Number','Neighborhood  District').distinct().filter((f2['Neighborhood  District']).contains('Nob Hill')).show())
f3 = f1.join(f2, on= ['Incident Number'])
print(f3.select('Call Type Group' , 'Neighborhood  District').show(10))

f3.close()
