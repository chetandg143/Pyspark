from pyspark import SparkContext

"""logFile = "/home/tibil/Downloads/spark-2.4.4-bin-hadoop2.7/README.md"  # Should be some file on your system
sc = SparkContext("local", "Basics")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'c' in s).count()
numBs = logData.filter(lambda s: 'd' in s).count()
print("Lines with c: %s, lines with d: %s" % (numAs, numBs))"""


logFile1 = "/home/tibil/Downloads/spark-2.4.4-bin-hadoop2.7/README.md"
#sc =SparkContext("local", "Basics")
sc =SparkContext("local", "Basics")

logData1 = sc.textFile(logFile1).cache()
error = logData1.filter(lambda line : "ERROR" in line).count()

numCs =logData1.filter(lambda v: 'y' in v).count()
numDs = logData1.filter(lambda v: 'z' in v).count()

print("Lines with y : %s , Lines with z: %s" % (numCs,numDs))


#sc =SparkContext("local", "Basics")
