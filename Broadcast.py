
from pyspark import SparkContext

sc = SparkContext("local" , "Broadcast")
words_new =sc.broadcast(["Rahul","Dawan" , "Virat" , "Shreyas " ," Pant"])

data =words_new.value
print("stored data --> %s " % (data))

elem = words_new.value[3]
print("printing particular element in RDD --> %s " % (elem))

