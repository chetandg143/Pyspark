import  pyspark
from pyspark import SparkContext ,SparkConf

import pyspark

sc = SparkContext( "local" , "Storage")
rd1 = sc.parallelize([1,2])
rd1.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2 )
rd1.getStorageLevel()
print(rd1.getStorageLevel())

rd2 =sc.parallelize([1,2])
rd2.persist(pyspark.StorageLevel.DISK_ONLY )
print(rd2.getStorageLevel())

__all__ = ["StorageLevel"]
class StorageLevel:
    def __init__(self , useDisk , useMemory , deserialized , replication =1):
        self.useDisk = useDisk
        self.useMemory = useMemory
        self.deserialized = deserialized
        self.replication = replication
StorageLevel.DISK_ONLY = StorageLevel(True , False , False)
StorageLevel.DISK_ONLY_2 = StorageLevel(True , False , False , 2)
StorageLevel.MEMORY_ONLY = StorageLevel(True , False ,False )
StorageLevel.MEMORY_ONLY_2 = StorageLevel(False ,True ,True , 2 )
StorageLevel.MEMORY_ONLY_SER = StorageLevel(False , True , False )
StorageLevel.MEMORY_ONLY_SER_2 = StorageLevel(False , True , False , 2 )
StorageLevel.MEMORY_AND_DISK = StorageLevel(True , True , True )
StorageLevel.MEMORY_AND_DISK_2 = StorageLevel(True , True , True , 2)
StorageLevel.MEMORY_AND_SER = StorageLevel(True , True , False )
StorageLevel.MEMORY_ONLY_SER_2 = StorageLevel(True , True , False , 2 )

