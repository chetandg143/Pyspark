import jtracker as jtracker
import self
from pyspark import SparkJobInfo, SparkStageInfo


def __init__(self , jtracker):
    self._jtracker = jtracker

#1 Active Job Ids
def getActiveJobsIds(self):
        return

sorted((list(self._jtracker.getActiveJobIds())))

#2 Active Stage Ids
def getActiveStageIds(self):
    return
sorted((list(self._jtracker.getActiveStageIds())))

#3 Job ids for group
def getJobIdsForGroup(self , jobGroup = None):
    return
list(self._jtracker.getJobIdsForGroup(self.jobGroup))

#4 Job information
def getJobInfo(self , jobId):
    job = self._jtracker.getJobInfo(jobId)
    if job is not None:
        return SparkJobInfo(jobId , job.stageIds() , str(job.status()))

#5 Stage information
def getStageInfo(self , stageId):
    stage = self._jtracker.getStageInfo(stageId)
    if stageId is not None:
        attrs = [getattr(stage , f)() for f in SparkStageInfo._fields[1:]]
        return SparkStageInfo(stageId , *attrs)