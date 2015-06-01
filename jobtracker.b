implement JobTracker;

include "sys.m";
include "jobtracker.m";
include "mrutil.m";
include "tables.m";
include "logger.m";
include "jobs.m";

Job : import jobmodule;
JobConfig : import jobmodule;
MapperTask : import mrutil;
ReducerTask : import mrutil;
TaskTrackerInfo : import mrutil;
Table : import table;
Strhash : import table;

sys : Sys;
table : Tables;
mrutil : MRUtil;
jobmodule : Jobs;
logger : Logger;


#implementation of functions of module JobTracker 

jobs : ref Tables->Table[ref Job];
taskTrackers : ref Tables->Strhash[ref TaskTrackerInfo];


init()
{
	sys = load Sys Sys->PATH;
	table = load Tables Tables->PATH;
	mrutil = load MRUtil MRUtil->PATH;
	jobmodule = load Jobs Jobs->PATH;
	logger = load Logger Logger->PATH;

	jobmodule->init();

	mrutil->init();
	
	logger->init();
	logger->setFileName("log_jobtracker");

	jobs = Tables->Table[ref Job].new(100, nil);
	taskTrackers = Tables->Strhash[ref TaskTrackerInfo].new(100, nil);

}

submitJob(config : ref JobConfig) : int
{
	if (config == nil) {
		logger->log("SubmitJob failed, try to submit a nil job!", Logger->ERROR);
		logger->scrlog("SubmitJob failed, try to submit a nil job!", Logger->ERROR);
		return -1;
	}
	newjob := Job.new(config);
	jobs.add(newjob.id, newjob);
	logger->logInfo("Submit new job, id:" + string newjob.id + "name:" + newjob.config.name);
	logger->scrlogInfo("Submit new job, id:" + string newjob.id + "name:" + newjob.config.name);
	return 0;
}

startJob(id : int) : int
{
	job := jobs.find(id);
	if (job == nil) {
		logger->log("StartJob failed, no such job!", Logger->ERROR);
		logger->scrlog("StartJob failed, no such job!", Logger->ERROR);
		return -1;
	}
	produceMapper(job);	
	produceReducer(job);
	mapper : ref MRUtil->MapperTask;
	for (i := 0; i < len job.mapperTasks.items; i++)
		for (p := job.mapperTasks.items[i]; p != nil; p = tl p) {
			(nil, mapper) = hd p;
			shootMapper(mapper);
		}
	logger->logInfo("Start job:" + string id);
	logger->scrlogInfo("Start job:" + string id);
	return 0;
}

updateTaskTrackers(taskTracker : ref TaskTrackerInfo) : int
{
	if (taskTracker == nil) {
		logger->log("UpdateTaskTracker failed, new tracker is nil!", Logger->ERROR);
		logger->scrlog("UpdateTaskTracker failed, new tracker is nil!", Logger->ERROR);
		return -1;
	}
	oldTaskTracker := taskTrackers.find(taskTracker.addr + "!" + string taskTracker.port);
	if (oldTaskTracker == nil)
		taskTrackers.add(taskTracker.addr + string taskTracker.port, taskTracker);
	else {
		oldTaskTracker.addr = taskTracker.addr;
		oldTaskTracker.port = taskTracker.port;
		oldTaskTracker.mapperTaskNum = taskTracker.mapperTaskNum;
		oldTaskTracker.reducerTaskNum = taskTracker.reducerTaskNum;
	}
	logger->logInfo("UpdateTaskTracker: " + taskTracker.addr + string taskTracker.port + "!"); 
	logger->scrlogInfo("UpdateTaskTracker: " + taskTracker.addr + string taskTracker.port + "!"); 
	return 0;
}

produceMapper(job : ref Job)
{
}

produceReducer(job : ref Job)
{
}

shootMapper(mapper : ref MapperTask) : int 
{
	return 0;
}

mapperSucceed(jobId : int, taskId : int) : int
{
	return 0;
}

reducerSucceed(jobId : int, taskId : int) : int
{
	return 0;
}

mapperFailed(jobId : int, taskId : int) : int
{
	return 0;
}

reducerFailed(jobId : int, taskId : int) : int
{
	return 0;
}





