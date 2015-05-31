implement JobTracker;

include "sys.m";
include "jobtracker.m";
include "mrutil.m";
include "table.m";
include "logger.m";
include "job.m";

Job : import job;
JobConfig : import job;
MapperTask : import mrutil;
ReducerTask : import mrutil;
TaskTrackerInfo : import mrutil;

sys : Sys;
table : Table;
mrutil : MRUtil;
job : Job;
logger : Logger;


#implementation of functions of module JobTracker 

jobs : ref Table->Table[ref Job];
taskTrackers : ref Table->StrHash[ref TaskTrackerInfo];

maxJobId : int;

init()
{
	sys = load Sys Sys->PATH;
	table = load Table Table->PATH;
	mrutil = load MRUtil MRUtil->PATH;
	job = load Job Job->PATH;
	logger = load Logger Logger->PATH;

	bjob->init();

	mrutil->init();
	
	logger->init();
	logger->setFileName("log_jobtracker");

	jobs = Table->Table[ref Job].new(100, nil);
	taskTrackers = Table->StrHash[ref TaskTrackerInfo].new(100, nil);

}

submitJob(config : ref JobConfig) : int
{
	if (config == nil) {
		logger->log("SubmitJob failed, try to submit a nil job!", Logger->ERROR);
		logger->scrlog("SubmitJob failed, try to submit a nil job!", Logger->ERROR);
		return -1;
	}
	newjob := ref Job(maxJobId++, config, MRUtil->INIT, nil, nil);
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
	for (p := job.mapperTasks; p != nil; p = tl p) 
		shootMapper(hd p);
	logger->logInfo("Start job:" + string id);
	logger->scrlogInfo("Start job:" + string id);
	return 0;
}

updateTaskTrackers(taskTracker : ref TaskTrackerInfo) : int
{
	if (TaskTrackerInfo == nil) {
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
}

mapperSucceed(jobId : int, taskId : int) : int
{
}

reducerSucceed(jobId : int, taskId : int) : int
{
}

mapperFailed(jobId : int, taskId : int) : int
{
}

reducerFailed(jobId : int, taskId : int) : int
{
}





