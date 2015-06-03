implement JobTracker;

include "sys.m";
include "jobtracker.m";
include "mrutil.m";
include "jobs.m";
include "ioutil.m";

include "tables.m";
include "logger.m";

Job : import jobmodule;
JobConfig : import jobmodule;
MapperTask : import mrutil;
ReducerTask : import mrutil;
TaskTrackerInfo : import mrutil;
FileBlock : import ioutil;
Table : import table;
Strhash : import table;
Connection : import sys;

sys : Sys;
table : Tables;
mrutil : MRUtil;
jobmodule : Jobs;
logger : Logger;
ioutil : IOUtil;


#implementation of functions of module JobTracker 

INF : con 100000;

maxTaskId : int;

jobs : ref Tables->Table[ref Job];
taskTrackers : ref Tables->Strhash[ref TaskTrackerInfo];


init()
{
	sys = load Sys Sys->PATH;
	table = load Tables Tables->PATH;
	mrutil = load MRUtil MRUtil->PATH;
	jobmodule = load Jobs Jobs->PATH;
	logger = load Logger Logger->PATH;
	ioutil = load IOUtil IOUtil->PATH;

	jobmodule->init();

	mrutil->init();
	ioutil->init();
	
	logger->init();
	logger->setFileName("log_jobtracker");

	maxTaskId = 0;
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
	if (produceMapper(job) != 0)
		return -1;	
	if (produceReducer(job) != 0)
		return -1;;
	mapper : ref MRUtil->MapperTask;
	for (i := 0; i < len job.mapperTasks.items; i++)
		for (p := job.mapperTasks.items[i]; p != nil; p = tl p) {
			(nil, mapper) = hd p;
			shootMapper(mapper);
		}
	job.status = MRUtil->PENDING;
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

getTaskTracker() : ref TaskTrackerInfo
{
	ret : ref TaskTrackerInfo;
	minNum := INF;
	for (i := 0; i < len taskTrackers.items; i++)
		for (p := taskTrackers.items[i]; p != nil; p = tl p){
			(nil, tt) := hd p;
			if (tt.mapperTaskNum + tt.reducerTaskNum < minNum) {
				ret = tt;
				minNum = tt.mapperTaskNum + tt.reducerTaskNum;
			}
		}
	if (minNum == INF) {
		logger->log("GetTaskTracker failed: no tasktracker available!", Logger->ERROR);
		logger->scrlog("GetTaskTracker failed: no tasktracker available!", Logger->ERROR);
		return nil;
	}
	return ret;
}

produceMapper(job : ref Job) : int
{
	fileName := job.config.inputFile;
	mapperAmount := job.config.mapperAmount;
	fileBlocks := ioutil->split(fileName, mapperAmount);
	job.config.mapperAmount = mapperAmount = len fileBlocks;
	for (p := fileBlocks; p != nil; p = tl p) {
		taskTracker := getTaskTracker();
		if (taskTracker == nil) {
			job.status = MRUtil->FAILED;
			logger->log("Produce mapper failed! No available taskTracker!", Logger->ERROR);
			logger->scrlog("Produce mapper failed! No available taskTracker!", Logger->ERROR);
			return -1;
		}
		task := ref MapperTask(maxTaskId++, job.id, MRUtil->PENDING, 1, taskTracker.addr, taskTracker.port, job.config.mrClassName, hd p, job.config.reducerAmount);
		job.mapperTasks.add(task.id, task);
	}
	return 0;
}

produceReducer(job : ref Job) : int
{
	reducerAmount := job.config.reducerAmount;
	for (i := 0; i < reducerAmount; i++) {
		taskTracker := getTaskTracker();
		if (taskTracker == nil) {
			job.status = MRUtil->FAILED;
			logger->log("Produce reducer failed! No available taskTracker!", Logger->ERROR);
			logger->scrlog("Produce reducer failed! No available taskTracker!", Logger->ERROR);
			return -1;
		}
		task := ref ReducerTask(maxTaskId++, job.id, MRUtil->PENDING, 1, taskTracker.addr, taskTracker.port, job.config.mrClassName, job.config.outputFile, job.config.outputRep, job.config.outputSize, job.config.mapperAmount, i);
		job.reducerTasks.add(task.id, task);
	}
	return 0;
}

### debug
getJob(id : int) : ref Job
{
	return jobs.find(id);
}
###

shootMapper(mapper : ref MapperTask) : int 
{
	(ok, conn) := sys->dial("tcp!" + mapper.taskTrackerAddr + "!" + string mapper.taskTrackerPort, nil);
	if (ok < 0) {
		logger->log("ShootMapper failed, dial failed!", Logger->ERROR);
		logger->scrlog("ShootMapper failed, dial failed!", Logger->ERROR);
		return -1;
	}

	msg := "mapper@" + string mapper.id + "@" + string mapper.jobId +
			"@" + string mapper.status + "@" + string mapper.attemptCount + 
			"@" + mapper.mrClassName + 
			"@" + mapper.inputFileBlock.fileName + "@" + string mapper.inputFileBlock.offset + "@" + string mapper.inputFileBlock.size;
	sys->fprint(conn.dfd, "%s", msg);

	buf := array [Sys->ATOMICIO] of byte;
	length := sys->read(conn.dfd, buf, len buf);
	ok = int string buf[:1];
	if (length == 0 || ok != 0) {
		logger->log("ShootMapper failed, feedback error!", Logger->ERROR);
		logger->scrlog("ShootMapper failed, feedback error!", Logger->ERROR);
		return -1;
	}	

	return 0;
}

shootReducer(reducer : ref ReducerTask, mapperFilePort : int) : int
{
	(ok, conn) := sys->dial("tcp!" + reducer.taskTrackerAddr + "!" + string reducer.taskTrackerPort, nil);
	if (ok < 0) {
		logger->log("ShootReducer failed, dial failed!", Logger->ERROR);
		logger->scrlog("ShootReducer failed, dial failed!", Logger->ERROR);
		return -1;
	}

	msg := "reducer@" + string reducer.id + "@" + string reducer.jobId + 
			 "@" + string reducer.status + "@" + string reducer.attemptCount + 
			 "@" + reducer.mrClassName + 
			 "@" + reducer.outputFile + "@" + string reducer.outputRep + "@" + string reducer.outputSize + "@" + string mapperFilePort;
	sys->fprint(conn.dfd, "%s", msg);

	buf := array [Sys->ATOMICIO] of byte;
	length := sys->read(conn.dfd, buf, len buf);
	ok = int string buf[:1];
	if (length == 0 || ok != 0) {
		logger->log("ShootReducer failed, feedback error!", Logger->ERROR);
		logger->scrlog("ShootReducer failed, feedback error!", Logger->ERROR);
		return -1;
	}

	return 0;
}

mapperSucceed(task : ref MapperTask, mapperFilePort : int) : int
{
	return 0;
}

reducerSucceed(task : ref ReducerTask) : int
{
	return 0;
}

mapperFailed(task : ref MapperTask) : int
{
	return 0;
}

reducerFailed(task : ref ReducerTask) : int
{
	return 0;
}

isRightMapper(task : MapperTask) : int
{
	job := jobs.find(task.jobId);
	if (job == nil)
		return 0;
	if (job.getStatus() != MRUtil->PENDING)
		return 0;

	localTask := job.getMapper(task.id);
	if (localTask == nil)
		return 0;
	if (localTask.status != MRUtil->PENDING)
		return 0;
	if ((localTask.taskTrackerAddr + string localTask.taskTrackerPort)
			!= (task.taskTrackerAddr + string task.taskTrackerPort))
		return 0;
	if (localTask.attemptCount != task.attemptCount)
		return 0;

	return 1;
}

isRightReducer(task : ReducerTask) : int
{
	job := jobs.find(task.jobId);
	if (job == nil)
		return 0;
	if (job.getStatus() != MRUtil->PENDING)
		return 0;

	localTask := job.getReducer(task.id);
	if (localTask == nil)
		return 0;
	if (localTask.status != MRUtil->PENDING)
		return 0;
	if ((localTask.taskTrackerAddr + string localTask.taskTrackerPort)
			!= (task.taskTrackerAddr + string task.taskTrackerPort))
		return 0;
	if (localTask.attemptCount != task.attemptCount)
		return 0;

	return 1;
}



