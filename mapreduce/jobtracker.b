implement JobTracker;

include "sys.m";
include "jobtracker.m";
include "mrutil.m";
include "jobs.m";
include "ioutil.m";

include "tables.m";
include "lists.m";
include "../logger/logger.m";

Job : import jobmodule;
JobConfig : import jobmodule;
MapperTask : import mrutil;
ReducerTask : import mrutil;
TaskTrackerInfo : import mrutil;
TaskTracker : import mrutil;
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
lists : Lists;


#implementation of functions of module JobTracker 

MapperFileAddr : adt {
	items : list of (int, string);
};

INF : con 100000;

maxTaskId : int;

jobs : ref Tables->Table[ref Job];
taskTrackers : ref Tables->Strhash[ref TaskTracker];
m2rTable : ref Tables->Table[ref MapperFileAddr];

init()
{
	sys = load Sys Sys->PATH;
	table = load Tables Tables->PATH;
	lists = load Lists Lists->PATH;	
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
	taskTrackers = Tables->Strhash[ref TaskTracker].new(100, nil);
	m2rTable = Tables->Table[ref MapperFileAddr].new(100, nil);

	spawn checkTrackers();
}

checkTrackers()
{
	while (1) {
		for (i := 0; i < len taskTrackers.items; i++)
			for (p := taskTrackers.items[i]; p != nil; p = tl p){
				(nil, taskTracker) := hd p;
				if (taskTracker.info.isWorking == 1 && (sys->millisec() - taskTracker.info.timeStamp > 10000)){
					taskTracker.info.isWorking = 0;
					for (p := taskTracker.mappers; p != nil; p = tl p)
						mapperFailed(hd p);
					for (q := taskTracker.reducers; q != nil; q = tl q)
						reducerFailed(hd q);
				}
			}
		sys->sleep(5000);
	}
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

updateTaskTrackers(taskTrackerInfo : ref TaskTrackerInfo) : int
{
	if (taskTrackerInfo == nil) {
		logger->log("UpdateTaskTracker failed, new tracker is nil!", Logger->ERROR);
		logger->scrlog("UpdateTaskTracker failed, new tracker is nil!", Logger->ERROR);
		return -1;
	}
	oldTaskTracker := taskTrackers.find(taskTrackerInfo.addr + string taskTrackerInfo.port);
	if (oldTaskTracker == nil) {
		taskTrackerInfo.timeStamp = sys->millisec();
		taskTrackers.add(taskTrackerInfo.addr + string taskTrackerInfo.port, ref TaskTracker(taskTrackerInfo, nil, nil));
		logger->logInfo("UpdateTaskTracker: " + taskTrackerInfo.addr + "!" + string taskTrackerInfo.port + "!"); 
		logger->scrlogInfo("UpdateTaskTracker: " + taskTrackerInfo.addr + "!" + string taskTrackerInfo.port + "!"); 
	}
	
	else {
		oldTaskTracker.info.addr = taskTrackerInfo.addr;
		oldTaskTracker.info.port = taskTrackerInfo.port;
		oldTaskTracker.info.isWorking = taskTrackerInfo.isWorking;
		oldTaskTracker.info.timeStamp = sys->millisec();
	}
	return 0;
}

getTaskTracker() : ref TaskTracker
{
	ret : ref TaskTracker;
	minNum := INF;
	for (i := 0; i < len taskTrackers.items; i++)
		for (p := taskTrackers.items[i]; p != nil; p = tl p){
			(nil, taskTracker) := hd p;
			if (taskTracker.info.isWorking == 1 && taskTracker.info.mapperTaskNum + taskTracker.info.reducerTaskNum < minNum) {
				ret = taskTracker;
				minNum = taskTracker.info.mapperTaskNum + taskTracker.info.reducerTaskNum;
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
		task := ref MapperTask(maxTaskId++, job.id, MRUtil->PENDING, 1, taskTracker.info.addr, taskTracker.info.port, job.config.mrClassName, job.config.reducerAmount, job.config.combinable, hd p);
		taskTracker.info.mapperTaskNum++;
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
		task := ref ReducerTask(maxTaskId++, job.id, MRUtil->PENDING, 1, taskTracker.info.addr, taskTracker.info.port, job.config.mrClassName, job.config.mapperAmount, i, job.config.outputFile, job.config.outputRep, job.config.outputSize);
		taskTracker.info.reducerTaskNum++;
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

	taskTracker := taskTrackers.find(mapper.taskTrackerAddr + string mapper.taskTrackerPort);
	taskTracker.mappers = mapper :: taskTracker.mappers;

	msg := "shootMapper" + "@" + mrutil->mapper2msg(mapper);
	sys->fprint(conn.dfd, "%s", msg); 
	
	return 0;
}

shootReducer(reducer : ref ReducerTask, mapperFileAddr : string) : int
{
	(ok, conn) := sys->dial("tcp!" + reducer.taskTrackerAddr + "!" + string reducer.taskTrackerPort, nil);
	if (ok < 0) {
		logger->log("ShootReducer failed, dial failed!", Logger->ERROR);
		logger->scrlog("ShootReducer failed, dial failed!", Logger->ERROR);
		return -1;
	}

	taskTracker := taskTrackers.find(reducer.taskTrackerAddr + string reducer.taskTrackerPort);
	taskTracker.reducers = reducer :: taskTracker.reducers;

	msg := "shootReducer"  + "@" + mapperFileAddr+ "@" + mrutil->reducer2msg(reducer);
	sys->fprint(conn.dfd, "%s", msg);

	return 0;
}

mapperSucceed(task : ref MapperTask, mapperFileAddr : string) : int
{
	if (!isRightMapper(task))
		return -1;

	job := jobs.find(task.jobId);
	localTask := job.getMapper(task.id);
	localTask.status = MRUtil->SUCCESS;
	
	taskTracker := taskTrackers.find(localTask.taskTrackerAddr + string localTask.taskTrackerPort);
	taskTracker.info.mapperTaskNum--;
	taskTracker.mappers = lists->delete(localTask, taskTracker.mappers);

	m2rRecord := m2rTable.find(task.jobId);
	if (m2rRecord == nil) 
		m2rTable.add(task.jobId, ref MapperFileAddr((task.id, mapperFileAddr) :: nil));
	else
		m2rRecord.items = (task.id, mapperFileAddr) :: m2rRecord.items;

	logger->logInfo("MapperTask " + string task.id + " from Job " + string task.jobId + " Succeed!!!");
	logger->scrlogInfo("MapperTask " + string task.id + " from Job " + string task.jobId + " Succeed!!!");

	if (m2rRecord != nil && len m2rRecord.items == job.config.mapperAmount) {
		produceReducer(job);
		for (i := 0; i < len job.reducerTasks.items; i++)
			for (p := job.reducerTasks.items[i]; p != nil; p = tl p) {
				(nil, reducer) := hd p;
				for (q := m2rRecord.items; q != nil; q = tl q) {
					(nil, mapperFileAddr) = hd q;
					shootReducer(reducer, mapperFileAddr);
				}
			}
	}
	
	return 0;
}

reducerSucceed(task : ref ReducerTask) : int
{
	if (!isRightReducer(task))
		return -1;

	job := jobs.find(task.jobId);
	localTask := job.getReducer(task.id);
	localTask.status = MRUtil->SUCCESS;

	taskTracker := taskTrackers.find(localTask.taskTrackerAddr + string localTask.taskTrackerPort);
	taskTracker.info.reducerTaskNum--;
	taskTracker.reducers = lists->delete(localTask, taskTracker.reducers);

	logger->logInfo("ReducerTask " + string task.id + " from Job " + string task.jobId + " Succeed!!!");
	logger->scrlogInfo("ReducerTask " + string task.id + " from Job " + string task.jobId + " Succeed!!!");

	if (job.getStatus() == MRUtil->SUCCESS) {
		logger->logInfo("Job " + string job.id + " Succeed!!!");
		logger->scrlogInfo("Job " + string job.id + " Succeed!!!");
		jobs.del(job.id);
	}

	return 0;
}

mapperFailed(task : ref MapperTask) : int
{
	if (!isRightMapper(task))
		return -1;

	job := jobs.find(task.jobId);
	localTask := job.getMapper(task.id);
	taskTracker := taskTrackers.find(localTask.taskTrackerAddr + string localTask.taskTrackerPort);

	# must be localTask here
	if (localTask.attemptCount > job.config.maxAttemptNum || changeMapperTaskTracker(localTask) != 0) {
		logger->log("Mapper task " + string localTask.id + " failed! Job: " + string job.id, Logger->ERROR);
		logger->scrlog("Mapper task " + string localTask.id + " failed! Job : " + string job.id, Logger->ERROR);
		localTask.status = MRUtil->FAILED;	
		taskTracker.info.mapperTaskNum--;
		taskTracker.mappers = lists->delete(localTask, taskTracker.mappers);
		logger->log("Job " + string job.id + " Failed!!! on mapper " + string task.id, Logger->ERROR);
		logger->scrlog("Job " + string job.id + " Failed!!! on mapper " + string task.id, Logger->ERROR);
	}

	else {
		logger->log("Mapper task " + string localTask.id + "change taskTracker!, the origin tasktracker was " + localTask.taskTrackerAddr + string localTask.taskTrackerPort, Logger->WARN);		
		logger->scrlog("Mapper task " + string localTask.id + "change taskTracker!, the origin tasktracker was " + localTask.taskTrackerAddr + string localTask.taskTrackerPort, Logger->WARN);		
		localTask.status = MRUtil->PENDING;
		localTask.attemptCount++;

		m2rRecord := m2rTable.find(localTask.jobId);
		newAddrList : list of (int, string);
		for (p := m2rRecord.items; p != nil; p = tl p) {
			(mapperId, mapperFileAddr) := hd p;
			if (mapperId != task.id)
				newAddrList = (mapperId, mapperFileAddr) :: newAddrList;
		}
		m2rRecord.items = newAddrList;

		shootMapper(localTask);
	}
	return 0;
}

reducerFailed(task : ref ReducerTask) : int
{
	if (!isRightReducer(task))
		return -1;

	job := jobs.find(task.jobId);
	localTask := job.getReducer(task.id);
	taskTracker := taskTrackers.find(task.taskTrackerAddr + string task.taskTrackerPort);

	if (localTask.attemptCount > job.config.maxAttemptNum || changeReducerTaskTracker(localTask) != 0) {
		logger->log("Reducer task " + string localTask.id + " failed! Job: " + string job.id, Logger->ERROR);
		logger->scrlog("Reducer task " + string localTask.id + " failed! Job : " + string job.id, Logger->ERROR);
		taskTracker.info.reducerTaskNum--;
		taskTracker.reducers = lists->delete(localTask, taskTracker.reducers);
		localTask.status = MRUtil->FAILED;	
		logger->log("Job " + string job.id + " Failed!!! on reducer " + string task.id, Logger->ERROR);
		logger->scrlog("Job " + string job.id + " Failed!!! on reducer " + string task.id, Logger->ERROR);
	}

	else {
		logger->log("Reducer task " + string localTask.id + "change taskTracker!, the origin tasktracker was " + localTask.taskTrackerAddr + string localTask.taskTrackerPort, Logger->WARN);		
		logger->scrlog("Reducer task " + string localTask.id + "change taskTracker!, the origin tasktracker was " + localTask.taskTrackerAddr + string localTask.taskTrackerPort, Logger->WARN);		
		localTask.status = MRUtil->PENDING;
		localTask.attemptCount++;
		m2rRecord := m2rTable.find(task.jobId);
		if (m2rRecord != nil)
			for (p := m2rRecord.items; p != nil; p = tl p){
				(nil, mapperFileAddr) := hd p;
				shootReducer(localTask, mapperFileAddr);
			}
	}
	return 0;
}

reducerFailedonMapper(reducer : ref ReducerTask, mapperFileAddr : string) : int
{
	if (!isRightReducer(reducer))
		return -1;

	m2rRecord := m2rTable.find(reducer.jobId);
	newAddrList : list of (int, string);
	wrongMapperId := -1;
	for (p := m2rRecord.items; p != nil; p = tl p) {
		(mapperId, localMapperFileAddr) := hd p;
		if (localMapperFileAddr != mapperFileAddr)
			newAddrList = (mapperId, localMapperFileAddr) :: newAddrList;
		else 
			wrongMapperId = mapperId;
	}
	m2rRecord.items = newAddrList;

	job := jobs.find(reducer.jobId);
	localTask := job.reducerTasks.find(reducer.id);
	taskTracker := taskTrackers.find(reducer.taskTrackerAddr + string reducer.taskTrackerPort);

	if (wrongMapperId != -1) {
		wrongMapper := job.mapperTasks.find(wrongMapperId);
		if (wrongMapper.attemptCount > job.config.maxAttemptNum || changeMapperTaskTracker(wrongMapper) != 0) {
			logger->log("Reducer task " + string reducer.id + " failed on mapper " + string wrongMapperId + " Job: " + string job.id, Logger->ERROR);
			logger->scrlog("Reducer task " + string reducer.id + " failed on mapper " + string wrongMapperId + " Job : " + string job.id, Logger->ERROR);
			taskTracker.info.reducerTaskNum--;
			taskTracker.reducers = lists->delete(localTask, taskTracker.reducers);
			localTask.status = MRUtil->FAILED;	
			logger->log("Job " + string job.id + " Failed!!! on reducer " + string reducer.id, Logger->ERROR);
			logger->scrlog("Job " + string job.id + " Failed!!! on reducer " + string reducer.id, Logger->ERROR);
		}
	}

	logger->log("Reducer task " + string localTask.id + "shoot again!" , Logger->WARN);		
	logger->scrlog("Reducer task " + string localTask.id + "shoot again!", Logger->WARN);		
	localTask.status = MRUtil->PENDING;
	for (p = m2rRecord.items; p != nil; p = tl p){
		(nil, mapperFileAddr) := hd p;
		shootReducer(reducer, mapperFileAddr);
	}

	return 0;
}

changeMapperTaskTracker(task : ref MRUtil->MapperTask) : int
{
	oldTaskTracker := taskTrackers.find(task.taskTrackerAddr + string task.taskTrackerPort);
	taskTracker := getTaskTracker();
	job := jobs.find(task.jobId);
	if (taskTracker == nil) {
		job.status = MRUtil->FAILED;
		logger->log("Change mapper taskTracker failed! No available taskTracker!", Logger->ERROR);
		logger->scrlog("Change mapper taskTracker failed! No available taskTracker!", Logger->ERROR);
		return -1;
	}

	taskTracker.info.mapperTaskNum++;
	oldTaskTracker.info.mapperTaskNum--;

	oldTaskTracker.mappers = lists->delete(task, oldTaskTracker.mappers);

	task.taskTrackerAddr = taskTracker.info.addr;
	task.taskTrackerPort = taskTracker.info.port;
	return 0;
}

changeReducerTaskTracker(task : ref MRUtil->ReducerTask) : int
{
	oldTaskTracker := taskTrackers.find(task.taskTrackerAddr + string task.taskTrackerPort);
	taskTracker := getTaskTracker();
	job := jobs.find(task.jobId);
	if (taskTracker == nil) {
		job.status = MRUtil->FAILED;
		logger->log("Change reducer taskTracker failed! No available taskTracker!", Logger->ERROR);
		logger->scrlog("Change reducer taskTracker failed! No available taskTracker!", Logger->ERROR);
		return -1;
	}

	taskTracker.info.reducerTaskNum++;
	oldTaskTracker.info.reducerTaskNum--;

	oldTaskTracker.reducers = lists->delete(task, oldTaskTracker.reducers);

	task.taskTrackerAddr = taskTracker.info.addr;
	task.taskTrackerPort = taskTracker.info.port;
	
	return 0;
}

isRightMapper(task : ref MapperTask) : int
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

isRightReducer(task : ref ReducerTask) : int
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



