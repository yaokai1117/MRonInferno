implement Jobs;

include "jobs.m";
include "sys.m";
include "tables.m";
include "mrutil.m";
include "ioutil.m";

MapperTask : import mrutil;
ReducerTask : import mrutil;
Table : import table;

sys : Sys;
table : Tables;
mrutil : MRUtil;

maxJobId : int;

init()
{
	sys = load Sys Sys->PATH;
	table = load Tables Tables->PATH;
	mrutil = load MRUtil MRUtil->PATH;

	mrutil->init();
}

Job.new(jc : ref JobConfig) : ref Job
{
	jb := ref Job(maxJobId++, jc, 2, nil, nil);
	jb.mapperTasks = Tables->Table[ref MRUtil->MapperTask].new(100, nil);
	jb.reducerTasks = Tables->Table[ref MRUtil->ReducerTask].new(100, nil);
	return jb;
}

Job.getMapper(jb : self ref Job, id : int) : ref MRUtil->MapperTask
{
	return jb.mapperTasks.find(id);
}

Job.getReducer(jb : self ref Job, id : int) : ref MRUtil->ReducerTask
{
	return jb.reducerTasks.find(id);
}

Job.setTaskStatus(jb : self ref Job, id : int, status : int) : int
{
	isMap := 1;
	task1 := jb.mapperTasks.find(id);
	task2 : ref ReducerTask;

	if (task1 == nil) {
		task2 = jb.reducerTasks.find(id);
		isMap = 0;
	}
	if (isMap == 0 && task2 == nil)
		return -1;
	
	if (isMap)
		task1.status = status;
	else
		task2.status = status;
	return 0;
}

Job.getTaskStatus(jb : self ref Job, id : int) : int
{
	isMap := 1;
	task1 := jb.mapperTasks.find(id);
	task2 : ref ReducerTask;

	if (task1 == nil) {
		task2 = jb.reducerTasks.find(id);
		isMap = 0;
	}
	if (isMap == 0 && task2 == nil)
		return -2;

	if (isMap)
		return task1.status;
	else
		return task2.status;
}


Job.addMapper(jb : self ref Job, mapper : ref MRUtil->MapperTask) : int
{
	if (mapper == nil)
		return -1;
	jb.mapperTasks.add(mapper.id, mapper);
	return 0;
}

Job.addReducer(jb : self ref Job, reducer : ref MRUtil->ReducerTask) : int
{
	if (reducer == nil)
		return -1;
	jb.reducerTasks.add(reducer.id, reducer);
	return 0;
}

Job.getStatus(jb : self ref Job) : int
{
	if (jb.status == MRUtil->INIT)
		return MRUtil->INIT;
	isPending := 0;
	mTaskArray := jb.mapperTasks.items;
	rTaskArray := jb.reducerTasks.items;
	mTask := ref MapperTask;
	rTask := ref ReducerTask;
	i : int;
	p : list of (int, ref MapperTask);
	q : list of (int, ref ReducerTask);
	for (i = 0; i < len mTaskArray; i++)
		for (p = mTaskArray[i]; p != nil; p = tl p) {
			(nil, mTask) = hd p;
			if (mTask.status == MRUtil->FAILED) {
				jb.status = MRUtil->FAILED;
				return MRUtil->FAILED;
			}
			if (mTask.status == MRUtil->PENDING)
				isPending = 1;
		}
	for (i = 0; i < len rTaskArray; i++)
		for (q = rTaskArray[i]; q != nil; q = tl q) {
			(nil, rTask) = hd q;
			if (rTask.status == MRUtil->FAILED) {
				jb.status = MRUtil->FAILED;
				return MRUtil->FAILED;
			}
			if (rTask.status == MRUtil->PENDING)
				isPending = 1;
		}
	if (isPending == 1) {
		jb.status = MRUtil->PENDING;
		return MRUtil->PENDING;
	}
	else {
		jb.status = MRUtil->SUCCESS;
		return MRUtil->SUCCESS;
	}
}

Job.toString(jb : self ref Job) : string
{
	ret := sys->sprint("JobId: %d, status: %d, %s", jb.id, jb.getStatus(), jb.config.toString());
	return ret;
}

JobConfig.toString(jc : self ref JobConfig) : string
{
	ret := sys->sprint("name: %s, mapperAmount: %d, reducerAmount: %d\n", jc.name, jc.mapperAmount, jc.reducerAmount);
	return ret;
}





