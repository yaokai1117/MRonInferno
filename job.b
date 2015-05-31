implement Job;

include "job.m";
include "sys.m";
include "table.m";
include "mrutil.m";

MapperTask : import mrutil;
ReducerTask : import mrutil;

sys : Sys;
table : Table;
mrutil : MRUtil;

init()
{
	sys = load Sys Sys->PATH;
	table = load Table Table->PATH;
	mrutil = load MRUtil MRUtil->PATH;

	mrutil->init();
}

Job.getMapper(jb : self ref Job, id : int) : ref MRUtil->MapperTask
{
	return jb.mapperTasks.find(id);
}

Job.getReduce(jb : self ref Job, id : int) : ref MRUtil->ReducerTask
{
	return jb.reducerTasks.find(id);
}

Job.setTaskStatus(jb : self ref Job, id : int, status : int) : int
{
	task := jb.mapperTasks.find(id);
	if (task == nil)
		task = jb.reducerTasks.find(id);
	if (task == nil)
		return -1;
	task.status = status;
	return 0;
}

Job.getTaskStatus(jb : self ref Job, id : int, status : int) : int
{
	task := jb.mapperTasks.find(id);
	if (task == nil)
		task = jb.reducerTasks.find(id);
	if (task == nil)
		return -2;
	return task.status;
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
	if (mapper == nil)
		return -1;
	jb.reducerTasks.add(reducer.id, reducer);
	return 0;
}

Job.getStatus(jb : self ref Job) : int
{
	isPending := 0;
	mTaskArray := jb.mapperTasks.items;
	rTaskArray := jb.reducerTasks.items;
	mTask := ref MapperTask;
	rTask := ref ReducerTask;
	i : int;
	p : list of (int, MapperTask);
	q : list of (int, ReducerTask);
	for (i = 0; i < len mTaskArray; i++)
		for (p = mTaskArray[i]; p != nil; p = tl p) {
			(nil, mTask) = hd p;
			if (mTask.status == MRUtil->FAILED)
				return MRUtil->FAILED;
			if (mTask.status == MRUtil->PENDING)
				isPending = 1;
		}
	for (i = 0; i < len rTaskArray; i++)
		for (q = rTaskArray[i]; q != nil; q = tl q) {
			(nil, rTask) = hd q;
			if (rTask.status == MRUtil->FAILED)
				return MRUtil->FAILED;
			if (rTask.status == MRUtil->PENDING)
				isPending = 1;
		}
	if (isPending == 1)
		return MRUtil->PENDING;
	else 
		return MRUtil->SUCCESS;
}

Job.toString(jb : self ref Job) : string
{
	ret := sys->sprint("JobId: %d, status: %d, %s", jb.id, jb.getStatus(), jb.config.toString());
	return ret;
}
