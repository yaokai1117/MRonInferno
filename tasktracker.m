TaskTracker : module {
	PATH : con "/usr/fyabc/tasktracker.dis";

	init : fn();

	TaskTrackerInfo : adt{

	}

	runMapperTask : fn(task : ref MRUtil->MapperTask);
	runAllReducerTask : fn(task : ref MRUtil->MapperTask , reducerTasks : list of MRUtil->ReducerTask);
	runReducerTask : fn(mapperTask : ref MRUtil->ReducerTask , reducerTask : ref MRUtil->ReducerTask);

	increaseReducerTaskAmount : fn();

	mapperSucceed : fn(task : ref MRUtil->MapperTask);
	mapperFailed : fn(task : ref MRUtil->MapperTask);
	reducerSucceed : fn(task : ref MRUtil->ReducerTask);
	reducerFailed : fn(task : ref MRUtil->ReducerTask);
	reducerFailedOnMapper : fn(reducerTask : ref MRUtil->ReducerTask , mapperTask : ref MRUtil->ReducerTask);

	heartbeat : fn();
};