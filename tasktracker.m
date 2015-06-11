TaskTracker : module {
	PATH : con "/usr/fyabc/tasktracker.dis";

	init : fn();

	runMapperTask : fn(mapper : ref MRUtil->MapperTask) : int;
	runReducerTask : fn(mapper : ref MRUtil->ReducerTask , reducer : ref MRUtil->ReducerTask) : int;

	increaseReducerTaskAmount : fn();

	mapperSucceed : fn(task : ref MRUtil->MapperTask);
	mapperFailed : fn(task : ref MRUtil->MapperTask);
	reducerSucceed : fn(task : ref MRUtil->ReducerTask);
	reducerFailed : fn(task : ref MRUtil->ReducerTask);

	heartbeat : fn();
};
