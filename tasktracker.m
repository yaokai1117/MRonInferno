TaskTracker : module {
	PATH : con "/usr/yaokai/tasktracker.dis";

	init : fn();

	runMapperTask : fn(mapper : ref MRUtil->MapperTask) : int;
	runReducerTask : fn(mapperFileAddr : string, reducer : ref MRUtil->ReducerTask) : int;
};
