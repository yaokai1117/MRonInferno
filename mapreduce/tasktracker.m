TaskTracker : module {
	PATH : con "/appl/MR/mapreduce/tasktracker.dis";

	init : fn();

	runMapperTask : fn(mapper : ref MRUtil->MapperTask) : int;
	runReducerTask : fn(mapperFileAddr : string, reducer : ref MRUtil->ReducerTask) : (int, string);
};
