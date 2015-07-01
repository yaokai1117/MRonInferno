MapperWorker : module {
	PATH : con "/appl/MR/mapreduce/mapperworker.dis";

	init : fn();

	run : fn(mapperTask : ref MRUtil->MapperTask);
};
