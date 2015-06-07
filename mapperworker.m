MapperWorker : module {
	PATH : con "/usr/yaokai/mapperworker.dis";

	init : fn(mapperTask : ref MRUtil->MapperTask);

	run : fn(mapperTask : ref MRUtil->MapperTask);

	collect : fn() : ref IOUtil->OutputCollector;

	saveToLocal : fn(mapperTask : ref MRUtil->MapperTask, collector : ref IOUtil->OutputCollector);
};
