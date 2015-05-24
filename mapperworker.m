MapperWorker : module {
	PATH : con "/usr/fyabc/mapperworker.dis";

	init : fn();

	#//There is a method that load the remote class(MR Instance).
	newMRInstance : fn() : ref MRUtil->MapReduce;

	run : fn();

	collect : fn() : ref MRUtil->OutPutCollector;

	saveToLocal : fn(collector : ref MRUtil->OutPutCollector);
};