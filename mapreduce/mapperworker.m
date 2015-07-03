########################################
#
#	The MapperWorker module is the worker to do the mapperTask.
#	It run the user map function and save the result to local.
#	
#	@author Yang Fan(fyabc) 
#	@author Kai Yao(yaokai)
#	@author Guanji Gao(ggj)
#
########################################

MapperWorker : module {
	PATH : con "/appl/MR/mapreduce/mapperworker.dis";

	init : fn();

	run : fn(mapperTask : ref MRUtil->MapperTask);
};
