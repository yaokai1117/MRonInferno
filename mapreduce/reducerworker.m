########################################
#
#	The ReducerWorker module is the worker to do the reducerTask.
#	It gets its partitions of files from each mapper,and merge them outside the memory.
#	Then reducerworker will run the user reduce function, save the result to local and upload the result to DFS.
#
#	@author Yang Fan(fyabc) 
#	@author Guanji Gao(ggj)
#
########################################

ReducerWorker : module {
	PATH : con "/appl/MR/mapreduce/reducerworker.dis";

	init : fn();
	
	run : fn(mapperFileAddrs : list of string , reducerTask : ref MRUtil->ReducerTask) : (int, string);
};
