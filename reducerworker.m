ReducerWorker : module {
	PATH : con "/usr/fyabc/reducerworker.dis";

	init : fn();

	#//There is a method that load the remote class(MR Instance).
	newMRInstance : fn() : ref MRUtil->MapReduce;

	createFolders : fn();
	
	run : fn();
};