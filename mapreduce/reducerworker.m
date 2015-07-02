ReducerWorker : module {
	PATH : con "/appl/MR/mapreduce/reducerworker.dis";

	init : fn();
	
	run : fn(mapperFileAddrs : list of string , reducerTask : ref MRUtil->ReducerTask) : (int, string);
};
