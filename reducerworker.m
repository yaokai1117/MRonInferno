ReducerWorker : module {
	PATH : con "/usr/yaokai/reducerworker.dis";

	init : fn();
	
	run : fn(mapperFileAddrs : list of string , reducerTask : ref MRUtil->ReducerTask);
};
