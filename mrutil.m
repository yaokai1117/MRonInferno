MRUtil : module {
	PATH : con "/usr/yaokai/mrutil.dis";
	
	SUCCESS : con 0;
	PENDING : con 1;
	INIT 	: con 2;
	FAILED 	: con -1;

		
	MapperTask : adt{
		# common task properties 
		id : int;
		jobId : int;
		status : int;		
		attemptCount : int;
		taskTrackerAddr : string;
		taskTrackerPort : int;
		mrClassName : string;

		toString : fn(mp : self ref MapperTask) : string;
		eq : fn(a, b : ref MapperTask) : int;				   

		# mapper
		inputFileBlock : ref IOUtil->FileBlock;
		reducerAmount : int;
	};

	ReducerTask : adt{
		# common task properties 
		id : int;
		jobId : int;
		status : int;		
		attemptCount : int;
		taskTrackerAddr : string;
		taskTrackerPort : int;
		mrClassName : string;

		toString : fn(rd : self ref ReducerTask) : string;
		eq : fn(a, b : ref ReducerTask) : int;				   

		# reducer
		outputFile : string;
		outputRep : int;
		outputSize : int;
		mapperAmount : int;
		partitionIndex : int;
	};

	TaskTrackerInfo : adt {
		addr : string;
		port : int;
		mapperTaskNum : int;
		reducerTaskNum : int;
		isWorking : int;

		toString : fn(tt : self ref TaskTrackerInfo) : string;
	};


	init : fn();
};
