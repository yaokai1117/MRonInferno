MRUtil : module {
	PATH : con "/appl/MR/mapreduce/mrutil.dis";
	
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
		reducerAmount : int;
		inputFileBlock : ref IOUtil->FileBlock;
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
		mapperAmount : int;
		partitionIndex : int;
		outputFile : string;
		outputRep : int;
		outputSize : int;
	};

	TaskTrackerInfo : adt {
		addr : string;
		port : int;
		mapperTaskNum : int;
		reducerTaskNum : int;
		isWorking : int;

		toString : fn(tt : self ref TaskTrackerInfo) : string;
	};

	TaskTracker : adt {
		info : ref TaskTrackerInfo;
		mappers : list of ref MapperTask;
		reducers : list of ref ReducerTask;
	};


	init : fn();
	mapper2msg : fn(mapper : ref MapperTask) : string;
	reducer2msg : fn(reducer : ref ReducerTask) : string;
	tracker2msg : fn(tracker : ref TaskTrackerInfo) : string;
	msg2mapper : fn(msg : list of string) : ref MapperTask;
	msg2reducer : fn(msg : list of string) : ref ReducerTask;
	msg2tracker : fn(msg : list of string) : ref TaskTrackerInfo;

};
