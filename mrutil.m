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
		taskTrackerName : string;
		mrClassName : string;
		outputDir : string;

		toString : fn(mp : self ref MapperTask) : string;

		# mapper
#		inputFileBlock : ref IOUtil->FileBlock;
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
		outputDir : string;

		toString : fn(rd : self ref ReducerTask) : string;

		# reducer
		outputFile : string;
		partitionIndex : int;
		mapperAmount : int;
		replicas : int;
		lineCount : int;
	};

	TaskTrackerInfo : adt {
		addr : string;
		port : int;
		mapperTaskNum : int;
		reducerTaskNum : int;

		toString : fn(tt : self ref TaskTrackerInfo) : string;
	};


	init : fn();
};
