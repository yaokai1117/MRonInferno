MRUtil : module {
	PATH : con "/usr/yaokai/mrutil.dis";
	
	SUCCESS : con 0;
	PENDING : con 1;
	INIT 	: con 2;
	FALIED 	: con -1;

		
	MapperTask : adt{
		# common task properties 
		taskId : int;
		jobId : int;
		status : int;		
		attemptCount : int;
		taskTrackerName : string;
		mrClassName : string;
		outputDir : string;

		# mapper
		inputFileBlock : ref IOUtil->FileBlock;
		reducerAmount : int;
	};

	ReducerTask : adt{
		# common task properties 
		taskId : int;
		jobId : int;
		status : int;		
		attemptCount : int;
		taskTrackerAddr : string;
		taskTrackerPort : int;
		mrClassName : string;
		outputDir : string;

		# reducer
		outputFile : string;
		partitionIndex : int;
		mapperAmount : int;
		replicas : int;
		lineCount : int;
	};

	TaskTrackerInfo : adt {
		address : string;
		port : int;
		mapperTaskNum : int;
		reducerTaskNum : int;
	};


	init : fn();
};
