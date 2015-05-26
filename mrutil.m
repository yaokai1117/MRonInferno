MRUtil : module {
	PATH : con "/usr/yaokai/mrutil.dis";
	
	SUCCESS : con 0;
	PENDING : con 1;
	FALIED 	: con -1;

		
	MapperTask : adt{
		# common task properties 
		taskId : int;
		jobId : int;
		taskStatus : int;		
		attemptCount : int;
		taskTrackerName : string;
		mrClassName : string;
		outputDir : string;

		createTaskFolder : fn();
		deleteTaskFolder : fn();

		# mapper
		inputFileBlock : ref IOUtil->FileBlock;
		reducerAmount : int;
	};

	ReducerTask : adt{
		# common task properties 
		taskId : int;
		jobId : int;
		taskStatus : int;		
		attemptCount : int;
		taskTrackerName : string;
		mrClassName : string;
		outputDir : string;

		createTaskFolder : fn();
		deleteTaskFolder : fn();

		# reducer
		outputFile : string;
		partitionIndex : int;
		mapperAmount : int;
		replicas : int;
		lineCount : int;
	};

	TaskTrackerInfo : adt {
		name : string;
		address : string;
		port : int;
		mapperTaskNum : int;
		reducerTaskNum : int;
	};


	init : fn();
};
