MRUtil : module {
	PATH : con "/usr/fyabc/mrutil.dis";
	Task : adt{
		taskId : int;
		jobId : int;
		taskStatus : int;		#//0 : pending; 1 : succeed; -1 : failed
		attemptCount : int;
		taskTrackerName : string;
		mrClassName : string;
		outputDir : string;

		createTaskFolder : fn();
		deleteTaskFolder : fn();
	}

	MapperTask : adt{
		basic : ref Task;	#//basic task info

		inputFileBlock : ref IOUtil->FileBlock;
		reducerAmount : int;
	}

	ReducerTask : adt{
		basic : ref Task;	#//basic task info

		outputFile : string;
		partitionIndex : int;
		mapperAmount : int;
		replicas : int;
		lineCount : int;
	}

	OutPutCollector : adt{

	}

	init : fn();
};