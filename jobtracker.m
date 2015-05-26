JobTracker : module {
		PATH : con "/usr/yaokai/jobtracker.dis";

		SUCCESS : con 0;
		PENDING : con 1;
		FAILED 	: con -1;

		Job : adt {
			id : int;
			config : JobConfig;
			status : int;
			mapperTasks : ref Tables->Table[ref MRUtil->MapperTask];
			reducerTasks : ref Tables->Table[ref MRUtil->ReducerTask];
			
			init : fn();
			
			getMapperTasks : fn() : list of ref MRUtil->MapperTask;
			getReducerTasks : fn() : list of ref MRUtil->ReducerTask;
			getMapper : fn(id : int) : ref MRUtil->MapperTask;
			getReducer : fn(id : int) : ref MRUtil->ReducerTask;

			setTaskStatus : fn(id : int, status : int) : int;
			getTaskStatus : fn(id : int) : int;

			addMapper : fn(ref MRUtil->MapperTask) : int;
			addReducer : fn(ref MRUtil->ReducerTask) : int;

			getStatus : fn() : int;

			toString : fn() : string;
		};

		JobConfig : adt {
			name : string;
			inputFile : string;
			outputFile : string;
			outputRep : int;
			outputBlockSize : int;
			mapperAmount : int;
			reducerAmount : int;
			maxAttemptNum : int;
		};

		init : fn();

		submitJob : fn(ref JobConfig) : int;
		startJob : fn(id : int) : int;

		updateTaskTrackers : fn(ref MRUtil->TaskTrackerInfo) : int;		

		produceMapper : fn();
		produceReducer : fn();
						 
		shotMapper : fn(ref MRUtil->MapperTask, ref MRUtil->TaskTrackerInfo) : int;
		
		mapperSucceed : fn(jobId : int, taskId : int) : int;
		reducerSucced : fn(jobId : int, taskId : int) : int;
		mapperFailed : fn(jobId : int, taskId : int) : int;
		reducerFailed : fn(jobId : int, taskId : int) : int;
};

