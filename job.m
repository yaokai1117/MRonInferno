Job : module {
		PATH : con "/usr/yaokai/job.dis";

		init : fn();

		Job : adt {
			id : int;
			config : ref JobConfig;
			status : int;
			mapperTasks : ref Tables->Table[ref MRUtil->MapperTask];
			reducerTasks : ref Tables->Table[ref MRUtil->ReducerTask];
			
			# functions
			getMapper : fn(jb : self ref Job, id : int) : ref MRUtil->MapperTask;
			getReducer : fn(jb : self ref Job, id : int) : ref MRUtil->ReducerTask;

			setTaskStatus : fn(jb : self ref Job, id : int, status : int) : int;
			getTaskStatus : fn(jb : self ref Job, id : int) : int;

			addMapper : fn(jb : self ref Job, mapper : ref MRUtil->MapperTask) : int;
			addReducer : fn(jb : self ref Job, reducer : ref MRUtil->ReducerTask) : int;

			getStatus : fn(jb : self ref Job) : int;

			toString : fn(jb : self ref Job) : string;
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

			#functions
			toString(jc : self ref JobConfig) : string;
		};
};
