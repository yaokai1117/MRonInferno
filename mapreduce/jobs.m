Jobs : module {
		PATH : con "/appl/MR/mapreduce/jobs.dis";

		Job : adt {
			id : int;
			config : ref JobConfig;
			status : int;
			mapperTasks : ref Tables->Table[ref MRUtil->MapperTask];
			reducerTasks : ref Tables->Table[ref MRUtil->ReducerTask];
			
			# functions
			new : fn(jc : ref JobConfig) : ref Job;

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
			mrClassName : string;
			inputFile : string;
			outputFile : string;
			outputRep : int;
			outputSize : int;
			mapperAmount : int;
			reducerAmount : int;
			maxAttemptNum : int;
			combinable : int;

			#functions
			toString : fn(jc : self ref JobConfig) : string;
		};


		init : fn();
		jobConfig2msg : fn(config : ref JobConfig) : string;
		msg2jobConfig : fn(msg : list of string) : ref JobConfig;
};
