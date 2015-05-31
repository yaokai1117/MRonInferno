JobTracker : module {
		PATH : con "/usr/yaokai/jobtracker.dis";

		init : fn();

		submitJob : fn(config : ref JobConfig) : int;
		startJob : fn(id : int) : int;

		updateTaskTrackers : fn(taskTracker : ref MRUtil->TaskTrackerInfo) : int;		

		produceMapper : fn(job : ref Job);
		produceReducer : fn(job : ref Job);
						 
		shootMapper : fn(mapper : ref MRUtil->MapperTask) : int;
		
		mapperSucceed : fn(jobId : int, taskId : int) : int;
		reducerSucceed : fn(jobId : int, taskId : int) : int;
		mapperFailed : fn(jobId : int, taskId : int) : int;
		reducerFailed : fn(jobId : int, taskId : int) : int;
};

