JobTracker : module {
		PATH : con "/usr/yaokai/jobtracker.dis";

		init : fn();

		submitJob : fn(config : ref Jobs->JobConfig) : int;
		startJob : fn(id : int) : int;

		updateTaskTrackers : fn(taskTracker : ref MRUtil->TaskTrackerInfo) : int;		

###### debug
		getJob : fn(id : int) : ref Jobs->Job;
		produceMapper : fn(job : ref Jobs->Job) : int;
		produceReducer : fn(job : ref Jobs->Job) : int;
######
						 
		shootMapper : fn(mapper : ref MRUtil->MapperTask) : int;
		
		mapperSucceed : fn(jobId : int, taskId : int) : int;
		reducerSucceed : fn(jobId : int, taskId : int) : int;
		mapperFailed : fn(jobId : int, taskId : int) : int;
		reducerFailed : fn(jobId : int, taskId : int) : int;
};

