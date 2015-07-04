########################################
#
#	The JobTracker module run the job.
#	It assigns and coordinates all the tasktrackers.
#	When the job is started, Jobtracker will produce mapperTasks and reducerTasks, and then shoot them to different tasktrackers.
#	Jobtracker will monitor the states of tasktrackers and tasks, 
#	and do some steps ,such as change tasks to other tasktrackers if some tasks are failed or some tasktrackers are broken.
#	
#	@author Kai Yao(yaokai)
#
########################################
JobTracker : module {
		PATH : con "/appl/MR/mapreduce/jobtracker.dis";

		init : fn();

		submitJob : fn(config : ref Jobs->JobConfig) : int;
		startJob : fn(id : int) : int;

		updateTaskTrackers : fn(taskTracker : ref MRUtil->TaskTrackerInfo) : int;		

###### debug
		getJob : fn(id : int) : ref Jobs->Job;
		produceMapper : fn(job : ref Jobs->Job) : int;
		produceReducer : fn(job : ref Jobs->Job) : int;
						 
		shootMapper : fn(mapper : ref MRUtil->MapperTask) : int;
		shootReducer : fn(reducer : ref MRUtil->ReducerTask, mapperFileAddr : string) : int;
######
		
		mapperSucceed : fn(task : ref MRUtil->MapperTask, mapperFileAddr : string) : int;
		reducerSucceed : fn(task : ref MRUtil->ReducerTask) : int;
		mapperFailed : fn(task : ref MRUtil->MapperTask) : int;
		reducerFailed : fn(task : ref MRUtil->ReducerTask) : int;
		reducerFailedonMapper : fn(task : ref MRUtil->ReducerTask, failedAddr : string) : int;
};

