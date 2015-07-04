########################################
#
#	Tasktracker accept some mappers and reducers from jobtrackerserver, and run the mapperTasks and reducerTasks.
#	It returns states of its tasks (succeed or failed) and the addresses of mapper result files to jobtrackerserver.
#
#	@author Yang Fan(fyabc) 
#	@author Kai Yao(yaokai)
#
########################################

TaskTracker : module {
	PATH : con "/appl/MR/mapreduce/tasktracker.dis";

	init : fn();

	runMapperTask : fn(mapper : ref MRUtil->MapperTask) : int;
	runReducerTask : fn(mapperFileAddr : string, reducer : ref MRUtil->ReducerTask) : (int, string);
};
