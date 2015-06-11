implement TaskTracker;

include "sys.m";
include "tasktracker.m";
include "ioutil.m";
include "mrutil.m";
include "jobs.m";

sys : Sys;
ioutil : IOUtil;
mrutil : MRUtil;
jobmodule : Jobs;

MapperTask : import mrutil;
ReducerTask : import mrutil;
TaskTrackerInfo : import mrutil;
TaskTracker : import mrutil;
Job : import jobmodule;
JobConfig : import jobmodule;

init()
{
}

runMapperTask(mapper : ref MapperTask) : int
{
	return 0;
}

runReducerTask(mapper : ref MapperTask, reducer : ref ReducerTask) : int
{
	return 0;
}

