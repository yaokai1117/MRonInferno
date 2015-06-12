implement TaskTracker;

include "sys.m";
include "tasktracker.m";
include "ioutil.m";
include "mrutil.m";

include "tables.m";

sys : Sys;
ioutil : IOUtil;
mrutil : MRUtil;

MapperTask : import mrutil;
ReducerTask : import mrutil;
TaskTrackerInfo : import mrutil;

init()
{
	sys = load Sys Sys->PATH;
	mrutil = load MRUtil MRUtil->PATH;
}

runMapperTask(mapper : ref MapperTask) : int
{
	sys->print("%s\n", mrutil->mapper2msg(mapper));
	return 0;
}

runReducerTask(mapperFileAddr : string, reducer : ref ReducerTask) : int
{
	sys->print("%s\n", mrutil->reducer2msg(reducer));
	return 0;
}


