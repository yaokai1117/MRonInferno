implement TestJob;

include "sys.m";
include "draw.m";
include "jobs.m";
include "mrutil.m";
include "tables.m";

sys : Sys;
jobs : Jobs;
mrutil : MRUtil;
table : Tables;

JobConfig : import jobs;
Job : import jobs;
MapperTask : import mrutil;
ReducerTask : import mrutil;

TestJob : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	jobs = load Jobs Jobs->PATH;
	mrutil = load MRUtil MRUtil->PATH;
	table = load Tables Tables->PATH;

	jobs->init();

	jc1 := ref JobConfig("play", "input1", "output1", 3, 1000, 4, 4, 3);
	jc2 := ref JobConfig("study", "input2", "output2", 3, 1000, 4, 4, 3);
	jb1 := Job.new(jc1);
	
	mp1 := ref MapperTask(23, 1, 2, 3, "home", "mydis", "outputdir1", 1);
	mp2 := ref MapperTask(233, 1, 2, 3, "school", "dis", "dir", 1);
	
	jb1.addMapper(mp1);
	jb1.addMapper(mp2);
	sys->print("%s", jb1.toString());

	jb1.status = 0;
	jb1.setTaskStatus(23, 1);
	sys->print("%s", jb1.toString());

	jb1.setTaskStatus(233, -1);
	sys->print("%s", jb1.toString());

	jb1.setTaskStatus(23, 0);
	jb1.setTaskStatus(233, 0);
	sys->print("%s", jb1.toString());
	
	
}
