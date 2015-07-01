implement TestJob;

include "sys.m";
include "draw.m";
include "jobs.m";
include "ioutil.m";
include "mrutil.m";
include "jobtracker.m";

include "tables.m";

sys : Sys;
jobs : Jobs;
mrutil : MRUtil;
ioutil : IOUtil;
jobtracker : JobTracker;
table : Tables;

JobConfig : import jobs;
Job : import jobs;
MapperTask : import mrutil;
ReducerTask : import mrutil;
TaskTrackerInfo : import mrutil;
TaskTracker : import mrutil;
FileBlock : import ioutil;

TestJob : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	jobs = load Jobs Jobs->PATH;
	mrutil = load MRUtil MRUtil->PATH;
	ioutil = load IOUtil IOUtil->PATH;
	jobtracker = load JobTracker JobTracker->PATH;
	table = load Tables Tables->PATH;

	jobs->init();
	jobtracker->init();

	jc1 := ref JobConfig("play", "class1", "input1", "output1", 3, 1000, 4, 4, 3);
	jc2 := ref JobConfig("study", "class2", "input2", "output2", 3, 1000, 4, 4, 3);

	tt1 := ref TaskTrackerInfo("school", 23, 2, 0, 1);
	tt2 := ref TaskTrackerInfo("home", 46, 1, 0, 1);
	tt3 := ref TaskTrackerInfo("ship", 69, 3, 0, 1);

	jobtracker->updateTaskTrackers(tt1);
	jobtracker->updateTaskTrackers(tt2);
	jobtracker->updateTaskTrackers(tt3);

	jobtracker->submitJob(jc1);
	jobtracker->startJob(0);

	jb1 := jobtracker->getJob(0);
	sys->print("%s", jb1.toString());

	mp := jb1.getMapper(0);
	fb := mp.inputFileBlock;
	sys->print("%s\n", mp.taskTrackerAddr);
	sys->print("%s %d %d \n", fb.fileName, int fb.offset, fb.size);

	mp2 := jb1.getMapper(1);
	fb2 := mp2.inputFileBlock;
	sys->print("%s\n", mp2.taskTrackerAddr);
	sys->print("%s %d %d \n", fb2.fileName, int fb2.offset, fb2.size);

	mp3 := jb1.getMapper(2);
	fb3 := mp3.inputFileBlock;
	sys->print("%s\n", mp3.taskTrackerAddr);
	sys->print("%s %d %d \n", fb3.fileName, int fb3.offset, fb3.size);


}
