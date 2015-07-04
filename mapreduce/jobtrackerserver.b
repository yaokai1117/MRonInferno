########################################
#
#	The implemention of the JobTrackerServer module.
#	The JobTrackerServer module is the server of jobtracker.
#	It receives message about jobs from the client side, shoot mapper and reducer tasks to tasktrackers and get heartbeats from tasktrackers.
#
#	@author Kai Yao(yaokai)
#	@author Yang Fan(fyabc) 
#
########################################

implement JobTrackerServer;

include "sys.m";
include "draw.m";
include "ioutil.m";
include "mrutil.m";
include "jobs.m";
include "jobtracker.m";
include "../logger/logger.m";

include "tables.m";

JobTrackerServer : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

Connection : import sys;

Job : import jobmodule;
JobConfig : import jobmodule;
MapperTask : import mrutil;
ReducerTask : import mrutil;
TaskTracker : import mrutil;
TaskTrackerInfo : import mrutil;

sys : Sys;
ioutil : IOUtil;
mrutil : MRUtil;
jobmodule : Jobs;
jobtracker : JobTracker;
logger : Logger;


init(ctxt : ref Draw->Context, args : list of string) 
{
	sys = load Sys Sys->PATH;
	ioutil = load IOUtil IOUtil->PATH;
	mrutil = load MRUtil MRUtil->PATH;
	jobmodule = load Jobs Jobs->PATH;
	jobtracker = load JobTracker JobTracker->PATH;
	logger = load Logger Logger->PATH;

	ioutil->init();
	mrutil->init();
	jobmodule->init();
	jobtracker->init();

	logger->init();
	logger->setFileName("log_jobtrackerserver");

	(n, conn) := sys->announce("tcp!*!66666");
	if (n < 0) {
		logger->log("JobTrackerServer: announce failed!", Logger->ERROR);
		logger->scrlog("JobTrackerServer: announce failed!", Logger->ERROR);
		exit;	
	}
	while (1) {
		listen(conn);
	}
}

listen(conn : Connection)
{
	buf := array [Sys->ATOMICIO] of byte;
	(ok, c) := sys->listen(conn);
	if (ok < 0) {
		logger->log("JobTrackerServer: listen failed!", Logger->ERROR);
		logger->scrlog("JobTrackerServer: listen failed!", Logger->ERROR);
		exit;
	}
	rdf := sys->open(conn.dir + "/remote", Sys->OREAD);
	n := sys->read(rdf, buf, len buf);
	connHandle(c);
}

connHandle(conn : Connection)
{
	addr := array [Sys->ATOMICIO] of byte;
	msgStr := array [Sys->ATOMICIO] of byte;	
	
	rdfd := sys->open(conn.dir + "/data", Sys->OREAD);
	wdfd := sys->open(conn.dir + "/data", Sys->OWRITE);
	rfd := sys->open(conn.dir + "/remote", Sys->OREAD);

	addrlen := sys->read(rfd, addr, len addr);
	logger->logInfo("Message from: " + string addr[:addrlen-1]);
	
	msglen := sys->read(rdfd, msgStr, len msgStr);
	receive : while (msglen > 0) {
		(nil, msg) := sys->tokenize(string msgStr[:msglen], "@");
		op := hd msg;
		msg = tl msg;
		case (op){
			"disconnect" => break receive;
			"submit" => {
				config := jobmodule->msg2jobConfig(msg);
				ok := jobtracker->submitJob(config);
				sys->fprint(wdfd, "%d", ok);
			}
			"start" => {
				id := int (hd msg);
				ok := jobtracker->startJob(id);
				sys->fprint(wdfd, "%d", ok);
			}
			"updateTaskTracker" => {
				tracker := mrutil->msg2tracker(msg);
				ok := jobtracker->updateTaskTrackers(tracker);
				sys->fprint(wdfd, "%d", ok);
			}
			"mapperSucceed" => {
				mapperFileAddr := hd msg;
				msg = tl msg;
				mapper := mrutil->msg2mapper(msg);
				ok := jobtracker->mapperSucceed(mapper, mapperFileAddr);
				sys->fprint(wdfd, "%d", ok);
			}
			"reducerSucceed" => {
				reducer := mrutil->msg2reducer(msg);
				ok := jobtracker->reducerSucceed(reducer);
				sys->fprint(wdfd, "%d", ok);
			}
			"mapperFailed" => {
				mapper := mrutil->msg2mapper(msg);
				ok := jobtracker->mapperFailed(mapper);
				sys->fprint(wdfd, "%d", ok);
			}
			"reducerFailed" => {
				failedAddr := hd msg; msg = tl msg;
				reducer := mrutil->msg2reducer(msg);
				ok := jobtracker->reducerFailedonMapper(reducer, failedAddr);
				sys->fprint(wdfd, "%d", ok);
			}
		}
	msglen = sys->read(rdfd, msgStr, len msgStr);
	}
}

