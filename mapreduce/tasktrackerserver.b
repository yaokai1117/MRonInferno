implement TaskTrackerServer;

include "sys.m";
include "draw.m";
include "bufio.m";
include "tables.m";

include "ioutil.m";
include "mrutil.m";
include "../logger/logger.m";
include "tasktracker.m";


TaskTrackerServer : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

sys : Sys;
bufio : Bufio;
ioutil : IOUtil;
mrutil : MRUtil;
logger : Logger;
tasktracker : TaskTracker;

Connection : import sys;
Iobuf : import bufio;

MapperTask : import mrutil;
ReducerTask : import mrutil;
TaskTrackerInfo : import mrutil;

localAddr : string;
localPort := 66667;
hostAddr : string;
hostPort := 66666;
mapperFilePort := 70000;

mutex : chan of int;


init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	mrutil = load MRUtil MRUtil->PATH;
	logger = load Logger Logger->PATH;
	tasktracker = load TaskTracker TaskTracker->PATH;

	mrutil->init();
	tasktracker->init();

	logger->init();
	logger->setFileName("log_tasktrackerserver");

	mutex = chan [1] of int;

	buffer := bufio->open("/appl/MR/config", Bufio->OREAD);
	hostAddr = buffer.gets('\n');
	hostAddr = hostAddr[:len hostAddr - 1];
	localAddr = buffer.gets('\n');
	localAddr = localAddr[:len localAddr - 1];


	(ok, conn) := sys->announce("tcp!*!" + string localPort);
	if (ok < 0) {
		logger->log("TaskTrackerServer: announce failed!", Logger->ERROR);
		logger->scrlog("TaskTrackerServer: announce failed!", Logger->ERROR);
		exit;	
	}

	spawn heartBeat();

	while(1) {
		listen(conn);
	}
}

listen(conn : Connection)
{
	buf := array [Sys->ATOMICIO] of byte;
	(ok, c) := sys->listen(conn);
	if (ok < 0) {
		logger->log("TaskTrackerServer: listen failed!", Logger->ERROR);
		logger->scrlog("TaskTrackerServer: listen failed!", Logger->ERROR);
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
		case (op) {
			"shootMapper" => {
				mapper := mrutil->msg2mapper(msg);
				spawn runMapper(mapper);
			}
			"shootReducer" => {
				mapperFileAddr := hd msg;
				msg = tl msg;
				reducer := mrutil->msg2reducer(msg);
				spawn runReducer(mapperFileAddr, reducer);
			}
		}
	msglen = sys->read(rdfd, msgStr, len msgStr);
	}
}

heartBeat()
{
	while (1) {
		sys->sleep(3000);
		(n, conn) := sys->dial("tcp!" + hostAddr + "!" + string hostPort, nil);	
		tracker := ref TaskTrackerInfo(localAddr, localPort, 0, 0, 1, 0);
		msg := "updateTaskTracker" + "@" + mrutil->tracker2msg(tracker);		
		sys->fprint(conn.dfd, "%s", msg);
	}
}

runMapper(mapper : ref MapperTask)
{
	mutex <- = 0;
	ok := tasktracker->runMapperTask(mapper);
	<- mutex;
	msg : string;
	if (ok == 0) {
		logger->logInfo("MapperTask " + string mapper.id + " from job " + string mapper.jobId + " succeed!");
		logger->scrlogInfo("MapperTask " + string mapper.id + " from job " + string mapper.jobId + " succeed!");

		msg = "mapperSucceed";
	}
	else {
		logger->logInfo("MapperTask " + string mapper.id + " from job " + string mapper.jobId + " failed!");
		logger->scrlogInfo("MapperTask " + string mapper.id + " from job " + string mapper.jobId + " failed!");
		
		msg = "mapperFailed";
	}

	(n, conn) := sys->dial("tcp!" + hostAddr + "!" + string hostPort, nil);	
	msg = msg + "@" + "tcp!" + localAddr + "!" + string (mapperFilePort + mapper.id)+ "@" + mrutil->mapper2msg(mapper);
	sys->fprint(conn.dfd, "%s", msg);

	buf := array [Sys->ATOMICIO] of byte;
	length := sys->read(conn.dfd, buf, len buf);
	logger->logInfo("Feed back " + string buf[:length]);
	logger->scrlogInfo("Feed back " + string buf[:length]);
}

runReducer(mapperFileAddr : string, reducer : ref ReducerTask)
{
	mutex <- = 0;
	(ok, failedAddr) := tasktracker->runReducerTask(mapperFileAddr, reducer);
	<- mutex;
	msg : string;
	if (ok == 0) {  	#succeed
		logger->logInfo("ReducerTask " + string reducer.id + " from job " + string reducer.jobId + " succeed!");
		logger->scrlogInfo("ReducerTask " + string reducer.id + " from job " + string reducer.jobId + " succeed!");

		msg = "reducerSucceed";
	}
	else if (ok == 1) { 	#still pending
		logger->logInfo("ReducerTask " + string reducer.id + " from job " + string reducer.jobId + " get mapper address!");
		return;
	}
	else {
		logger->logInfo("ReducerTask " + string reducer.id + " from job " + string reducer.jobId + " failed!");
		logger->scrlogInfo("ReducerTask " + string reducer.id + " from job " + string reducer.jobId + " failed!");
		
		msg = "reducerFailed" + "@" + failedAddr;
	}

	(n, conn) := sys->dial("tcp!" + hostAddr + "!" + string hostPort, nil);	
	msg = msg + "@" + mrutil->reducer2msg(reducer);
	sys->fprint(conn.dfd, "%s", msg);

	buf := array [Sys->ATOMICIO] of byte;
	length := sys->read(conn.dfd, buf, len buf);
	logger->logInfo("Feed back " + string buf[:length]);
	logger->scrlogInfo("Feed back " + string buf[:length]);
}


