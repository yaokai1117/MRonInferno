implement DFSNodeServer;

include "sys.m";
include "draw.m";
include "dfsutil.m";


Connection : import sys;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;
FD : import sys;

sys : Sys;
dfsutil : DFSUtil;

dataPath : con "/usr/yaokai/ser/";
homePath : con "/usr/yaokai/";


DFSNodeServer : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;

	dfsutil->init();

	(n, conn) := sys->announce("tcp!*!2334");
	if (n < 0) {
		sys->print("DFSServer: announce failed %r\n");
		exit;
	}
	while(1) {
		listen(conn, ctxt);
	}
}

listen(conn : Connection, ctxt : ref Draw->Context) 
{
	buf := array [sys->ATOMICIO] of byte;
	(ok, c) := sys->listen(conn);
	if (ok < 0) {
		sys->print("DFSSlaveServer: listen failed %r\n");
	    exit;
	}
	rdf := sys->open(conn.dir + "/remote", sys->OREAD);
	n := sys->read(rdf, buf, len buf);
	connHandle(c, ctxt);
}

connHandle(conn : Connection, ctxt : ref Draw->Context)
{
	addr := array [sys->ATOMICIO] of byte;
	msgStr := array [sys->ATOMICIO] of byte;
	msg : list of string;
	
	rdfd := sys->open(conn.dir + "/data", sys->OREAD);
	wdfd := sys->open(conn.dir + "/data", sys->OWRITE);
	rfd := sys->open(conn.dir + "/remote", sys->OREAD);

	addrlen := sys->read(rfd, addr, len addr);
	sys->print("Message from: %s\n", string addr[:addrlen]);

	msglen := sys->read(rdfd, msgStr, len msgStr);
	receive : while (msglen > 0) {
		(nil, msg) = sys->tokenize(string msgStr[:msglen], "@");
		op := hd msg;
		msg = tl msg;
		case (op)
		{
			"disconnect" => break receive;
			"read" => {
				chunkId := int hd msg;

				datafd := sys->open(dataPath + string chunkId, Sys->OREAD);

				if(datafd != nil)
					spawn sendReadData(datafd, chunkId, conn);

				break receive;
			}
			
			"write" => {
				chunkId := int hd msg;

				dfd := sys->open(conn.dir + "/data", sys->ORDWR);
				sys->export(dfd, dataPath, Sys->EXPWAIT);	
				sys->print("here");
				break receive;
			}
			
			"delete" => {
				chunkId := hd msg;
				sys->remove(dataPath + chunkId);	
			}
			
			"lineos" => {
			}
			
			"lineosc" =>{
			}
			* =>
				sys->fprint(wdfd, "Unknown message!\n");
		}
		msglen = sys->read(rdfd, msgStr, len msgStr);
	}

}

sendReadData(datafd : ref FD, chunkId : int, conn : Connection)
{
	dfd := sys->open(conn.dir + "/data", sys->ORDWR);
	sys->mount(dfd, nil, dataPath + "remote",Sys->MCREATE, nil);
	datacopyfd := sys->create(dataPath + "remote/" + string chunkId, sys->ORDWR, 8r600);

	buf := array [Sys->ATOMICIO] of byte;
	length : int;
	do {
		length = sys->read(datafd, buf, len buf);
		sys->write(datacopyfd, buf[:length], length);
	}while (length == len buf);

	sys->unmount(nil, dataPath + "remote");
}


