implement DFSNodeServer;

include "sys.m";
include "draw.m";
include "bufio.m";

include "dfsutil.m";
include "dfsclient.m";


Connection : import sys;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;
FD : import sys;
Iobuf : import bufio;

sys : Sys;
bufio : Bufio;
dfsutil : DFSUtil;
dfsclient : DFSClient;

dataPath := string "/appl/MR/ser/";
homePath := string "/appl/MR/";
localPort := int 2334;
chunkNumber := int 0;


DFSNodeServer : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsclient = load DFSClient DFSClient->PATH;

	dfsutil->init();

	buffer := bufio->open("/appl/MR/config", Bufio->OREAD);
	buffer.gets('\n');
	localAddr := buffer.gets('\n');
	localAddr = localAddr[:len localAddr - 1];

	spawn heartBeat(localAddr);

	(n, conn) := sys->announce("tcp!*!" + string localPort);
	if (n < 0) {
		sys->print("DFSServer: announce failed %r\n");
		exit;
	}
	while(1) {
		listen(conn, ctxt);
	}
}

heartBeat(localAddr : string) {
	while (1) {
		dfsclient->init();
		dfsclient->updateNode(localAddr, localPort, chunkNumber); 
		dfsclient->disconnect();
		sys->sleep(30000);
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
				dfd := sys->open(conn.dir + "/data", sys->ORDWR);
				sys->fprint(dfd, "start");
				sys->export(dfd, dataPath, Sys->EXPWAIT);	
				chunkNumber++;
				break receive;
			}
			
			"delete" => {
				chunkId := hd msg;
				ok := sys->remove(dataPath + chunkId);	
				sys->fprint(wdfd, "%d", ok);	
				chunkNumber--;
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


