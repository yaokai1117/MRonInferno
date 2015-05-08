implement DFSSlaveServer;

include "sys.m";
include "draw.m";
include "dfsslave.m";
include "dfsutil.m";


Connection : import sys;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;
FD : import sys;

sys : Sys;
dfsslave : DFSSlave;
dfsutil : DFSUtil;
xmlhandle : XmlHandle;

dataPath : con "/usr/fyabc/slvser/";
homePath : con "/usr/fyabc/";


DFSSlaveServer : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsslave = load DFSSlave DFSSlave->PATH;
	xmlhandle = load XmlHandle XmlHandle->PATH;
	
	dfsslave->init();
	dfsutil->init();

	(n, conn) := sys->announce("tcp!*!2333");
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
				msg = tl msg;
				offset := big hd msg;
				msg = tl msg;
				size := int hd msg;

				datafd := dfsslave->read(chunkId, offset, size);

				if(datafd != nil)
					spawn sendReadData(datafd, chunkId, conn);
				else
				{					
				}
				
				sys->remove(dataPath + "datacopy_" + string chunkId);
			}
			
			"write" => {
				chunkId := int hd msg;
				msg = tl msg;
				offset := big hd msg;
				msg = tl msg;
				size := int hd msg;
				
				sys->export(conn.dfd, dataPath, Sys->EXPWAIT);
				
				datacopyfd := sys->open(dataPath + string chunkId, sys->OREAD);
				stat := dfsslave->write(chunkId, offset, size, datacopyfd);
				
				sys->fprint(wdfd, "%d", stat);
			}
			
			"delete" => {
				chunkId := int hd msg;

				stat := dfsslave->delete(chunkId);
				
				sys->fprint(wdfd, "%d", stat);
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
	datacopyfd := sys->create(dataPath + "remote/" + "datacopy_" + string chunkId, sys->ORDWR, 8r600);

	buf := array [Sys->ATOMICIO] of byte;
	length : int;
	do {
		length = sys->read(datafd, buf, len buf);
		sys->write(datacopyfd, buf[:length], length);
	}while (length == len buf);

	sys->unmount(nil, dataPath + "remote");
}


