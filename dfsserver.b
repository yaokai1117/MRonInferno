implement DFSServer;

include "sys.m";
include "draw.m";
include "dfsmaster.m";
include "dfsutil.m";
include "hash.m";
include "tables.m";
include "lists.m";

sys : Sys;
dfsmaster : DFSMaster;
dfsutil : DFSUtil;

Connection : import sys;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;


DFSServer : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsmaster = load DFSMaster DFSMaster->PATH;
	
	dfsmaster->init();
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
		sys->print("DFSServer: listen failed %r\n");
	    exit;
	}
	rdf := sys->open(conn.dir + "/remote", sys->OREAD);
	n := sys->read(rdf, buf, len buf);
	hdlrthread(c, ctxt);
}

hdlrthread(conn : Connection, ctxt : ref Draw->Context)
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
	msg = split(string msgStr[:msglen], '@');
	op := hd msg;
	msg = tl msg;
	name : string;
	replicas : int;
	fileList : list of string;
	offset : big;
	size : int;
	ok : int;
	case (op)
	{
		"create" => {
			name = hd msg;
			msg = tl msg;
			replicas = int hd msg;
			dfsmaster->createFile(name, replicas);
			sys->fprint(wdfd, "Successfully create file %s !\n", name);
		}

		"delete" => {
			name = hd msg;
			msg =tl msg;
			dfsmaster->deleteFile(name);
			sys->fprint(wdfd, "Successfully delete file %s !\n", name);
		}

		"list" => {
			fileList := dfsmaster->listFiles();
			sys->fprint(wdfd, "%s", list2string(fileList));
		}

		"chunk" => {
			name = hd msg;
			msg = tl msg;
			offset = big hd msg;
			msg = tl msg;
			size = int hd msg;
			ok = dfsmaster->createChunk(name, offset, size);
			sys->fprint(wdfd, "%d", ok);
		}
		
		* =>
			sys->fprint(wdfd, "Unknown message!");
	}

}

split(src : string, div : int) : list of string
{
	ret : list of string;
	i := 0;
	offset := 0;
	length := len src;
	lists := load Lists Lists->PATH;
	
	while (i < length) {
		if (src[i] == div) {
			ret = src[offset:i] :: ret;
			offset = i + 1;
		}
		i++;
	}
	if (offset < length)
		ret = src[offset:] :: ret;
	ret = lists->reverse(ret);
	return ret;
}

list2string(src : list of string) : string
{
	ret : string;
	for (p := src; p != nil; p = tl p)
		ret = ret + hd p;
	return ret;
}
	
		

	
	

















