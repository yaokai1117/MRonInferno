implement DFSServer;

include "sys.m";
include "draw.m";
include "dfsmaster.m";
include "dfsutil.m";
include "hash.m";
include "tables.m";

Connection : import sys;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;

sys : Sys;
dfsmaster : DFSMaster;
dfsutil : DFSUtil;

dataPath : con "/usr/yaokai/ser";
homePath : con "/usr/yaokai";


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
	
	dfsmaster->updateNode(ref DFSNode("home", 110, 3));
	dfsmaster->updateNode(ref DFSNode("school", 233, 0));
	dfsmaster->updateNode(ref DFSNode("hospital", 120, 2));
	
	rdfd := sys->open(conn.dir + "/data", sys->OREAD);
	wdfd := sys->open(conn.dir + "/data", sys->OWRITE);
	dfd := sys->open(conn.dir + "/data", sys->ORDWR);
	rfd := sys->open(conn.dir + "/remote", sys->OREAD);

	addrlen := sys->read(rfd, addr, len addr);
	sys->print("Message from: %s\n", string addr[:addrlen]);

	msglen := sys->read(rdfd, msgStr, len msgStr);
	(nil, msg) = sys->tokenize(string msgStr[:msglen], "@");
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
		
		"get" => {
			name = hd msg;
			msg = tl msg;
			file := dfsmaster->getFile(name);
			sys->mount(dfd, nil, dataPath,Sys->MCREATE, nil);
			sys->chdir(dataPath);
			xmlf := sys->create(name + ".xml", sys->ORDWR, 8r600);
			file2xml(xmlf, file);
			sys->unmount(nil, dataPath);
			sys->chdir(homePath);
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

list2string(src : list of string) : string
{
	ret : string;
	for (p := src; p != nil; p = tl p)
		ret = ret + hd p;
	return ret;
}
	
file2xml(xmlf : ref Sys->FD, file : ref DFSFile) 
{
	sys->fprint(xmlf, "<file>");
	sys->fprint(xmlf, "<name>%s</name>", file.name);
	sys->fprint(xmlf, "<id>%d</id>", file.id);
	sys->fprint(xmlf, "<rep>%d</rep>", file.replicas);
	for (p := file.chunks; p != nil; p = tl p)
		chunk2xml(xmlf, hd p);
	sys->fprint(xmlf, "</file>");
}

chunk2xml(xmlf : ref Sys->FD, chunk : ref DFSChunk)
{
	sys->fprint(xmlf, "<chunk>");
	sys->fprint(xmlf, "<id>%d</id>", chunk.id);
	sys->fprint(xmlf, "<offset>%bd</offset>", chunk.offset);
	sys->fprint(xmlf, "<size>%d</size>", chunk.size);
	for (p := chunk.nodes; p != nil; p = tl p)
		node2xml(xmlf, hd p);
	sys->fprint(xmlf, "</chunk>");
}
	
node2xml(xmlf : ref Sys->FD, node : ref DFSNode)
{
	sys->fprint(xmlf, "<node>");
	sys->fprint(xmlf, "<a>%s</a>", node.addr);
	sys->fprint(xmlf, "<p>%d</p>", node.port);
	sys->fprint(xmlf, "<c>%d</c>", node.chunkNumber);
	sys->fprint(xmlf, "</node>");
}


