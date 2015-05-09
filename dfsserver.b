implement DFSServer;

include "sys.m";
include "draw.m";
include "dfsmaster.m";
include "dfsutil.m";
include "hash.m";
include "tables.m";
include "xmlhandle.m";

include "xml.m";
include "bufio.m";

Connection : import sys;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;

sys : Sys;
dfsmaster : DFSMaster;
dfsutil : DFSUtil;
xmlhandle : XmlHandle;

dataPath : con "/usr/yaokai/ser/";
homePath : con "/usr/yaokai/";


DFSServer : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsmaster = load DFSMaster DFSMaster->PATH;
	xmlhandle = load XmlHandle XmlHandle->PATH;
	
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
	connHandle(c, ctxt);
}

connHandle(conn : Connection, ctxt : ref Draw->Context)
{
	addr := array [sys->ATOMICIO] of byte;
	msgStr := array [sys->ATOMICIO] of byte;
	msg : list of string;
	
	dfsmaster->updateNode(ref DFSNode("127.0.0.1", 2334, 3));
#	dfsmaster->updateNode(ref DFSNode("home", 110, 3));
#	dfsmaster->updateNode(ref DFSNode("school", 233, 0));
#	dfsmaster->updateNode(ref DFSNode("hospital", 120, 2));
	
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
			"create" => {
				if (len msg != 2) {
					sys->fprint(wdfd, "Unknown message!\n");
					break;
				}
				name := hd msg;
				msg = tl msg;
				replicas := int hd msg;
				dfsmaster->createFile(name, replicas);
				sys->fprint(wdfd, "Successfully create file %s !\n", name);
			}

			"delete" => {
				name := hd msg;
				msg =tl msg;
				ok := dfsmaster->deleteFile(name);
				if (ok == 0)
					sys->fprint(wdfd, "Successfully delete file %s !\n", name);
				else
					sys->fprint(wdfd, "No such file in file list!\n");
			}

			"list" => {
				fileList := dfsmaster->listFiles();
				sys->fprint(wdfd, "%s", list2string(fileList));
			}
			
			"get" => {
				name := hd msg;
				msg = tl msg;
				file := dfsmaster->getFile(name);
				xmlf := sys->create(dataPath + name + ".xml", sys->ORDWR, 8r600);
				xmlhandle->file2xml(xmlf, file);
				sys->seek(xmlf, big 0, Sys->SEEKSTART);
				spawn sendXml(name, xmlf, conn);
				break receive;
			}

			"chunk" => {
				name := hd msg;
				msg = tl msg;
				offset := big hd msg;
				msg = tl msg;
				size := int hd msg;
				ok := dfsmaster->createChunk(name, offset, size);
				sys->fprint(wdfd, "%d", ok);
			}

			"upNode" => {
				addr := hd msg;
				msg = tl msg;
				port := int hd msg;
				msg = tl msg;
				chunkNumber := int hd msg;
				node := ref DFSNode(addr, port, chunkNumber);
				dfsmaster->updateNode(node);
				sys->fprint(wdfd, "Successfully update node %s", node.toString());
			}
			
			"rmNode" => {
				addr := hd msg;
				msg = tl msg;
				port := int hd msg;
			    msg = tl msg;
				ok := dfsmaster->removeNode(ref DFSNode(addr, port, 0));
				if (ok == 0)
					sys->fprint(wdfd, "Successfully remove node addr: %s, port: %d\n", addr, port);
				else
					sys->fprint(wdfd, "No such node in node list!\n");
					
			}
			* =>
				sys->fprint(wdfd, "Unknown message!\n");
		}
		msglen = sys->read(rdfd, msgStr, len msgStr);
	}

}

list2string(src : list of string) : string
{
	ret : string;
	for (p := src; p != nil; p = tl p)
		ret = ret + hd p;
	return ret;
}

sendXml(name : string, xmlf : ref Sys->FD, conn : Connection)
{
	dfd := sys->open(conn.dir + "/data", sys->ORDWR);
	sys->mount(dfd, nil, dataPath + "remote",Sys->MCREATE, nil);
	xmlf2 := sys->create(dataPath + "remote/" + name + ".xml", sys->ORDWR, 8r600);
	buf := array [Sys->ATOMICIO] of byte;
	length : int;
	do {
		length = sys->read(xmlf, buf, len buf);
		sys->write(xmlf2, buf[:length], length);
	}while ( length == len buf);
	sys->unmount(nil, dataPath + "remote");
	sys->remove(dataPath + name + ".xml");
}


