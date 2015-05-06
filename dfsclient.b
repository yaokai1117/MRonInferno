implement DFSClient;

include "sys.m";
include "dfsutil.m";
include "dfsclient.m";

sys : Sys;
dfsutil : DFSUtil;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;
Connection : import sys;

masterAddr : string;
masterPort : int;
defaultRep : int;
defaultChunkSize : int;

conn : Connection;

init()
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	
	masterAddr = "127.0.0.1";
	masterPort = 2333;
	defaultRep = 3;
	defaultChunkSize = 1<<20;

	ok : int;
	(ok, conn) = sys->dial("tcp!" + masterAddr + "!" + string masterPort, nil);
	if (ok < 0) 
		sys->print("Error: DFSClient--dial failed!\n");
}

createFile(fileName : string, replicas : int) : int 
{
	buf := array [Sys->ATOMICIO] of byte;
	msg := "create@" + fileName + "@" + string replicas; 
	sys->fprint(conn.dfd, "%s", msg);
	length := sys->read(conn.dfd, buf, len buf);
	if (length <= 0) {
		sys->print("Error: DFSClient--createFile failed!\n");	
		return 1;
	}
	sys->print("%s", string buf[:length]);
	return 0;
}

createChunk(fileName : string, offset : big, size : int) : int
{
	buf := array [Sys->ATOMICIO] of byte;
	msg := "chunk@" + fileName + "@" + string offset + "@" + string size;
	sys->fprint(conn.dfd, "%s", msg);
	length := sys->read(conn.dfd, buf, len buf);
	ok := int string buf[:length];
	if (ok != 0) {
		sys->print("Error: DFSClient--createChunk failed: %d !,offset: %bd, size: %d\n", ok, offset, size);
		return 1;
	}
	return 0;
}

listFiles() : string
{
	buf := array [Sys->ATOMICIO] of byte;
	msg := "list";
	sys->fprint(conn.dfd, "%s", msg);
	length := sys->read(conn.dfd, buf, len buf);
	if (length <= 0)
		return nil;
	ret := string buf[:length];
	return ret;
}

getFile(fileName : string) : ref DFSFile
{
	return nil;
}

deleteFile(fileName : string)
{
}

updateNode(addr : string, port : int, chunkNumber : int) : int
{
	buf := array [Sys->ATOMICIO] of byte;
	msg := "upNode@" + addr + "@" + string port + "@" +  string chunkNumber; 
	sys->fprint(conn.dfd, "%s", msg);
	length := sys->read(conn.dfd, buf, len buf);
	if (length <= 0) {
		sys->print("Error: DFSClient--updateNode failed!\n");	
		return 1;
	}
	sys->print("%s", string buf[:length]);
	return 0;
}

removeNode(addr : string, port : int) : int
{
	buf := array [Sys->ATOMICIO] of byte;
	msg := "rmNode@" + addr + "@" + string port; 
	sys->fprint(conn.dfd, "%s", msg);
	length := sys->read(conn.dfd, buf, len buf);
	if (length <= 0) {
		sys->print("Error: DFSClient--removeNode failed!\n");	
		return 1;
	}
	sys->print("%s", string buf[:length]);
	return 0;
}



