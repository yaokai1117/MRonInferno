implement DFSClient;

include "sys.m";
include "dfsutil.m";
include "dfsclient.m";

include "xmlhandle.m";
include "xml.m";
include "bufio.m";


sys : Sys;
dfsutil : DFSUtil;
xml : Xml;
xmlhd : XmlHandle;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;
Connection : import sys;
Parser : import xml;

masterAddr : string;
masterPort : int;
defaultRep : int;
defaultChunkSize : int;
defaultTempPath : string;

conn : Connection;

init()
{
	if (sys == nil)
		sys = load Sys Sys->PATH;
	if (dfsutil == nil)
		dfsutil = load DFSUtil DFSUtil->PATH;
	if (xml == nil)
		xml = load Xml Xml->PATH;
	if (xmlhd == nil)
		xmlhd = load XmlHandle XmlHandle->PATH;

	xml->init();
	dfsutil->init();

	masterAddr = "127.0.0.1";
	masterPort = 2333;
	defaultRep = 3;
	defaultChunkSize = 1<<20;
	defaultTempPath = "/usr/yaokai/cli/";

	ok : int;
	(ok, conn) = sys->dial("tcp!" + masterAddr + "!" + string masterPort, nil);
	if (ok < 0) 
		sys->print("Error: DFSClient--dial failed!\n");
}

disconnect()
{
	sys->fprint(conn.dfd, "disconnect");
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
	buf := array [Sys->ATOMICIO] of byte;
	msg := "get@" + fileName;
	sys->fprint(conn.dfd, "%s", msg);
	sys->export(conn.dfd, defaultTempPath, Sys->EXPWAIT);	
	parser := xmlhd->init(defaultTempPath + fileName + ".xml");
	file := xmlhd->xml2file(parser);
	return file;
}

deleteFile(fileName : string) : int
{
	buf := array [Sys->ATOMICIO] of byte;
	msg := "delete@" + fileName;
	sys->fprint(conn.dfd, "%s", msg);
	length := sys->read(conn.dfd, buf, len buf);
	if (length <= 0) {
		sys->print("Error: DFSClient--deleteFile failed!\n");	
		return 1;
	}
	sys->print("%s", string buf[:length]);
	return 0;
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



