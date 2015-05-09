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
FD : import sys;

masterAddr : string;
masterPort : int;
defaultRep : int;
defaultChunkSize : int;
defaultTempPath : string;

conn : Connection; init() { if (sys == nil)
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

# fyabc

readChunk(chunk : ref DFSChunk) : ref FD
{
	node : ref DFSNode;

	for(p := chunk.nodes; p != nil; p = tl p)
	{
		node = hd p;
		if((ret := readChunkWithNode(node, chunk.id)) != nil)
			return ret;
		}
		sys->print("All nodes of this chunk %d are failed to read!", chunk.id);
	return nil;
}

writeChunk(chunk : ref DFSChunk, offset : big, size : int, datafd : ref FD) : int
{
	ret := 0;
	node : ref DFSNode;

	for(p := chunk.nodes; p != nil; p = tl p)
	{
		sys->seek(datafd, offset, Sys->SEEKSTART);
		node = hd p;
		if(writeChunkWithNode(node, chunk.id, size, datafd) != 0)
		{
			sys->print("Write chunk %d in node addr: %s, port: %d failed!\n", chunk.id, node.addr, node.port);
			ret = -1;
		}
		else
			sys->print("Successed write chunk %d in node addr: %s, port: %d!\n", chunk.id, node.addr, node.port);
	}
    
    return ret;
}

deleteChunk(chunk : ref DFSChunk) : int
{
	ret := 0;
	node : ref DFSNode;
	for(p := chunk.nodes; p != nil; p = tl p)
	{
		node = hd p;
		if(deleteChunkWithNode(node, chunk.id)!=0)
		{
			sys->print("Delete chunk %d in node addr: %s, port: %d failed!\n", chunk.id, node.addr, node.port);
			ret = -1;
		}
		else
		sys->print("Successed delete chunk %d in node addr: %s, port: %d!\n", chunk.id, node.addr, node.port);
    }
    
    return ret;
}

lineOffset(fileName : string) : array of big
{
	return nil;
}

lineOffsetChunk(chunk : ref DFSChunk) : array of big
{
	return nil;
}

readChunkWithNode(node : ref DFSNode, chunkId : int) : ref FD
{
	ok : int;
	(ok, conn) = sys->dial("tcp!" + node.addr + "!" + string node.port, nil);
	if (ok < 0)
	{
		sys->print("Error: DFSClient->readChunk--dial failed!\n");
		return nil;
	}

	buf := array [Sys->ATOMICIO] of byte;
	msg := "read@" + string chunkId;
	sys->fprint(conn.dfd, "%s", msg);

	sys->export(conn.dfd, defaultTempPath, Sys->EXPWAIT);

	ret := sys->open(defaultTempPath + string chunkId, sys->ORDWR);

	return ret;
}

writeChunkWithNode(node : ref DFSNode, chunkId : int, size : int, datafd : ref FD) : int
{
	ok : int;
	(ok, conn) = sys->dial("tcp!" + node.addr + "!" + string node.port, nil);
	if (ok < 0)
	{
		sys->print("Error: DFSClient->writeChunk--dial failed!\n");
		return -1;
	}

	buf := array [Sys->ATOMICIO] of byte;
	msg := "write" ;
	sys->fprint(conn.dfd, "%s", msg);

	sendWriteData(datafd, chunkId, size, conn);

	return 0; 
}

deleteChunkWithNode(node : ref DFSNode, chunkId : int) : int
{
	ok : int;
	(ok, conn) = sys->dial("tcp!" + node.addr + "!" + string node.port, nil);
	if (ok < 0)
	{
		sys->print("Error: DFSClient->writeChunk--dial failed!\n");
		return -1;
	}
	
	buf := array [Sys->ATOMICIO] of byte;
	msg := "delete@" + string chunkId;
	sys->fprint(conn.dfd, "%s", msg);

	length := sys->read(conn.dfd, buf, len buf);
	return int string buf[:length];	
}

sendWriteData(datafd : ref FD, chunkId : int, size : int, conn : Connection)
{
	stmsg := array[100] of byte;
	n := sys->read(conn.dfd, stmsg, len stmsg);
	sys->mount(conn.dfd, nil, defaultTempPath + "remote",Sys->MCREATE, nil);
	datacopyfd := sys->create(defaultTempPath + "remote/" + string chunkId, sys->ORDWR, 8r600);

	buf := array [Sys->ATOMICIO] of byte;
	length : int;
	(nil, dir) := sys->fstat(datafd);
	total := dir.length;
	offset := sys->seek(datafd, big 0, Sys->SEEKRELA);
	if (int(total - offset) < size)
		size = int(total - offset);
	do {
		if (size < len buf) 
			length = sys->read(datafd, buf, size);
		else
			length = sys->read(datafd, buf, len buf);
		sys->write(datacopyfd, buf[:length], length);
		size -= length; 
	}while (size > 0);

	sys->unmount(nil, defaultTempPath + "remote");
}

