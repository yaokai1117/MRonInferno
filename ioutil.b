implement IOUtil;

include "sys.m";
include "ioutil.m";
include "logger.m";
include "dfsclient.m";
include "dfsutil.m";
include "lists.m";


sys : Sys;
logger : Logger;
dfsutil : DFSUtil;
dfsclient : DFSClient;
lists : Lists;

Connection : import sys;
FD : import sys;
DFSFile : import dfsutil;
DFSChunk : import dfsutil;

init()
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsclient = load DFSClient DFSClient->PATH;
	lists = load Lists Lists->PATH;
}

split(fileName : string, number : int) : list of ref FileBlock
{
	ret : list of ref FileBlock;
	dfsclient->init();
	file := dfsclient->getFile(fileName);
	tSize := big (lists->last(file.chunks)).size + (lists->last(file.chunks)).offset;
	size := int (tSize / big number) + 1;
	offset := big 0;
	for (i := 0; i < number; i++) {
		fb : ref FileBlock;
		if (i < number - 1)
			fb = ref FileBlock(fileName, offset, size);
		else
			fb = ref FileBlock(fileName, offset, int (tSize - offset));
		ret = fb :: ret;	
		offset += big size;
	}
	return ret;
}

sendRemoteFile(port : int, fd : ref FD)
{
	(nil, dir) := sys->fstat(fd);
	fileName := dir.name;
	totalPath := sys->fd2path(fd);
	filePath := totalPath[:len totalPath-len fileName];

	(n, c) := sys->announce("tcp!*!" + string port);
	if (n < 0) {
		logger->log("IOUtil->sendRemoteFile: announce failed %r\n", Logger->ERROR);
		logger->scrlog("IOUtil->sendRemoteFile: announce failed %r\n", Logger->ERROR);
		exit;
	}

	while(1)
	{
		sys->seek(fd, big 0, sys->SEEKSTART);

		(ok, conn) := sys->listen(c);
		if (ok < 0) {
			logger->log("IOUtil->sendRemoteFile: listen failed %r\n", Logger->ERROR);
			logger->scrlog("IOUtil->sendRemoteFile: listen failed %r\n", Logger->ERROR);
			exit;
		}

		dfd := sys->open(conn.dir + "/data", sys->ORDWR);
		sys->write(dfd,array of byte fileName,len fileName);
		sys->mount(dfd, nil, filePath + "remote",Sys->MCREATE, nil);
		copyfd := sys->create(filePath + "remote/" + fileName, sys->ORDWR, 8r600);

		buf := array [Sys->ATOMICIO] of byte;
		length : int;
		do {
			length = sys->read(fd, buf, len buf);
			sys->write(copyfd, buf[:length], length);
		}while (length == len buf);

		sys->unmount(nil, filePath + "remote");
	}
}

getRemoteFile(addr : string, port : int, destPath : string) : ref FD
{
	(ok, conn) := sys->dial("tcp!" + addr + "!" + string port, nil);
	if (ok < 0)
	{
		logger->log("IOUtil->getRemoteFile--dial failed %r\n", Logger->ERROR);
		logger->scrlog("IOUtil->getRemoteFile--dial failed %r\n", Logger->ERROR);
		return nil;
	}

	buf := array [sys->ATOMICIO] of byte;
	length := sys->read(conn.dfd, buf, len buf);
	fileName := string buf[:length];

	sys->export(conn.dfd, destPath, Sys->EXPWAIT);

	ret := sys->open(destPath + fileName, sys->ORDWR);

	return ret;

}

splitLine(line : string) : (string ,string)
{
	(number, words) := sys->tokenize(line, "@");
	if(number<2)
		return (line, nil);
	else
		return (hd words,hd (tl words));
}

mergeSortedFiles(files : list of string, outputFile : string)
{

}
