implement IOUtil;

include "sys.m";
include "sort.m";
include "ioutil.m";
include "logger.m";
include "dfsclient.m";
include "dfsutil.m";
include "lists.m";


sys : Sys;
sort : Sort;
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
	sort = load Sort Sort->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsclient = load DFSClient DFSClient->PATH;
	lists = load Lists Lists->PATH;
	logger = load Logger Logger->PATH;

	logger->init();
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

sendRemoteFile(port : int, dir : string)
{
	addr := array [sys->ATOMICIO] of byte;
	msg := array [sys->ATOMICIO] of byte;

	(n, c) := sys->announce("tcp!*!" + string port);
	if (n < 0) {
		logger->log("IOUtil->sendRemoteFile: announce failed %r\n", Logger->ERROR);
		logger->scrlog("IOUtil->sendRemoteFile: announce failed %r\n", Logger->ERROR);
		exit;
	}

	while(1)
	{
		(ok, conn) := sys->listen(c);
		if (ok < 0) {
			logger->log("IOUtil->sendRemoteFile: listen failed %r\n", Logger->ERROR);
			logger->scrlog("IOUtil->sendRemoteFile: listen failed %r\n", Logger->ERROR);
			exit;
		}

		rdfd := sys->open(conn.dir + "/data", sys->OREAD);
		rfd := sys->open(conn.dir + "/remote", sys->OREAD);

		addrlen := sys->read(rfd, addr, len addr);
		msglen := sys->read(rdfd, msg, len msg);

		fileName := string (msg[:msglen]);

		fd := sys->open(dir + fileName, sys->OREAD);

		if (fd == nil)
		{
			logger->log("IOUtil->sendRemoteFile: open file " + fileName + " failed %r\n",Logger->ERROR);
			logger->scrlog("IOUtil->sendRemoteFile: open file " + fileName + " failed %r\n",Logger->ERROR);
			exit;
		}

		dfd := sys->open(conn.dir + "/data", sys->ORDWR);
		sys->mount(dfd, nil, dir + "remote",sys->MCREATE, nil);
		copyfd := sys->create(dir + "remote/" + fileName, sys->ORDWR, 8r600);

		buf := array [sys->ATOMICIO] of byte;
		length : int;
		do {
			length = sys->read(fd, buf, len buf);
			sys->write(copyfd, buf[:length], length);
		}while (length == len buf);

		sys->unmount(nil, dir + "remote");

		logger->logInfo("IOUtil->sendRemoteFile: send file " + fileName + " to " + string addr + "\n");
		logger->scrlogInfo("IOUtil->sendRemoteFile: send file " + fileName + " to " + string addr + "\n");
	}
}

getRemoteFile(addr : string, port : int, fileName : string, destPath : string) : ref FD
{
	(ok, conn) := sys->dial("tcp!" + addr + "!" + string port, nil);
	if (ok < 0)
	{
		logger->log("IOUtil->getRemoteFile--dial failed %r\n", Logger->ERROR);
		logger->scrlog("IOUtil->getRemoteFile--dial failed %r\n", Logger->ERROR);
		return nil;
	}

	sys->fprint(conn.dfd, "%s", fileName);

	sys->export(conn.dfd, destPath, Sys->EXPWAIT);

	ret := sys->open(destPath + fileName, sys->ORDWR);
	if (ret == nil)
	{
		logger->log("IOUtil->getRemoteFile: get file " + fileName + " failed\n",Logger->ERROR);
		logger->scrlog("IOUtil->getRemoteFile: get file " + fileName + " failed\n",Logger->ERROR);
		return nil;
	}

	logger->logInfo("IOUtil->getRemoteFile: get file " + fileName + " from " + addr + "!" + string port + " to " + destPath + "\n");
	logger->scrlogInfo("IOUtil->getRemoteFile: get file " + fileName + " from " + addr + "!" + string port + " to " + destPath + "\n");
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

KVsCmp.gt(kvs : self ref KVsCmp, a,b : ref KVs) : int
{
	if (a.key > b.key)
		return 1;
	else
		return 0;
}

OutputCollector.collect(collector : self ref OutputCollector, key : string, value : string)
{

	for(p := collector.collection; p != nil; p = tl p)
	{
		if((hd p).key == key)
			break;
	}

	if(p == nil)
	{
		collector.collection = (ref KVs(key,list of {value})) :: collector.collection;
	}
	else
	{
		(hd p).values = value :: (hd p).values;
	}

}

OutputCollector.getMap(collector : self ref OutputCollector) : array of ref KVs
{
	temp := collector.collection;
	lenMap := len temp;
	ret := array[lenMap] of ref KVs;
	for(i := 0; i < lenMap; i++)
	{
		ret[i] = hd temp;
		temp = tl temp;
	}

	cmp := ref KVsCmp();
	sort->sort(cmp,ret);
	return ret;
}