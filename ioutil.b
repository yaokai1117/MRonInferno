implement IOUtil;

include "sys.m";
include "sort.m";
include "bufio.m";
include "ioutil.m";
include "logger.m";
include "dfsclient.m";
include "dfsutil.m";
include "lists.m";
include "tables.m";


sys : Sys;
sort : Sort;
bufio : Bufio;
logger : Logger;
dfsutil : DFSUtil;
dfsclient : DFSClient;
lists : Lists;
tables : Tables;

Connection : import sys;
FD : import sys;
Iobuf : import bufio;
DFSFile : import dfsutil;
DFSChunk : import dfsutil;
Strhash : import tables;

InputPair : adt{
	line : string;
	buffer : ref Iobuf;
};

PrQueue : adt{
	qu : array of InputPair;
	length : int;

	new : fn(length : int) : ref PrQueue;
	add : fn(pq : self ref PrQueue, node : InputPair) : int;
	poll : fn(pq : self ref PrQueue) : InputPair;
};

init()
{
	sys = load Sys Sys->PATH;
	sort = load Sort Sort->PATH;
	bufio = load Bufio Bufio->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsclient = load DFSClient DFSClient->PATH;
	lists = load Lists Lists->PATH;
	logger = load Logger Logger->PATH;
	tables = load Tables Tables->PATH;

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
	if(sys->open(dir + "remote/" , sys->OREAD) == nil)
		sys->create(dir + "remote/", sys->OREAD, sys->DMDIR + 8r777);

	addr := array [sys->ATOMICIO] of byte;
	msg := array [sys->ATOMICIO] of byte;

	(n, c) := sys->announce("tcp!*!" + string port);
	if (n < 0) {
		logger->log("IOUtil->sendRemoteFile: announce failed", Logger->ERROR);
		logger->scrlog("IOUtil->sendRemoteFile: announce failed", Logger->ERROR);
		exit;
	}

	while(1)
	{
		(ok, conn) := sys->listen(c);
		if (ok < 0) {
			logger->log("IOUtil->sendRemoteFile: listen failed", Logger->ERROR);
			logger->scrlog("IOUtil->sendRemoteFile: listen failed", Logger->ERROR);
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
			logger->log("IOUtil->sendRemoteFile: open file " + fileName + " failed",Logger->ERROR);
			logger->scrlog("IOUtil->sendRemoteFile: open file " + fileName + " failed",Logger->ERROR);
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

		logger->logInfo("IOUtil->sendRemoteFile: send file " + fileName + " to " + string addr[:addrlen]);
		logger->scrlogInfo("IOUtil->sendRemoteFile: send file " + fileName + " to " + string addr[:addrlen]);
	}
}

getRemoteFile(addr : string, fileName : string, destPath : string) : ref FD
{
	(ok, conn) := sys->dial(addr, nil);
	if (ok < 0)
	{
		logger->log("IOUtil->getRemoteFile--dial failed", Logger->ERROR);
		logger->scrlog("IOUtil->getRemoteFile--dial failed", Logger->ERROR);
		return nil;
	}

	sys->fprint(conn.dfd, "%s", fileName);

	sys->export(conn.dfd, destPath, Sys->EXPWAIT);

	ret := sys->open(destPath + fileName, sys->ORDWR);
	if (ret == nil)
	{
		logger->log("IOUtil->getRemoteFile: get file " + fileName + " failed",Logger->ERROR);
		logger->scrlog("IOUtil->getRemoteFile: get file " + fileName + " failed",Logger->ERROR);
		return nil;
	}

	logger->logInfo("IOUtil->getRemoteFile: get file " + fileName + " from " + addr + " to " + destPath);
	logger->scrlogInfo("IOUtil->getRemoteFile: get file " + fileName + " from " + addr + " to " + destPath);
	return ret;
}

splitLine(line : string) : (string , string)
{
	(length , words) := sys->tokenize(line , " ");
	if(length < 2)
		return (line , nil);
	else
		return (hd words , hd (tl words));
}

mergeSortedFiles(files : list of string, outputFile : string)
{
	line : string;
	temp : InputPair;

	buffer : ref Iobuf;
	out := bufio->open(outputFile, bufio->OWRITE);

	bqueue := PrQueue.new(len files);

	for ( ; files != nil ; files = tl files)
	{
		buffer = bufio->open(hd files, bufio->OREAD);
		if(buffer != nil)
		{
			line = buffer.gets('\n');
			if(line != nil)
			{
				bqueue.add(InputPair(line , buffer));	
			}
			else
			{
				buffer.flush();
				buffer.close();
			}			
		}
	}

	while(bqueue.length > 0)
	{
		temp = bqueue.poll();
		out.puts(temp.line);

		line = temp.buffer.gets('\n');
		if(line != nil)
		{
			bqueue.add(InputPair(line , temp.buffer));
		}
		else
		{
			temp.buffer.flush();
			temp.buffer.close();
		}
	}

	out.flush();
	out.close();
}

PrQueue.new(length : int) : ref PrQueue
{
	if (length == 0)
		length = 10;
	return ref PrQueue(array [length] of InputPair, 0);
}

PrQueue.add(pq : self ref PrQueue, node : InputPair) : int
{
	temp : InputPair;

	if(pq.length == len (pq.qu))
		return -1;

	pq.qu[pq.length++] = node;

	for(i := pq.length - 1;i > 0 ; i = (i - 1) / 2)
		if ((pq.qu[(i - 1) / 2]).line > (pq.qu[i]).line)
		{
			temp = pq.qu[i];
			pq.qu[i] = pq.qu[(i - 1) / 2];
			pq.qu[(i - 1) / 2] = temp;
		}
		else break;

	return 0;
}

PrQueue.poll(pq : self ref PrQueue) : InputPair
{
	temp : InputPair;

	if (pq.length == 0)
		return (nil , nil);

	ret := pq.qu[0];

	pq.qu[0] = pq.qu[--pq.length];

	i := 0;
	while (i < pq.length / 2)
	{
		if(2 * i + 2 < pq.length && (pq.qu[2 * i + 1]).line > (pq.qu[2 * i + 2]).line)
		{
			if((pq.qu[2 * i + 2]).line < (pq.qu[i]).line)
			{
				temp = pq.qu[i];
				pq.qu[i] = pq.qu[2 * i + 2];
				pq.qu[2 * i + 2] = temp;
				i = 2 * i + 2;
			}
			else
				break;	
		}
		else
		{
			if((pq.qu[2 * i + 1]).line < (pq.qu[i]).line)
			{
				temp = pq.qu[i];
				pq.qu[i] = pq.qu[2 * i + 1];
				pq.qu[2 * i + 1] = temp;
				i = 2 * i + 1;
			}
			else
				break;
		}
	}

	return ret;
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
	kvs := collector.collection.find(key);
	if (kvs == nil)
		collector.collection.add(key, ref KVs(key, value :: nil));
	else
		kvs.values = value :: kvs.values;
}

OutputCollector.getMap(collector : self ref OutputCollector) : array of ref KVs
{
	temp : list of ref KVs;
	for (i := 0; i < len collector.collection.items; i++)
		for (p := collector.collection.items[i]; p != nil; p = tl p) {
			(nil, kvs) := hd p;
			temp = kvs :: temp;
		}
	
	lenMap := len temp;
	ret := array[lenMap] of ref KVs;
	for(i = 0; i < lenMap; i++)
	{
		ret[i] = hd temp;
		temp = tl temp;
	}

	cmp := ref KVsCmp();
	sort->sort(cmp,ret);
	return ret;
}
