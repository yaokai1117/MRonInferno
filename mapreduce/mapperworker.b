########################################
#
#	The implemention of the MapperWorker module.
#	the mapreduce module and inputFileBlock are downloaded from DFS.
#	collect() collect the pair of (key , value) into OutputCollector;
#	saveToLocal() sort and combine the pairs in OutputCollector and write it into a local file.
#
#	@author Yang Fan(fyabc) 
#	@author Guanji Gao(ggj)
#
########################################

implement MapperWorker;

include "sys.m";
include "draw.m";
include "lists.m";
include "tables.m";
include "sort.m";
include "bufio.m";

include "mapperworker.m";
include "mapreduce.m";
include "mrutil.m";
include "ioutil.m";

include "../dfs/dfsutil.m";
include "../dfs/dfsclient.m";

sys : Sys;
lists : Lists;
sort : Sort;
bufio : Bufio;
tables : Tables;

mapreduce : MapReduce;
ioutil : IOUtil;
mrutil : MRUtil;
dfsutil : DFSUtil;
dfsclient : DFSClient;

FD : import sys;
Iobuf : import bufio;
MapperTask : import mrutil;
OutputCollector : import ioutil;
DFSFile : import dfsutil;
DFSChunkCmp : import dfsutil;
Strhash : import tables;

mapperPath := "/appl/MR/task/";
mapperAddr : string;

downloadMutex : chan of int;
downloadMutex2 : chan of int;

init()
{
	sys = load Sys Sys->PATH;
	lists = load Lists Lists->PATH;
	sort = load Sort Sort->PATH;
	bufio = load Bufio Bufio->PATH;

	ioutil = load IOUtil IOUtil->PATH;
	mrutil = load MRUtil MRUtil->PATH;

	dfsclient = load DFSClient DFSClient->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;

	tables = load Tables Tables->PATH;

	ioutil->init();
	mrutil->init();
	dfsutil->init();

	buffer := bufio->open("/appl/MR/config", Bufio->OREAD);
	buffer.gets('\n');
	mapperAddr = buffer.gets('\n');
	mapperAddr = mapperAddr[: len mapperAddr - 1];

	downloadMutex = chan [1] of int;
	downloadMutex2 = chan [1] of int;
}

run(mapperTask : ref MapperTask)
{
	downloadMutex <-= 0;
	getmr(mapperTask);
	<-downloadMutex;

	folderName := mapperPath + "tasks_" + string mapperTask.id + "/";
	sys->create(folderName, sys->OREAD, sys->DMDIR + 8r777);

	downloadMutex2 <-= 0;
	getFileBlock(mapperTask);
	<-downloadMutex2;

	buffer := bufio->open(folderName + mapperTask.inputFileBlock.fileName + "_inputFileBlock", Bufio->OREAD);

	collector := collect(buffer);

	saveToLocal(mapperTask, collector, folderName);

	spawn ioutil->sendRemoteFile(70000 + mapperTask.id,folderName);
} 

collect(buffer : ref Iobuf) : ref OutputCollector
{
	collector := ref OutputCollector(nil);
	collector.collection = Strhash[ref IOUtil->KVs].new(10000, nil);
	line : string;

	while((line = buffer.gets('\n')) != nil)
	{
		kvList := mapreduce->filt(line);
		for ( ; kvList != nil ; kvList = tl kvList)
		{
			(key , value) := hd kvList;
			mapreduce->map(key, value, collector);
		}
	}

	return collector;
}

saveToLocal(mapperTask : ref MapperTask, collector : ref OutputCollector, folderName : string)
{
	fds := array[mapperTask.reducerAmount] of ref FD;
	buffers := array[mapperTask.reducerAmount] of ref Iobuf;
	for(i := 0; i < mapperTask.reducerAmount; i++)
	{
		fds[i] = sys->create(folderName + "tcp!" +  mapperAddr + "!" + string (70000 + mapperTask.id) + "_part_" + string i, sys->ORDWR, 8r600);
		buffers[i] = bufio->open(folderName + "tcp!" +  mapperAddr + "!" + string (70000 + mapperTask.id) + "_part_" + string i, bufio->OWRITE);
	}

	recordMap := collector.getMap();

	mapSize := len recordMap;
	keyRange : int;
	if(mapreduce->keySpaceSize() % mapperTask.reducerAmount == 0)
		keyRange = mapreduce->keySpaceSize() / mapperTask.reducerAmount;
	else
		keyRange = mapreduce->keySpaceSize() / mapperTask.reducerAmount + 1;

	for(i = 0; i < mapSize; i++)
	{
		key := recordMap[i].key;
		values := recordMap[i].values;

		j := mapreduce->hashKey(key) / keyRange;

		if (mapperTask.combinable == 1) {
			(key_cb, value_cb) := mapreduce->combine(key, values);
			buffers[j].puts(key_cb + " " + value_cb + "\n");
		}
		else {
			for( ; values != nil; values = tl values)
			{
				value := hd values;
				buffers[j].puts(key + " " + value + "\n");
			}
		}
	}

	for(i = 0; i < mapperTask.reducerAmount; i++)
	{
		buffers[i].flush();
		buffers[i].close();
	}
}

getmr(mapperTask : ref MapperTask)
{
	if(sys->open(mapperPath + mapperTask.mrClassName, Sys->OREAD) == nil)
	{
		dfsclient->init();
		file := dfsclient->getFile(mapperTask.mrClassName);
		total := (lists->last(file.chunks)).offset + big (lists->last(file.chunks)).size;
		fd := sys->create(mapperPath + mapperTask.mrClassName, Sys->ORDWR, 8r600);
		dd(fd, file,big 0,total);
	}
	mapreduce = load MapReduce (mapperPath + mapperTask.mrClassName);
	mapreduce->init();
}

getFileBlock(mapperTask : ref MapperTask)
{
	folderName := mapperPath + "tasks_" + string mapperTask.id + "/";
	dfsclient->init();
	file := dfsclient->getFile(mapperTask.inputFileBlock.fileName);
	fd := sys->create(folderName + mapperTask.inputFileBlock.fileName + "_inputFileBlock", Sys->ORDWR, 8r600);
	dd(fd, file, mapperTask.inputFileBlock.offset, big mapperTask.inputFileBlock.size);
}

dd(fd : ref Sys->FD, file : ref DFSFile, offset : big, size : big)
{
	if (dfsclient == nil)
		dfsclient = load DFSClient DFSClient->PATH;
	if (dfsutil == nil) {
		dfsutil = load DFSUtil DFSUtil->PATH;
		dfsutil->init();
	}
	if (sys == nil)
		sys == load Sys Sys->PATH;
	sort = load Sort Sort->PATH;

	dfsclient->init();
	chunks := file.getChunks();
	cmp := ref DFSChunkCmp();
	sort->sort(cmp, chunks);
	i := 0; 
	while (i < len chunks && chunks[i].offset + big chunks[i].size < offset)
		i++;
	while (i < len chunks && chunks[i].offset < offset + size) {
		tempfd := dfsclient->readChunk(chunks[i]);
		start := big 0;
		buf := array [Sys->ATOMICIO] of byte;

		if (chunks[i].offset < offset)
			start = sys->seek(tempfd, offset - chunks[i].offset, Sys->SEEKSTART);

		if (chunks[i].offset + big chunks[i].size > offset + size) {
			total := int (offset + size - chunks[i].offset - start);	
			while (total > len buf) {
				sys->read(tempfd, buf, len buf);
				sys->write(fd, buf, len buf);
				total -= len buf;
			}
			if (total > 0) {
				sys->read(tempfd, buf[:total], total);
				sys->write(fd, buf[:total], total);
			}
		}
		else while ((length := sys->read(tempfd, buf, len buf)) != 0)
			sys->write(fd, buf, length);
		sys->remove(sys->fd2path(tempfd));
		i++;
	}
}
