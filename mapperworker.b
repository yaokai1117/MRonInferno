implement MapperWorker;

include "sys.m";
include "draw.m";
include "lists.m";
include "sort.m";
include "bufio.m";

include "mapperworker.m";
include "mapreduce.m";
#include "tasktracker.m";
include "mrutil.m";
include "ioutil.m";

include "dfsutil.m";
include "dfsclient.m";
include "download.m";

sys : Sys;
lists : Lists;
sort : Sort;
bufio : Bufio;

mapreduce : MapReduce;
#tasktracker : TaskTracker;
ioutil : IOUtil;
mrutil : MRUtil;
dfsutil : DFSUtil;
dfsclient : DFSClient;
download : Download;

FD : import sys;
Iobuf : import bufio;
MapperTask : import mrutil;
OutputCollector : import ioutil;
DFSFile : import dfsutil;
DFSChunkCmp : import dfsutil;
buffer : ref Iobuf;

mapperPath := "/usr/fyabc/task/";
folderName : string;

init(mapperTask : ref MapperTask)
{
	if(sys == nil)
		sys = load Sys Sys->PATH;
	if(lists == nil)
		lists = load Lists Lists->PATH;
	if(sort == nil)
		sort = load Sort Sort->PATH;
	if(bufio == nil)
		bufio = load Bufio Bufio->PATH;

	if(ioutil == nil)
		ioutil = load IOUtil IOUtil->PATH;
	if(mrutil == nil)
		mrutil = load MRUtil MRUtil->PATH;
#	if(tasktracker == nil)
#		tasktracker = load TaskTracker TaskTracker->PATH;

	if (dfsclient == nil)
		dfsclient = load DFSClient DFSClient->PATH;
	if (dfsutil == nil) 
		dfsutil = load DFSUtil DFSUtil->PATH;
	if (download == nil) 
		download = load Download Download->PATH;

	ioutil->init();
	mrutil->init();
#	tasktracker->init();
	dfsclient->init();
	dfsutil->init();

	folderName = mapperPath + "tasks_" + string mapperTask.id + "/";
	sys->create(folderName, sys->OREAD, sys->DMDIR + 8r777);

	if(sys->open(mapperPath + mapperTask.mrClassName, Sys->OREAD) == nil)
	{
		file := dfsclient->getFile(mapperTask.mrClassName);
		total := (lists->last(file.chunks)).offset + big (lists->last(file.chunks)).size;
		fd := sys->create(mapperPath + mapperTask.mrClassName, Sys->ORDWR, 8r600);
		dd(fd, file,big 0,total);
	}

	mapreduce = load MapReduce (mapperPath + mapperTask.mrClassName);
	mapreduce->init();

	dfsclient->init();
	file := dfsclient->getFile(mapperTask.inputFileBlock.fileName);
	fd := sys->create(folderName + mapperTask.inputFileBlock.fileName + "_inputFileBlock", Sys->ORDWR, 8r600);
	if(file == nil) sys->print("1\n");
	dd(fd, file, mapperTask.inputFileBlock.offset, big mapperTask.inputFileBlock.size);
	buffer = bufio->open(folderName + mapperTask.inputFileBlock.fileName + "_inputFileBlock", Bufio->OREAD);

	run(mapperTask);
}

run(mapperTask : ref MapperTask)
{
	collector := collect();

	saveToLocal(mapperTask, collector);

#	tasktracker->mapperSucceed(mapperTask);
} 

collect() : ref OutputCollector
{
	collector := ref OutputCollector(nil);
	line : string;

	while((line = buffer.gets('\n')) != nil)
	{
		(key , value) := ioutil->splitLine(line);
		mapreduce->map(key, line, collector);
	}

	return collector;
}

saveToLocal(mapperTask : ref MapperTask, collector : ref OutputCollector)
{
	fds := array[mapperTask.reducerAmount] of ref FD;
	for(i := 0; i < mapperTask.reducerAmount; i++)
	{
		fds[i] = sys->create(folderName + "part_" + string i, sys->ORDWR, 8r600);
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
		for( ; values != nil; values = tl values)
		{
			value := hd values;
			sys->fprint(fds[j], "%s", key + " " + value + "\n");
		}
	}
}

###########################################
#	MapperTask : adt{
#		# common task properties 
#		taskId : int;
#		jobId : int;
#		taskStatus : int;		
#		attemptCount : int;
#		taskTrackerName : string;
#		mrClassName : string;
#		outputDir : string;
#
#		createTaskFolder : fn();
#		deleteTaskFolder : fn();
#
#		# mapper
#		inputFileBlock : ref IOUtil->FileBlock;
#		reducerAmount : int;
#	};
###########################################

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