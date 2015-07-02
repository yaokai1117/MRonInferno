implement ReducerWorker;

include "sys.m";
include "lists.m";
include "sort.m";
include "tables.m";
include "bufio.m";

include "reducerworker.m";
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

mrutil : MRUtil;
ioutil : IOUtil;
mapreduce : MapReduce;
dfsutil : DFSUtil;
dfsclient : DFSClient;

FD : import sys;
Iobuf : import bufio;
ReducerTask : import mrutil;
OutputCollector : import ioutil;
DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;
DFSChunkCmp : import dfsutil;
Strhash : import tables;

reducerPath := "/appl/MR/task/";
downloadMutex : chan of int;

init()
{
	sys = load Sys Sys->PATH;
	lists = load Lists Lists->PATH;
	sort = load Sort Sort->PATH;
	bufio = load Bufio Bufio->PATH;
	mrutil = load MRUtil MRUtil->PATH;
	ioutil = load IOUtil IOUtil->PATH;
	tables = load Tables Tables->PATH;

	mrutil->init();
	ioutil->init();
	downloadMutex = chan [1] of int;
}

run(mapperFileAddrs : list of string , reducerTask : ref ReducerTask) : (int, string)
{
	downloadMutex <-= 0;
	getmr(reducerTask);
	<-downloadMutex;

	folderName := reducerPath + "tasks_" + string reducerTask.id + "/";
	sys->create(folderName, sys->OREAD, sys->DMDIR + 8r777);
	files : list of string;

	for( ; mapperFileAddrs != nil; mapperFileAddrs = tl mapperFileAddrs)
	{
		mapperFileAddr := hd mapperFileAddrs;
		if (nil == ioutil->getRemoteFile(mapperFileAddr , mapperFileAddr + "_part_" + string reducerTask.partitionIndex , folderName))
			return (-1, mapperFileAddr);
		files = (folderName + mapperFileAddr + "_part_" + string reducerTask.partitionIndex) :: files;
	}


	unreducedFile := folderName + "unreducedFile";
	sys->create(unreducedFile , sys->ORDWR , 8r755);
	ioutil->mergeSortedFiles(files , unreducedFile);

	collector := ref OutputCollector(nil);
	collector.collection = Strhash[ref IOUtil->KVs].new(10000, nil);
	reader := bufio->open(unreducedFile , bufio->OREAD);
	line , key , prevkey , value : string;
	values : list of string;
	prevkey = key = line = nil;

	while(1)
	{
		line = reader.gets('\n');
		if (line == nil)
		{
			if(key != nil){
				mapreduce->reduce(key , values , collector);
			}
			break;
		}

		if(line[len line - 1] == '\n')
			line = line[ : len line - 1];

		(key , value) = ioutil->splitLine(line);
		if(prevkey != nil && key != prevkey)
		{
			mapreduce->reduce(prevkey , values , collector);
			values = nil;
		}
		values = value :: values;
		prevkey = key;
	}

	sys->create(folderName + string reducerTask.id + "_" + reducerTask.outputFile , sys->ORDWR , 8r755);
	buffer := bufio->open(folderName + string reducerTask.id + "_" + reducerTask.outputFile , bufio->OWRITE);
	output := collector.getMap();
	for(i := 0; i < len output; i++ )
	{
		kvs := output[i];
		for ( ; kvs.values != nil ; kvs.values = tl (kvs.values))
		{
			buffer.puts(kvs.key + " " + hd (kvs.values) + "\n");
		}
	}

	buffer.flush();
	buffer.close();

	ud(folderName , string reducerTask.id + "_" + reducerTask.outputFile , reducerTask.outputRep , reducerTask.outputSize);
	return (1, nil);
}

getmr(reducerTask : ref ReducerTask)
{
	if(sys->open(reducerPath + reducerTask.mrClassName, Sys->OREAD) == nil)
	{
		dfsclient->init();
		file := dfsclient->getFile(reducerTask.mrClassName);
		total := (lists->last(file.chunks)).offset + big (lists->last(file.chunks)).size;
		fd := sys->create(reducerPath + reducerTask.mrClassName, Sys->ORDWR, 8r600);
		dd(fd, file,big 0,total);
	}
	mapreduce = load MapReduce (reducerPath + reducerTask.mrClassName);
	mapreduce->init();
}

ud(folderName : string , fileName : string , replicas : int , chunkSize : int)
{
	sys->chdir(folderName);
	if (dfsclient == nil)
		dfsclient = load DFSClient DFSClient->PATH;
	if (dfsutil == nil) {
		dfsutil = load DFSUtil DFSUtil->PATH;
		dfsutil->init();
	}
	dfsclient->init();

	dfsclient->createFile(fileName, replicas);

	fd := sys->open(fileName, Sys->ORDWR);
	if (fd == nil)
		exit;
	(nil, dir) := sys->fstat(fd);
	totalSize := dir.length;
	offset := big 0;
	while (totalSize > big chunkSize) {
		dfsclient->createChunk(fileName, offset, chunkSize);
		offset += big chunkSize;
		totalSize -= big chunkSize;
	}
	if (totalSize != big 0)
		dfsclient->createChunk(fileName, offset, int totalSize);

	file := dfsclient->getFile(fileName);

	for (p := file.chunks; p != nil; p = tl p) {
		chunk := hd p;
		dfsclient->writeChunk(chunk, chunk.offset, chunk.size, fd);
	}
	sys->print("%s", file.toString());
	for (p = file.chunks; p != nil; p = tl p) {
		chunk := hd p;
		sys->print("\t%s", chunk.toString());
		for (q := chunk.nodes; q != nil; q = tl q)
			sys->print("\t\t%s", (hd q).toString());
	}
	sys->chdir("/appl/MR/");
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
