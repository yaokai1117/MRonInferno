implement DFSMaster;

include "sys.m";
include "draw.m";
include "dfsmaster.m";
include "dfsutil.m";
include "sort.m";
include "lists.m";
include "tables.m";
include "hash.m";

sys : Sys;
dfsutil : DFSUtil;
sort : Sort;
lists : Lists;
tables : Tables;
hash : Hash;

Table : import tables;
HashTable : import hash;
HashVal : import hash;

DFSNode : import dfsutil;
DFSChunk : import dfsutil;
DFSFile : import dfsutil;
DFSChunkCmp : import dfsutil;
DFSNodeCmp : import dfsutil;

metadata : ref MetaData;
maxFileId := 0 ;
maxChunkId := 0;

init()
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	sort = load Sort Sort->PATH;
	lists = load Lists Lists->PATH;
	tables = load Tables Tables->PATH;
	hash = load Hash Hash->PATH;
	metadata = ref MetaData(nil, nil, nil, nil);
	metadata.fileIndex = hash->new(100);
	metadata.files = Table[ref DFSFile].new(100, nil);
	metadata.chunks = Table[ref DFSChunk].new(1000, nil);
}

createFile(fileName : string, replicas : int)  
{
	file := ref DFSFile(maxFileId++, fileName, nil, replicas);
	metadata.fileIndex.insert(fileName, HashVal(file.id, 0.0, nil));
	metadata.files.add(file.id, file);
}

getFile(fileName : string) : ref DFSFile
{
	hval := metadata.fileIndex.find(fileName);
	if (hval == nil)
		return nil;
	ret := metadata.files.find(hval.i);
	return ret;
}

listFiles() : list of string
{
	ret : list of string;
	p : list of (int, ref DFSFile);
	file : ref DFSFile;
	length := len metadata.files.items;
	nilint : int;
	size : big;

	ret = "id\tname\tsize\treplicas\n" :: ret;

	for (i := 0; i < length; i++) {
		for(p = metadata.files.items[i]; p != nil; p = tl p) {
			(nilint, file) = hd p;
			if (file.chunks == nil)
				size = big 0;
			else
				size = (hd file.chunks).offset + big (hd file.chunks).size;
			ret = sys->sprint("%d\t%s\t%bd\t%d\n", file.id, file.name, size, file.replicas) :: ret;
		}
	}
	ret = lists->reverse(ret);
	return ret;
}

deleteFile(fileName : string)
{
	hval := metadata.fileIndex.find(fileName);	
	id := hval.i;
	file := metadata.files.find(id);
	metadata.fileIndex.delete(fileName);
	metadata.files.del(id);
	for (p := file.chunks; p != nil; p = tl p)
		metadata.chunks.del((hd p).id);
}

createChunk(fileName : string, offset : big, size : int) : int
{
	hval := metadata.fileIndex.find(fileName);
	fileId := hval.i;
	file := metadata.files.find(fileId);
	nodes := allocNodes(file.replicas);
	if (nodes == nil)
		return 1;
	chunk := ref DFSChunk(maxChunkId++, fileId, offset, size, nodes);
	file.addChunk(chunk);
	metadata.chunks.add(chunk.id, chunk);
	return 0;
}

allocNodes(replicas : int) : list of ref DFSNode
{
	ret : list of ref DFSNode;
	length := len metadata.nodes;
	dnc := ref DFSNodeCmp();
	i := 0;
	if (length == 0)
		return nil;
	nodes := array [length] of ref DFSNode;

	for (p := metadata.nodes; p != nil; p = tl p)
		nodes[i++] = hd p;
	sort->sort(dnc, nodes);
	if (replicas > length)
		replicas = length;
	for (i = 0; i < replicas; i++)
		ret = nodes[replicas-1-i] :: ret;
	return ret;
}

updateNode(node : ref DFSNode)
{
	old := lists->find(node, metadata.nodes);
	if (old == nil)
		metadata.nodes = node :: metadata.nodes;
	else {
		oldNode := hd old;
		oldNode.chunkNumber = node.chunkNumber;
	}
}

removeNode(node : ref DFSNode) : int
{
	if (lists->find(node, metadata.nodes) == nil)
		return 1;
	metadata.nodes = lists->delete(node, metadata.nodes);
	return 0;
}

	
