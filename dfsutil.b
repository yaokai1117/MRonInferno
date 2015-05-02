implement DFSUtil;

include "dfsutil.m";
include "sys.m";
include "lists.m";
lists : Lists;
sys : Sys;

# load the Sys and Lists module, must be called before using toString and removeChunk, removeNode
init()
{
	sys = load Sys Sys->PATH;
	lists = load Lists Lists->PATH;
}

# implement functions of DFSNode
DFSNode.eq(a, b : ref DFSNode) : int
{
	return (a.addr == b.addr) && (a.port == b.port);
}

DFSNode.toString(dn : self ref DFSNode) : string
{
	ret := sys->sprint("addr: %s, port: %d, chunkNumber: %d\n", dn.addr, dn.port, dn.chunkNumber);
	return ret;
}

# implement DFSNodeCmp, a comparator of DFSNode
DFSNodeCmp.gt(dnc : self ref DFSNodeCmp, x, y : ref DFSNode) : int
{
	if (x.chunkNumber > y.chunkNumber)
		return 1;
	else
		return 0;
}

# implement functions of DFSChunk
DFSChunk.getNodes(dc : self ref DFSChunk) : array of ref DFSNode 
{
	length := len dc.nodes;
	ret := array [length] of ref DFSNode;
	i := 0;
	for (p := dc.nodes; p != nil; p = tl p) {
		ret[i] = hd p; 
		i++;
	}
	return ret;
}

DFSChunk.addNode(dc : self ref DFSChunk, node : ref DFSNode)
{
	dc.nodes = node :: dc.nodes;
}

DFSChunk.removeNode(dc : self ref DFSChunk, node : ref DFSNode)
{
	dc.nodes = lists->delete(node, dc.nodes);
}

DFSChunk.eq(a, b : ref DFSChunk) : int
{
	return (a.id == b.id) && (a.fileId == b.fileId);
}

DFSChunk.toString(dc : self ref DFSChunk) : string
{
	ret := sys->sprint("id: %d, fileId: %d, offset: %d, size: %d\n", dc.id, dc.fileId, int dc.offset, dc.size);
	return ret;
}

# implement DFSChunkCmp, a comparator of DFSChunk
DFSChunkCmp.gt(dcc : self ref DFSChunkCmp, x, y : ref DFSChunk) : int 
{
	if (x.offset > y.offset)
		return 1;
	else
		return 0;
}

# implement functions of DFSFile
DFSFile.getChunks(df : self ref DFSFile) : array of ref DFSChunk
{
	length := len df.chunks;
	ret := array [length] of ref DFSChunk;
	i := 0;
	for (p := df.chunks; p != nil; p = tl p) {
		ret[i] = hd p;
		i++;
	}
	return ret;
}

DFSFile.addChunk(df : self ref DFSFile, chunk : ref DFSChunk)
{
	df.chunks = chunk :: df.chunks;
}

DFSFile.removeChunk(df : self ref DFSFile, chunk : ref DFSChunk)
{
	df.chunks = lists->delete(chunk, df.chunks);
}
			
DFSFile.toString(df : self ref DFSFile) : string
{
	ret := sys->sprint("id: %d, name: %s, replicas: %d\n", df.id, df.name, df.replicas);
	return ret;
}
	
DFSFile.eq(a, b : ref DFSFile) : int
{
	return a.id == b.id;
}
