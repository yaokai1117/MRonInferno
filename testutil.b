implement Test;

include "sys.m";
sys : Sys;
include "draw.m";
include "dfsutil.m";
include "sort.m";

sort : Sort;
dfs : DFSUtil;
DFSNode : import dfs;
DFSChunk : import dfs;
DFSFile : import dfs;
DFSNodeCmp : import dfs;
DFSChunkCmp : import dfs;


Test : module
{
	init : fn (ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	dfs = load DFSUtil DFSUtil->PATH;
	sort = load Sort Sort->PATH;
	chunk := ref DFSChunk(233,1, big 0,10,nil);
	dfs->init();
	
	sys->print("test addNode, getNodes and Sort->sort\n");
	for (i := 0; i < 10; i++) {
		chunk.addNode(ref DFSNode("test"+string i, 1995, i));
	}
	chunk.addNode(ref DFSNode("test plus", 1995, 7));
	nodes := chunk.getNodes();
	for (i = 0; i < 11; i++)
		sys->print("%s", nodes[i].toString());
	dnc := ref DFSNodeCmp();
	sort->sort(dnc, nodes);
	for (i = 0; i < 11; i++)
		sys->print("%s", nodes[i].toString());
	sys->print("test removeNode\n"); 
	for (i = 0; i < 5; i++) 
		chunk.removeNode(ref DFSNode("test" + string (2*i), 1995, 2*i));
	for (p := chunk.nodes; p != nil; p = tl p)
		sys->print("%s", (hd p).toString());


	sys->print("test addChunk, getChunks and Sort->sort\n");
	dfsfile := ref DFSFile(88, "yua", nil, 3);
	for (i = 0; i < 10; i++)
		dfsfile.addChunk(ref DFSChunk(i, 88, big i, 10, nil));
	chunks := dfsfile.getChunks();
	for (i = 0; i < 10; i++)
		sys->print("%s", chunks[i].toString());
	dfc := ref DFSChunkCmp();
	sort->sort(dfc, chunks);
	for (i = 0; i < 10; i++)
		sys->print("%s", chunks[i].toString());
	sys->print("test removeChunk\n");
	for (i = 0; i < 5; i++)
		dfsfile.removeChunk(ref DFSChunk(2*i, 88, big (2*i), 10, nil));
	for (q := dfsfile.chunks; q != nil; q = tl q)
		sys->print("%s", (hd q).toString());
	
}
