implement TestMaster;

include "sys.m";
include "draw.m";
include "dfsmaster.m";
include "dfsutil.m";
include "hash.m";
include "tables.m";

sys : Sys;
dfsmaster : DFSMaster;
dfsutil : DFSUtil;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;

TestMaster : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	dfsmaster = load DFSMaster DFSMaster->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;

	dfsmaster->init();
	dfsutil->init();

# test createFile, getFile, listFile, deleteFile
	dfsmaster->createFile("yaokaitext",3);
	dfsmaster->createFile("yaokaitext2",3);
	strlist := dfsmaster->listFiles();
	for (p := strlist; p != nil; p = tl p)
		sys->print("%s", hd p);
	file := dfsmaster->getFile("yaokaitext");
	sys->print("%s", file.toString());
	dfsmaster->deleteFile("yaokaitext2");	
	strlist = dfsmaster->listFiles();
	for (p = strlist; p != nil; p = tl p)
		sys->print("%s", hd p);

# test createChunk, updateNode 
	dfsmaster->updateNode(ref DFSNode("home", 110, 3));
	dfsmaster->updateNode(ref DFSNode("school", 233, 0));
	dfsmaster->updateNode(ref DFSNode("hospital", 120, 2));
	dfsmaster->createChunk("yaokaitext", big 0, 10);	
	file = dfsmaster->getFile("yaokaitext");
	chunk := hd file.chunks;
	sys->print("%s", chunk.toString());
	for (q := chunk.nodes; q != nil; q = tl q)
		sys->print("%s", (hd q).toString());
	strlist = dfsmaster->listFiles();
	for (p = strlist; p != nil; p = tl p)
		sys->print("%s", hd p);
	dfsmaster->removeNode(ref DFSNode("hospital", 120, 2));
	dfsmaster->createChunk("yaokaitext", big 10, 10);
	file = dfsmaster->getFile("yaokaitext");
	chunk2 := hd file.chunks;
	sys->print("%s", chunk2.toString());
	for (q = chunk2.nodes; q != nil; q = tl q)
		sys->print("%s", (hd q).toString());
}
