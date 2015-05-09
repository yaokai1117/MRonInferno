implement TestCli;

include "sys.m";
include "draw.m";
include "dfsutil.m";
include "dfsclient.m";

sys : Sys;
dfsutil : DFSUtil;
dfsclient : DFSClient;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;

TestCli : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsclient = load DFSClient DFSClient->PATH;
	dfsutil->init();

	dfsclient->init();
	dfsclient->createFile("yaokai", 3);
	dfsclient->updateNode("127.0.0.1", 2334, 0);
	dfsclient->createChunk("yaokai", big 0, 20);
	dfsclient->createChunk("yaokai", big 20, 30);
#	dfsclient->createChunk("gagaga", big 0, 20);
#	str := dfsclient->listFiles();
#	sys->print("%s", str);
#	dfsclient->removeNode("school", 233);
#	dfsclient->createChunk("yaokai", big 20, 20);
#	dfsclient->deleteFile("gagaga");
#	dfsclient->deleteFile("gagaga");
	file := dfsclient->getFile("yaokai");
#	sys->print("%s", file.toString());
#	for (p := file.chunks; p != nil; p = tl p) {
#		chunk := hd p;
#		sys->print("\t%s", chunk.toString());
#		for (q := chunk.nodes; q != nil; q = tl q)
#			sys->print("\t\t%s", (hd q).toString());
#	}
	dfsclient->init();
	fd := sys->open("/usr/yaokai/cli/dfsclient.b",Sys->OREAD);
	for (p := file.chunks; p != nil; p = tl p) {
		chunk := hd p;
		dfsclient->writeChunk(chunk, chunk.offset, chunk.size, fd);
	}
	sys->print("fuck\n");
	
}
