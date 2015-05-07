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
	dfsclient->createFile("gagaga", 2);
	dfsclient->updateNode("home", 110, 3);
	dfsclient->updateNode("school", 233, 0);
	dfsclient->updateNode("hospital", 120, 2);
	dfsclient->createChunk("yaokai", big 0, 20);
	dfsclient->createChunk("gagaga", big 0, 20);
	str := dfsclient->listFiles();
	sys->print("%s", str);
	dfsclient->removeNode("school", 233);
	dfsclient->createChunk("yaokai", big 20, 20);
	dfsclient->deleteFile("gagaga");
	dfsclient->deleteFile("gagaga");
	file := dfsclient->getFile("yaokai");
	sys->print("%s", file.toString());
	for (p := file.chunks; p != nil; p = tl p) {
		chunk := hd p;
		sys->print("\t%s", chunk.toString());
		for (q := chunk.nodes; q != nil; q = tl q)
			sys->print("\t\t%s", (hd q).toString());
	}
}
