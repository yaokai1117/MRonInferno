########################################
#
#	The implemention of Delete.
#	This program can be called in inferno shell, to delete a file in distributed file system.
#	An example of this command is: "delete hello.txt".
#
#	@author Kai Yao(yaokai)
#
########################################
implement Delete;

include "sys.m";
include "draw.m";
include "dfsclient.m";
include "dfsutil.m";

sys : Sys;
dfsclient : DFSClient;
dfsutil : DFSUtil;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;

Delete : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsclient = load DFSClient DFSClient->PATH;

	dfsutil->init();

	args = tl args;
	fileName := hd args;
	
	dfsclient->init();
	file := dfsclient->getFile(fileName);
	if (file == nil)
		exit;

	dfsclient->init();
	if (dfsclient->deleteFile(fileName) != 0)
		exit;
	for (p := file.chunks; p != nil; p = tl p) {
		dfsclient->init();
		ok := dfsclient->deleteChunk(hd p);
	}
}
	
