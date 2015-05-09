implement Upload;

include "sys.m";
include "draw.m";
include "dfsclient.m";
include "dfsutil.m";
include "arg.m";

sys : Sys;
dfsclient : DFSClient;
dfsutil : DFSUtil;
arg : Arg;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;

Upload : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsclient = load DFSClient DFSClient->PATH;
	arg = load Arg Arg->PATH;

	dfsutil->init();
	dfsclient->init();

	replicas := 3;
	chunkSize := 100; 		# debug
	arg->init(args);
	while ((c := arg->opt()) != 0)
		case c {
			'r' => replicas = int arg->arg();
			's' => chunkSize = int arg->arg();
			* => {
				sys->print("unknown option (%c)\n", c);
				exit;
			}
		}
	args = arg->argv();
	fileName := hd args;

	dfsclient->createFile(fileName, replicas);
	
	fd := sys->open(fileName, Sys->ORDWR);
	if (fd == nil)
		exit;
	(nil, dir) := sys->fstat(fd);
	totalSize := dir.length;
	offset := big 0;
	while (totalSize > big chunkSize) {
		totalSize -= big chunkSize;
		if (big chunkSize > totalSize)
			chunkSize = int totalSize;
		dfsclient->createChunk(fileName, offset, chunkSize);
		offset += big chunkSize;
	}

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
}
	
	
	





