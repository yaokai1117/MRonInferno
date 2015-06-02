DFSClient : module {
	PATH : con "/usr/yaokai/dfsclient.dis";
	
# establish a connection to master server
	init : fn();

# disconnect from master server, getFile has the same effect
	disconnect : fn();

	createFile : fn(fileName : string, replicas : int) : int;
	createChunk : fn(fileName : string, offset : big, size : int) : int;
	listFiles : fn() : string;
	# Important!!! getfile will cause disconnection!! all opretaions after a getFile must init() first!!
	getFile : fn(fileName : string) : ref DFSUtil->DFSFile;
	deleteFile : fn(fileName : string) : int;

	readChunk : fn(chunk : ref DFSUtil->DFSChunk) : ref Sys->FD;
	writeChunk : fn(chunk : ref DFSUtil->DFSChunk, offset : big, size : int, datafd : ref Sys->FD) : int;
	deleteChunk : fn(chunk : ref DFSUtil->DFSChunk) : int;
	lineOffset : fn(fileName : string) : array of big;
	lineOffsetChunk : fn(chunk : ref DFSUtil->DFSChunk) : array of big;

	updateNode : fn(addr : string, port : int, chunkNumber : int) : int;
	removeNode : fn(addr : string, port : int) : int;
};


