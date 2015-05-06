DFSClient : module {
	PATH : con "/usr/yaokai/dfsclient.dis";
	
	init : fn();

	createFile : fn(fileName : string, replicas : int) : int;
	createChunk : fn(fileName : string, offset : big, size : int) : int;
	listFiles : fn() : string;
	getFile : fn(fileName : string) : ref DFSUtil->DFSFile;
	deleteFile : fn(fileName : string);

#	readChunk : fn(chunk : ref DFSUtil->DFSChunk, offset : big, size : int) : array of byte;
#	writeChunk : fn(chunk : ref DFSUtil->DFSChunk, offset : big, size : int, data : array of byte) : int;
#	deleteChunk : fn(chunk : ref DFSUtil->DFSChunk) : int;
#	lineOffset : fn(fileName : string) : array of big;
#	lineOffsetChunk : fn(chunk : ref DFSUtil->DFSChunk) : array of big;

	updateNode : fn(addr : string, port : int, chunkNumber : int) : int;
	removeNode : fn(addr : string, port : int) : int;
};


