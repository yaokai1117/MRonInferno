DFSClient : module {
	PATH : con "/usr/yaokai/dfsutil.dis";
	
	masterAddr : string;
	masterPort : int;
	defaultReplicas : int;
	defaultChunkSize : int;

	init : fn();

	createFile : fn(fileName : string, replicas : int);
	listFiles : fn() : list of string;
	getFile : fn(fileName : string) : ref DFSFile;
	deleteFile : fn(fileName : string);

	readChunk : fn(chunk : ref DFSChunk, offset : big, size : int) : array of byte;
	writeChunk : fn(chunk : ref DFSChunk, offset : big, size : int, data : array of byte) : int;
	deleteChunk : fn(chunk : ref DFSChunk) : int;
	lineOffset : fn(fileName : string) : array of big;
	lineOffsetChunk : fn(chunk : ref DFSChunk) : array of big;
}


