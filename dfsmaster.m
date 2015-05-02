DFSMaster : module {
	PATH : con "/usr/yaokai/dfsmaster.dis";

	init : fn();

	MetaData : adt {
		nodes : list of ref DFSNode;
		fileIndex : ref HashTable;
		files : ref Table[ref DFSFile];
		chunks : ref Table[ref DFSChunk];
 	};

	createFile : fn(fileName : string, replicas : int);
	getFile : fn(fileName : string) : ref DFSFile;
	listFiles : fn() : list of string;
	deleteFile : fn(fileName : string);	

	createChunk : fn(fileName : string, offset : big, size : int); 

	updateNode : fn(node : ref DFSNode);
	removeNode : fn(node : ref DFSNode);
 };	
