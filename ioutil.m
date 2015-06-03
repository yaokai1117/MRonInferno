IOUtil : module{
	PATH : con "/usr/yaokai/ioutil.dis";

	FileBlock : adt{
		fileName : string;
		offset : big;
		size : int;
	};

	init : fn();

	split : fn(fileName : string, number : int) : list of ref FileBlock;
	#//split the dfsFile into fileBlocks

	sendRemoteFile : fn(port : int, fd : ref Sys->FD);

	getRemoteFile : fn(addr :string, port : int, destPath : string) : ref Sys->FD;

	splitLine : fn(line : string) : (string ,string);
	#//split the line into (key,value)

	mergeSortedFiles : fn(files : list of string, outputFile : string);
};
