########################################
#
#	The IOUtil module includes some often used adts and utilities when doing MapReduce.
#	The FileBlock adt is the input of each mappertask. It is created by function split().
#	The OutputCollector adt is a collector of KVs. It is used in mappertask and reducertask.
#	sendRemoteFile and getRemoteFile are used to send mapper results to workers.
#	mergeSortedFiles is used to merge the mapper results into a file.
#	
#	@author Yang Fan(fyabc) 
#
########################################

IOUtil : module{
	PATH : con "/appl/MR/mapreduce/ioutil.dis";

	FileBlock : adt{
		fileName : string;
		offset : big;
		size : int;
	};
 
	KVs : adt{
		key : string;
		values : list of string;
	};

	KVsCmp : adt{
		gt : fn(kvs : self ref KVsCmp, a,b : ref KVs) : int;
	};

	OutputCollector : adt{
		collection : ref Tables->Strhash[ref KVs];

		collect : fn(collector : self ref OutputCollector, key : string, value : string);
		getMap : fn(collector : self ref OutputCollector) : array of ref KVs;
	};

	init : fn();

	split : fn(fileName : string, number : int) : list of ref FileBlock;
	#//split the dfsFile into fileBlocks

	sendRemoteFile : fn(port : int, dir : string);

	getRemoteFile : fn(addr :string, fileName : string, destPath : string) : ref Sys->FD;

	splitLine : fn(line : string) : (string ,string);
	#//split the line into (key,value)

	mergeSortedFiles : fn(files : list of string, outputFile : string);
};
