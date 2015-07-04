########################################
#
#	The MapReduce module includes the MapReduce interface.
#	When submiting a mapreduce job, the user should implement this interface and upload it to DFS.
#	map() collect a pair of (key , value) into collector.
#	reduce() collect pairs of (key , value) which have the same key into collector.
#	filt() filt the input FileBlock and change it into list of (key , value).
#	if the mapreduce job is combinable (It means that the value meets the commutative law and associative law),
#	combine() will combine pairs of (key , value) which have the same key into (key , value).
#	keySpaceSize() return the hash size of hashKey();
#	hashKey() return the hash number of the key. The result of mapper tasks will be divided into blocks according to the hash key,
#	they are sent to different reducer tasks.
#	
#	@author Yang Fan(fyabc)
#
########################################


MapReduce : module{
	init : fn();

	map : fn(key : string , value : string , collector : ref IOUtil->OutputCollector);

	reduce : fn(key : string , values : list of string , collector : ref IOUtil->OutputCollector);

	filt : fn(line : string) : list of (string , string);

	combine : fn(key : string, values : list of string) : (string, string);

	keySpaceSize : fn() : int;

	hashKey : fn(key : string) : int;
};
