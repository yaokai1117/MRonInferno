MapReduce : module{
	init : fn();

	map : fn(key : string , value : string , collector : ref IOUtil->OutputCollector);

	reduce : fn(key : string , values : list of string , collector : ref IOUtil->OutputCollector);

	filt : fn(line : string) : list of (string , string);

	keySpaceSize : fn() : int;

	hashKey : fn(key : string) : int;
};