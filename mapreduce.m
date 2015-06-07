MapReduce : module{
	init : fn();

	map : fn(key : string , value : string , collector : ref IOUtil->OutputCollector);

	reduce : fn(key : string , values : list of string , collector : ref IOUtil->OutputCollector);

	keySpaceSize : fn() : int;

	hashKey : fn(key : string) : int;
};