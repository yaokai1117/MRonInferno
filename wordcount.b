implement MapReduce;

include "sys.m";
include "ioutil.m";
include "mapreduce.m";

sys : Sys;
ioutil : IOUtil;

OutputCollector : import ioutil;

jobName := "WordCount";

init()
{
	sys = load Sys Sys->PATH;
	ioutil = load IOUtil IOUtil->PATH;

	ioutil->init();
}

map(key : string , value : string , collector : ref OutputCollector)
{
	word : string;
	(number , words) := sys->tokenize(value, " ");
	for( ; words != nil; words = tl words)
	{
		word = hd words;
		collector.collect(word , "1");
	}
}

reduce(key : string , values : list of string , collector : ref OutputCollector)
{

}

keySpaceSize() : int
{
	return 128;
}

hashKey(key : string) : int
{
	return key[0] % 128;
}