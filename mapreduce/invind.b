implement MapReduce;

include "sys.m";
include "ioutil.m";
include "tables.m";
include "mapreduce.m";

sys : Sys;
ioutil : IOUtil;

OutputCollector : import ioutil;

jobName := "InvertedIndex";

init()
{
	sys = load Sys Sys->PATH;
	ioutil = load IOUtil IOUtil->PATH;

	ioutil->init();
	#ioutil->getInputFile();
}

map(key : string , value : string , collector : ref OutputCollector)
{
	collector.collect(key , value);
}

reduce(key : string , values : list of string , collector : ref OutputCollector)
{
	value : string;
	for( ; values != nil ; values = tl values)
		value = value + ";" + hd values;
	collector.collect(key , value);
}

filt(line : string) : list of (string , string)
{
	ret : list of (string , string);

	(nil , words) := sys->tokenize(line , " ");
	
	for( ; words != nil ; words = tl words) {
		word := hd words;
		for (i := 0; i < len word && (word[i] < 65 || word[i] > 122);i++);
		for (j := len word - 1; j > 0 && (word[j] < 65 || word[j] > 122); j--);
		if (i > j)
			continue;
		ret = (word[i : j + 1] + "$" + "hello.txt", "1") :: ret;
	}

	return ret;
}

combine(key : string, values : list of string) : (string, string)
{
	(nil , words) := sys->tokenize(key , "$");
	return (hd words, hd (tl words) + "$" + string len values);
}

keySpaceSize() : int
{
	return 58;
}

hashKey(key : string) : int
{
	return key[0] - 65;
}
