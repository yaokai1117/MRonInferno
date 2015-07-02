implement MapReduce;

include "sys.m";
include "ioutil.m";
include "tables.m";
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
	collector.collect(key , value);
}

reduce(key : string , values : list of string , collector : ref OutputCollector)
{
	count := 0;
	for ( ; values != nil ; values = tl values)
	{
		count += int (hd values);
	}
	collector.collect(key , string count);
}

filt(line : string) : list of (string , string)
{
	ret : list of (string , string);

#	if(line[len line-1] == '\n')
#		line = line[: len line - 1];
#	for (i := 0 ; i < len line && line[i] == '\t' ; i++);
#	line = line[i :];
	
	(nil , words) := sys->tokenize(line , " ");
	
	for( ; words != nil ; words = tl words) {
		word := hd words;
		for (i := 0; i < len word && (word[i] < 65 || word[i] > 122);i++);
		for (j := len word - 1; j > 0 && (word[j] < 65 || word[j] > 122); j--);
		if (i > j)
			continue;
		ret = (word[i : j + 1], "1") :: ret;
	}

	return ret;
}

combine(key : string, values : list of string) : (string, string)
{
	return (key, string len values);
}

keySpaceSize() : int
{
	return 58;
}

hashKey(key : string) : int
{
	return key[0] - 65;
}
