implement MapReduce;

include "sys.m";
include "ioutil.m";
include "tables.m";
include "mapreduce.m";

sys : Sys;
ioutil : IOUtil;

OutputCollector : import ioutil;

jobName := "MatrixMultiply";
arow := 100;
acol := 100;
bcol := 100;

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
	row := array [acol] of { * => 0};
	col := array [acol] of { * => 0};
	total := 0;
	for ( ; values != nil ; values = tl values)
	{
		value := hd values;
		(nil , words) := sys->tokenize(value , "$");
		name := hd words; words = tl words;
		ij := hd words; words = tl words;
		zhi := hd words;
		if (name == "a")
			row [int ij] = int zhi;
		else
			col [int ij] = int zhi;
	}

	for (i := 0; i < acol ; i ++)
		total += row [i] * col[i];

	if (total != 0)
		collector.collect(key , string total);
}

filt(line : string) : list of (string , string)
{
	ret : list of (string , string);
	temp : int;
	name , i , j , num : string;

	if(line[len line-1] == '\n')
		line = line[: len line - 1];
	(length , words) := sys->tokenize(line , " ");
	if (words != nil)	{
		name = hd words; words = tl words;		
	}
	if (words != nil)	{
		i = hd words; words = tl words;		
	}	
	if (words != nil)	{
		j = hd words; words = tl words;		
	}	
	if (words != nil)	{
		num = hd words; words = tl words;		
	}

	if (length != 4 || (name != "a" && name != "b"))
	{
		return nil;
	}

	if (name == "a")
		for (temp = 0; temp < acol; temp++)
			ret = (i + "$" + string temp , name + "$" + j + "$" + num) :: ret;
	else
		for (temp = 0; temp < acol; temp++)
			ret = (string temp + "$" + j , name + "$" + i + "$" + num) :: ret;
	return ret;
}

combine(key : string, values : list of string) : (string, string)
{
	return (nil , nil);
}

keySpaceSize() : int
{
	return arow;
}

hashKey(key : string) : int
{
	(nil , words) := sys->tokenize(key , "$");
	return int (hd words);
}
