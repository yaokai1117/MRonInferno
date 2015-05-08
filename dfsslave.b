implement DFSSlave;

include "sys.m";
include "draw.m";
include "dfsslave.m";

sys : Sys;

FD : import sys;

dataPath : con "/usr/fyabc/slv/";
homePath : con "/usr/fyabc/";
servPath : con "/usr/fyabc/slvser/";

init()
{
	sys = load Sys Sys->PATH;
}

read(chunkId : int, offset : big, size : int) : ref FD
{
	data := sys->create(servPath + "datacopy_" + string chunkId, sys->ORDWR, 8r600);
	dataSource := sys->open(dataPath + string chunkId, sys->OREAD);

	if(dataSource != nil)
	{
		buf := array[1<<20] of byte;
		length := sys->pread(dataSource, buf, size, offset);
		if (length > 0)
		{
			if(sys->pwrite(data, buf, length, big 0) != 0){
				return data;
			}
			else{
				return nil;
			}
		}
		else
		{
			return nil;
		}
	}
	else
	{
		return nil;
	}
}

write(chunkId : int, offset : big,size : int, data : ref FD) : int
{
	dataDestination := sys->open(dataPath + string chunkId, sys->OWRITE);

	if(dataDestination != nil)
	{
		buf : array [1<<20] of byte;
		length := sys->pread(data, buf, size, big 0);
		if(length > 0)
		{
			if(sys->pwrite(dataDestination, buf, length, offset) != 0)
				return 0;
			else
				return -1;
		}
		else
		{
			return -1;
		}
	}
	else
	{
		return -1;
	}
}

delete(chunkId : int) : int
{
	return sys->remove(dataPath + string chunkId);
}

linesOffset(chunkId : int) : array of big
{
	return nil;
}

