implement MRUtil;

include "mrutil.m";
include "ioutil.m";
include "sys.m";
sys : Sys;

init()
{
	sys = load Sys Sys->PATH;
}


MapperTask.toString(mp : self ref MapperTask) : string
{
	ret := sys->sprint("id: %d, jobId: %d, status: %d, reducerAmount: %d, attempt: %d\n", mp.id, mp.jobId, mp.status, mp.reducerAmount, mp.attemptCount);
	return ret;
}

MapperTask.eq(a, b : ref MapperTask) : int
{
	return a.id == b.id;
}

ReducerTask.toString(rd : self ref ReducerTask) : string
{
	ret := sys->sprint("id: %d, jobId: %d, status: %d, mapperAmount: %d, attempt: %d\n", rd.id, rd.jobId, rd.status, rd.mapperAmount, rd.attemptCount);
	return ret;
}

ReducerTask.eq(a, b : ref ReducerTask) : int
{
	return a.id == b.id;
}

TaskTrackerInfo.toString(tt : self ref TaskTrackerInfo) : string
{
	ret := sys->sprint("addr: %s, port: %d, mapperTaskNum: %d, reducerTaskNum: %d, isWorking: %d", tt.addr, tt.port, tt.mapperTaskNum, tt.reducerTaskNum, tt.isWorking);
	return ret;
}
