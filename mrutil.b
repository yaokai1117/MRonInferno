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

ReducerTask.toString(rd : self ref ReducerTask) : string
{
	ret := sys->sprint("id: %d, jobId: %d, status: %d, mapperAmount: %d, attempt: %d\n", rd.id, rd.jobId, rd.status, rd.mapperAmount, rd.attemptCount);
	return ret;
}

TaskTrackerInfo.toString(tt : self ref TaskTrackerInfo) : string
{
	ret := sys->sprint("addr: %s, port: %d, mapperTaskNum: %d, reducerTaskNum: %d", tt.addr, tt.port, tt.mapperTaskNum, tt.reducerTaskNum);
	return ret;
}
