implement MRUtil;

include "mrutil.m";
include "ioutil.m";
include "tables.m";
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

mapper2msg(mapper : ref MapperTask) : string
{
	msg := "mapper@" + string mapper.id + "@" + string mapper.jobId +
			"@" + string mapper.status + "@" + string mapper.attemptCount + 
			"@" + mapper.taskTrackerAddr + "@" + string mapper.taskTrackerPort + 
			"@" + mapper.mrClassName + "@" + string mapper.reducerAmount + 
			"@" + mapper.inputFileBlock.fileName + "@" + string mapper.inputFileBlock.offset + "@" + string mapper.inputFileBlock.size;
	return msg;
}

reducer2msg(reducer : ref ReducerTask) : string
{
	msg := "reducer@" + string reducer.id + "@" + string reducer.jobId + 
			 "@" + string reducer.status + "@" + string reducer.attemptCount + 
			 "@" + reducer.taskTrackerAddr + "@" + string reducer.taskTrackerPort + 
			 "@" + reducer.mrClassName + "@" + string reducer.mapperAmount + "@" +  string reducer.partitionIndex + 
			 "@" + reducer.outputFile + "@" + string reducer.outputRep + "@" + string reducer.outputSize;
	return msg;
}

tracker2msg(tracker : ref TaskTrackerInfo) : string
{
	msg := "tracker@" + tracker.addr + "@" + string tracker.port + 
			 "@" + string tracker.mapperTaskNum + "@" + string tracker.reducerTaskNum +
			 "@" + string tracker.isWorking;
	return msg;
}


msg2mapper(msg : list of string) : ref MapperTask
{
	msg = tl msg;
	id := int (hd msg); msg = tl msg;
	jobId := int (hd msg); msg = tl msg;
	status := int (hd msg); msg = tl msg;
	attemptCount := int (hd msg); msg = tl msg;
	taskTrackerAddr := (hd msg); msg = tl msg;
	taskTrackerPort := int (hd msg); msg = tl msg;
	mrClassName := (hd msg); msg = tl msg;
	reducerAmount := int (hd msg); msg = tl msg;
	fileName := (hd msg); msg = tl msg;
	offset := big (hd msg); msg = tl msg;
	size := int (hd msg); msg = tl msg;
	fileBlock := ref IOUtil->FileBlock(fileName, offset, size);	
	return ref MapperTask(id, jobId, status, attemptCount, taskTrackerAddr, taskTrackerPort, mrClassName, reducerAmount, fileBlock);
}

msg2reducer(msg : list of string) : ref ReducerTask
{
	msg = tl msg;
	id := int (hd msg); msg = tl msg;
	jobId := int (hd msg); msg = tl msg;
	status := int (hd msg); msg = tl msg;
	attemptCount := int (hd msg); msg = tl msg;
	taskTrackerAddr := (hd msg); msg = tl msg;
	taskTrackerPort := int (hd msg); msg = tl msg;
	mrClassName := (hd msg); msg = tl msg;
	mapperAmount := int	(hd msg); msg = tl msg;
	partitionIndex := int (hd msg); msg = tl msg;
	outputFile := (hd msg); msg = tl msg;
	outputRep := int (hd msg); msg = tl msg;
	outputSize := int (hd msg); msg = tl msg;
	return ref ReducerTask(id, jobId, status, attemptCount, taskTrackerAddr, taskTrackerPort, mrClassName, mapperAmount, partitionIndex, outputFile, outputRep, outputSize);	
}

msg2tracker(msg : list of string) : ref TaskTrackerInfo
{
	msg = tl msg;
	addr := hd msg; msg = tl msg;
	port := int (hd msg); msg = tl msg;
	mapperTaskNum := int (hd msg); msg = tl msg;
	reducerTaskNum := int (hd msg); msg = tl msg;
	isWorking := int (hd msg);
	return ref TaskTrackerInfo(addr, port, mapperTaskNum, reducerTaskNum, isWorking);
}


