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

msg2mapper(msg : list of string) : ref MapperTask
{
	id := int (hd tl msg);
	jobId := int (hd tl msg);
	status := int (hd tl msg);
	attemptCount := int (hd tl msg);
	taskTrackerAddr := (hd tl msg);
	taskTrackerPort := int (hd tl msg);
	mrClassName := (hd tl msg);
	reducerAmount := int (hd tl msg);
	fileName := (hd tl msg);
	offset := big (hd tl msg);
	size := int (hd tl msg);
	fileBlock := ref IOUtil->FileBlock(fileName, offset, size);	
	return ref MapperTask(id, jobId, status, attemptCount, taskTrackerAddr, taskTrackerPort, mrClassName, reducerAmount, fileBlock);
}

msg2reducer(msg : list of string) : ref ReducerTask
{
	id := int (hd tl msg);
	jobId := int (hd tl msg);
	status := int (hd tl msg);
	attemptCount := int (hd tl msg);
	taskTrackerAddr := (hd tl msg);
	taskTrackerPort := int (hd tl msg);
	mrClassName := (hd tl msg);
	mapperAmount := int	(hd tl msg);
	partitionIndex := int (hd tl msg);
	outputFile := (hd tl msg);
	outputRep := int (hd tl msg);
	outputSize := int (hd tl msg);
	return ref ReducerTask(id, jobId, status, attemptCount, taskTrackerAddr, taskTrackerPort, mrClassName, mapperAmount, partitionIndex, outputFile, outputRep, outputSize);	
}
