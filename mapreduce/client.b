########################################
#
#	The implemention of SimpleClient.
#	SimpleClient is the client side of the MapReduce framework.
#	The user of the MapReduce framework use this program to submit a job and start the job.
#	
#	@author Kai Yao(yaokai)
#	@author Yang Fan(fyabc)
#
########################################

implement SimpleClient;

include "sys.m";
include "draw.m";
include "bufio.m";

sys : Sys;
bufio : Bufio;

Iobuf : import bufio;
Connection : import sys;

SimpleClient : module
{
	init : fn (ctxt : ref Draw->Context, args : list of string);
};

init (ctxt : ref Draw->Context, args : list of string)
{
	buf := array [Sys->ATOMICIO] of byte; 
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;

	buffer := bufio->open("/appl/MR/config", Bufio->OREAD);
	hostAddr := buffer.gets('\n');
	hostAddr = hostAddr[: len hostAddr - 1];

	(n, conn) := sys->dial("tcp!" + hostAddr + "!66666", nil);
	p := args;
	p = tl p;
	sys->write(conn.dfd, array of byte hd p, len hd p);
	length := sys->read(conn.dfd, buf, len buf);
	sys->print("%s", string buf[:length]);
}
