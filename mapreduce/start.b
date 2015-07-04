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
	sys->write(conn.dfd, array of byte "start@0", len "start@0");
	length := sys->read(conn.dfd, buf, len buf);
	sys->print("%s", string buf[:length]);
}
