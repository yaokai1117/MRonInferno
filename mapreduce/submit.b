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

	fd := sys->open("/appl/MR/jobconfig", Sys->OREAD);
	buf2 := array [Sys->ATOMICIO] of byte;
	confLen := sys->read(fd, buf2, len buf);
	for (i := 0; i < len buf2; i++)
		if (buf2[i] == byte '\n')
			buf2[i] = byte '@';

	(n, conn) := sys->dial("tcp!" + hostAddr + "!66666", nil);
	sys->fprint(conn.dfd, "submit@jobConfig@%s", string buf2[:confLen]);
	length := sys->read(conn.dfd, buf, len buf);
	sys->print("%s", string buf[:length]);
}
