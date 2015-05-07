implement SimpleClient;

include "sys.m";
include "draw.m";

sys : Sys;
Connection : import Sys;

SimpleClient : module
{
	init : fn (ctxt : ref Draw->Context, args : list of string);
};

init (ctxt : ref Draw->Context, args : list of string)
{
	buf := array [Sys->ATOMICIO] of byte; 
	sys = load Sys Sys->PATH;

	(n, conn) := sys->dial("tcp!127.0.0.1!2333", nil);
	p := args;
	p = tl p;
	sys->write(conn.dfd, array of byte hd p, len hd p);
	length := sys->read(conn.dfd, buf, len buf);
	sys->print("%s", string buf[:length]);
}
