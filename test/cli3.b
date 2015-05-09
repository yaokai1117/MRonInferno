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

	(n, conn) := sys->dial("tcp!127.0.0.1!2334", nil);
	p := args;
	p = tl p;
	sys->write(conn.dfd, array of byte hd p, len hd p);
	sys->sleep(1000);
	sys->mount(conn.dfd, nil, "/usr/yaokai/cli/remote", Sys->MCREATE, nil);
	datacp := sys->create("/usr/yaokai/cli/remote/" + "1024", Sys->ORDWR, 8r600);
	sys->unmount(nil, "/usr/yaokai/cli/remote/");
	sys->print("over\n");
}
