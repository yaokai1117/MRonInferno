implement SimpleServer;

include "sys.m";
include "draw.m";

sys : Sys;
Connection : import Sys;

SimpleServer : module
{
	init : fn (ctxt : ref Draw->Context, nil : list of string);
};

init (ctxt : ref Draw->Context, nil : list of string)
{
	sys = load Sys Sys->PATH;
	(n, conn) := sys->announce("tcp!*!2333");
	if (n < 0)
	{
		sys->print("SimpleServer - announce failed : %r\n");
		exit;
	}
	while (1)
	{
		listen(conn, ctxt);
	}
}

listen (conn : Connection, ctxt : ref Draw->Context)
{
	buf := array [sys->ATOMICIO] of byte;
	(ok, c) := sys->listen(conn);
	if (ok < 0)
	{
		sys->print("SimpleServer - listen failed : %r\n");
		exit;
	}
	rfd := sys->open(conn.dir + "/remote", Sys->OREAD);
	n := sys->read(rfd, buf, len buf);
	spawn hdlrthread(c, ctxt);
}

hdlrthread (conn : Connection, ctxt : ref Draw->Context)
{
	addr := array [sys->ATOMICIO] of byte;
	msg := array [sys->ATOMICIO] of byte;
	
	rdfd := sys->open(conn.dir + "/data", Sys->OREAD);
	wdfd := sys->open(conn.dir + "/data", Sys->OWRITE);
	rfd := sys->open(conn.dir + "/remote", Sys->OREAD);

	length1 := sys->read(rfd, addr, len addr);
	sys->print("SimpleServer - Got new connection from %s\n", string addr[:length1]);

	sys->write(wdfd, array of byte "Here is yaokai listening\n", len "Here is yaokai listening\n");

	fd := sys->open(conn.dir + "/data", Sys->ORDWR);
	sys->export(fd, "/usr/yaokai/ser", Sys->EXPASYNC);
}






