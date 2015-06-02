implement IOUtil;

include "sys.m";
include "ioutil.m";
include "filelogger.m";

sys : Sys;
filelogger : FileLogger;

Connection : import sys;
FD : import sys;

init()
{
	sys = load Sys Sys->PATH;
}

split(fileName : string, number : int) : list of FileBlock
{
	return nil;
}

sendRemoteFile(port : int, fd : ref FD)
{
	(nil, dir) := sys->fstat(fd);
	fileName := dir.name;
	totalPath := sys->fd2path(fd);
	filePath := totalPath[:len totalPath-len fileName];

	(n, c) := sys->announce("tcp!*!" + string port);
	if (n < 0) {
		filelogger->log("Error:IOUtil->sendRemoteFile: announce failed %r\n");
		exit;
	}

	while(1)
	{
		sys->seek(fd, big 0, sys->SEEKSTART);

		(ok, conn) := sys->listen(c);
		if (ok < 0) {
			filelogger->log("Error:IOUtil->sendRemoteFile: listen failed %r\n");
			exit;
		}

		dfd := sys->open(conn.dir + "/data", sys->ORDWR);
		sys->write(dfd,array of byte fileName,len fileName);
		sys->mount(dfd, nil, filePath + "remote",Sys->MCREATE, nil);
		copyfd := sys->create(filePath + "remote/" + fileName, sys->ORDWR, 8r600);

		buf := array [Sys->ATOMICIO] of byte;
		length : int;
		do {
			length = sys->read(fd, buf, len buf);
			sys->write(copyfd, buf[:length], length);
		}while (length == len buf);

		sys->unmount(nil, filePath + "remote");
	}
}

getRemoteFile(addr : string, port : int, destPath : string) : ref FD
{
	(ok, conn) := sys->dial("tcp!" + addr + "!" + string port, nil);
	if (ok < 0)
	{
		filelogger->log("Error:IOUtil->getRemoteFile--dial failed %r\n");
		return nil;
	}

	buf := array [sys->ATOMICIO] of byte;
	length := sys->read(conn.dfd, buf, len buf);
	fileName := string buf[:length];

	sys->export(conn.dfd, destPath, Sys->EXPWAIT);

	ret := sys->open(destPath + fileName, sys->ORDWR);

	return ret;

}

splitLine(line : string) : (string ,string)
{
	(number, words) := sys->tokenize(line, "@");
	if(number<2)
		return (line, nil);
	else
		return (hd words,hd (tl words));
}

mergeSortedFiles(files : list of string, outputFile : string)
{

}