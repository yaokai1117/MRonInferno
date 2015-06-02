implement Download;

include "sys.m";
include "draw.m";
include "arg.m";
include "dfsutil.m";
include "dfsclient.m";
include "sort.m";
include "lists.m";
include "download.m";

sys : Sys;
arg : Arg;
dfsutil : DFSUtil;
dfsclient : DFSClient;
sort : Sort;
lists : Lists;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;
DFSChunkCmp : import dfsutil;

dataPath := string "/usr/yaokai/cli/";

init(ctxt : ref Draw->Context, args : list of string) 
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsclient = load DFSClient DFSClient->PATH;
	arg = load Arg Arg->PATH;
	lists = load Lists Lists->PATH;

	dfsutil->init();
	dfsclient->init();
	
	offset := big 0;
	size := big 0;
	arg->init(args);
	while ((c := arg->opt()) != 0)
		case c {
			'o' => offset = big arg->arg();
			's' => size = big arg->arg();
			* => {
				sys->print("unknown option (%c)\n", c);
				exit;
			}
		}
	args = arg->argv();
	fileName := hd args;
	file := dfsclient->getFile(fileName);
	total := (lists->last(file.chunks)).offset + big (lists->last(file.chunks)).size;
	if (offset > total)
		offset = big 0;
	if (size == big 0 || size > total - offset)
		size = total - offset;
	fd := sys->create(dataPath + fileName, Sys->ORDWR, 8r600);		
	download(fd, file, offset, size);	
}

download(fd : ref Sys->FD, file : ref DFSFile, offset : big, size : big)
{
	if (dfsclient == nil)
		dfsclient = load DFSClient DFSClient->PATH;
	if (dfsutil == nil) {
		dfsutil = load DFSUtil DFSUtil->PATH;
		dfsutil->init();
	}
	if (sys == nil)
		sys == load Sys Sys->PATH;
	sort = load Sort Sort->PATH;

	dfsclient->init();

	chunks := file.getChunks();
	cmp := ref DFSChunkCmp();
	sort->sort(cmp, chunks);

	i := 0; 
	while (i < len chunks && chunks[i].offset + big chunks[i].size < offset)
		i++;
	while (i < len chunks && chunks[i].offset < offset + size) {
		tempfd := dfsclient->readChunk(chunks[i]);
		start := big 0;
		buf := array [Sys->ATOMICIO] of byte;

		if (chunks[i].offset < offset)
			start = sys->seek(tempfd, offset - chunks[i].offset, Sys->SEEKSTART);

		if (chunks[i].offset + big chunks[i].size > offset + size) {
			total := int (offset + size - chunks[i].offset - start);	
			while (total > len buf) {
				sys->read(tempfd, buf, len buf);
				sys->write(fd, buf, len buf);
				total -= len buf;
			}
			if (total > 0) {
				sys->read(tempfd, buf[:total], total);
				sys->write(fd, buf[:total], total);
			}
		}
		else while ((length := sys->read(tempfd, buf, len buf)) != 0)
			sys->write(fd, buf, length);
		sys->remove(sys->fd2path(tempfd));
		i++;
	}
}









		
