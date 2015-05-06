implement XmlHandle;

include "sys.m";
include "dfsutil.m";
include "xmlhandle.m";

sys : Sys;

file2xml(xmlf : ref Sys->FD, file : ref DFSUtil->DFSFile) 
{
	if (sys == nil)
		sys = load Sys Sys->PATH;
	sys->fprint(xmlf, "<file>");
	sys->fprint(xmlf, "<name>%s</name>", file.name);
	sys->fprint(xmlf, "<id>%d</id>", file.id);
	sys->fprint(xmlf, "<rep>%d</rep>", file.replicas);
	for (p := file.chunks; p != nil; p = tl p)
		chunk2xml(xmlf, hd p);
	sys->fprint(xmlf, "</file>");
}

chunk2xml(xmlf : ref Sys->FD, chunk : ref DFSUtil->DFSChunk)
{
	if (sys == nil)
		sys = load Sys Sys->PATH;
	sys->fprint(xmlf, "<chunk>");
	sys->fprint(xmlf, "<id>%d</id>", chunk.id);
	sys->fprint(xmlf, "<offset>%bd</offset>", chunk.offset);
	sys->fprint(xmlf, "<size>%d</size>", chunk.size);
	for (p := chunk.nodes; p != nil; p = tl p)
		node2xml(xmlf, hd p);
	sys->fprint(xmlf, "</chunk>");
}
	
node2xml(xmlf : ref Sys->FD, node : ref DFSUtil->DFSNode)
{
	if (sys == nil)
		sys = load Sys Sys->PATH;
	sys->fprint(xmlf, "<node>");
	sys->fprint(xmlf, "<a>%s</a>", node.addr);
	sys->fprint(xmlf, "<p>%d</p>", node.port);
	sys->fprint(xmlf, "<c>%d</c>", node.chunkNumber);
	sys->fprint(xmlf, "</node>");
}

