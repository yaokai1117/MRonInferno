implement XmlTest;

include "sys.m";
include "draw.m";
include "xml.m";
include "bufio.m";
include "dfsutil.m";
include "xmlhandle.m";

Parser : import xml;
Item : import xml;
DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;

sys : Sys;
xml : Xml;
dfsutil : DFSUtil;
xmlhd : XmlHandle;

XmlTest : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	xml = load Xml Xml->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	xmlhd = load XmlHandle XmlHandle->PATH;

	xml->init();
	dfsutil->init();
	parser := xmlhd->init("AAA.xml"); 
	file := xmlhd->xml2file(parser);
	sys->print("%s", file.toString());
	for (p := file.chunks; p != nil; p = tl p) {
		chunk := hd p;
		sys->print("\t%s", chunk.toString());
		for (q := chunk.nodes; q != nil; q = tl q)
			sys->print("\t\t%s", (hd q).toString());
	}
}	

