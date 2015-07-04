########################################
#
#	The Implement of XmlHandle.
#
#	@author Kai Yao(yaokai)
#
########################################


implement XmlHandle;

include "sys.m";
include "../dfs/dfsutil.m";
include "xml.m";
include "bufio.m";
include "xmlhandle.m";

sys : Sys;
xml : Xml;
dfsutil : DFSUtil;

DFSFile : import dfsutil;
DFSNode : import dfsutil;
DFSChunk : import dfsutil;
Parser : import xml;
Item : import xml;

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

init(fileName : string) : ref Parser
{
	if (sys == nil)
		sys = load Sys Sys->PATH;
	xml = load Xml Xml->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;

	xml->init();
	dfsutil->init();
	warning := chan of (Xml->Locator, string);
	preelem : string;
	(parser, err) := xml->open(fileName, warning, preelem);
	if (parser == nil)
		sys->print("Error: %s", err);
	parser.next();
	parser.down();
	return parser;
}

xml2file(parser : ref Parser) : ref DFSFile
{
	(nil, name) := read(parser);
	(nil, id) := read(parser);
	(nil, rep) := read(parser);
	file := ref DFSFile(int id, name, nil, int rep);
	item := parser.next();
	while(item != nil) {
		parser.down();
		chunk := xml2chunk(parser, int id);
		file.addChunk(chunk);
		item = parser.next();
	}
	parser.up();
	return file;
}

xml2chunk(parser : ref Parser, fileId : int) : ref DFSChunk
{
	(nil, id) := read(parser);
	(nil, offset) := read(parser);
	(nil, size) := read(parser);
	chunk := ref DFSChunk(int id, fileId, big offset, int size, nil); 
	item := parser.next();
	while (item != nil) {
		parser.down();
		node := xml2node(parser);
		chunk.addNode(node);
		item = parser.next();
	}
	parser.up();
	return chunk;
}	

xml2node(parser : ref Parser) : ref DFSNode
{
	(nil, addr) := read(parser);
	(nil, port) := read(parser);
	(nil, chunkNumber) := read(parser);
	node := ref DFSNode(addr, int port, int chunkNumber);
	parser.up();
	return node;
}

read(parser : ref Parser) : (string, string)
{
	name, text : string;
	item := parser.next();
	if (item == nil) 
		return (nil, nil);
	pick it := item {
		Tag => name = it.name;
		* => {
			sys->print("Parsing error, wrong depth!\n");
			return (nil, nil);
		}
	}
	parser.down();
	mark := parser.mark();
	item = parser.next();
	if (item == nil) {
		return (name, nil);
	}
	pick it2 := item {
		Tag => {
			text = it2.name;
			parser.goto(mark);	
		}
		Text => {
			text = it2.ch;
			parser.up();
		}
		* => {
			sys->print("Parsing error, wrong item!\n");
			return (nil, nil);
		}
	}
	return (name, text);
}
