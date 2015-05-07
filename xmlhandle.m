XmlHandle : module {
	PATH : con "/usr/yaokai/xmlhandle.dis";

# write the data of DFS adts to xml files

	file2xml : fn(xmlf : ref Sys->FD, file : ref DFSUtil->DFSFile);
	chunk2xml : fn(xmlf : ref Sys->FD, chunk : ref DFSUtil->DFSChunk);
	node2xml : fn(xmlf : ref Sys->FD, node : ref DFSUtil->DFSNode);

	
# parse a xml file to DFS adts

	init : fn(fileName : string) : ref Xml->Parser;

	# read <name>text</name> to (name, text), if finding <parent><chile>...</child></parent>, return (parent, child) and go deep
	read : fn(parser : ref Xml->Parser) : (string, string);

	xml2file : fn(parser : ref Xml->Parser) : ref DFSUtil->DFSFile;
	xml2chunk : fn(parser : ref Xml->Parser, fileId : int) : ref DFSUtil->DFSChunk;
	xml2node : fn(parser : ref Xml->Parser) : ref DFSUtil->DFSNode;
	
};
