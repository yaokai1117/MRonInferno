XmlHandle : module {
	PATH : con "/usr/yaokai/xmlhandle.dis";

# write the data of DFS adts to xml files
	file2xml : fn(xmlf : ref Sys->FD, file : ref DFSUtil->DFSFile);
	chunk2xml : fn(xmlf : ref Sys->FD, chunk : ref DFSUtil->DFSChunk);
	node2xml : fn(xmlf : ref Sys->FD, node : ref DFSUtil->DFSNode);
	
};
