Download : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
	download : fn(fd : ref Sys->FD, file : ref DFSUtil->DFSFile, offset : big, size : big);
};
