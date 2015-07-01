DFSUtil :  module {
	PATH : con "/appl/MR/dfs/dfsutil.dis";
	
	# load ths Sys and Lists module, must be called before using toString, removeChunk and removeNode
	init : fn();

	# the DFSNode adt
	DFSNode : adt {
		addr : string;
		port : int;
		chunkNumber : int;

		toString : fn(dn : self ref DFSNode) : string;
		eq : fn(a, b : ref DFSNode) : int;
	};

	# comparator of DFSNode
	DFSNodeCmp : adt {
		gt : fn(dnc : self ref DFSNodeCmp, x, y : ref DFSNode) : int;
	};

	# the DFSChunk adt
	DFSChunk : adt {
		id : int;
		fileId : int;
		offset : big;
		size : int;
		nodes : list of ref DFSNode;
		
		getNodes : fn(dc : self ref DFSChunk) : array of ref DFSNode;

		addNode : fn(dc : self ref DFSChunk, node : ref DFSNode);
		removeNode : fn(dc : self ref DFSChunk, node : ref DFSNode);
		toString : fn(dc : self ref DFSChunk) : string;
		eq : fn(a, b : ref DFSChunk) : int;
	};

	# comparator of DFSChunk
	DFSChunkCmp : adt {
		gt : fn(dcc : self ref DFSChunkCmp, x, y : ref DFSChunk) : int;
	};	  

	# the DFSFile adt
	DFSFile : adt {
		id : int;
		name : string;
		chunks : list of ref DFSChunk; 	  
		replicas : int;

		getChunks : fn(df : self ref DFSFile) : array of ref DFSChunk;

		addChunk : fn(df : self ref DFSFile, chunk : ref DFSChunk);
		removeChunk : fn(df : self ref DFSFile, chunk : ref DFSChunk);
		toString : fn(df : self ref DFSFile) : string;
		eq : fn(a, b : ref DFSFile) : int;
	};
	
};
	
