implement List;

include "sys.m";
include "draw.m";
include "dfsclient.m";
include "dfsutil.m";
include "arg.m";

sys : Sys;
dfsclient : DFSClient;
dfsutil : DFSUtil;
arg : Arg;

DFSFile : import dfsutil;
DFSChunk : import dfsutil;
DFSNode : import dfsutil;

List : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	dfsutil = load DFSUtil DFSUtil->PATH;
	dfsclient = load DFSClient DFSClient->PATH;
	arg = load Arg Arg->PATH;

	dfsutil->init();
	dfsclient->init();

	lflag := 0;
#	arg->init(args);
#	while ((c := arg->opt()) != 0)
#	case c {
#			'l' => lflag = 1;
#			* => {
#				sys->print("unknown option (%c)\n", c);
#				exit;
#			}
#		}
	
	strSrc := dfsclient->listFiles();
#	strList := sys->tokenize(strSrc, "\n");

	sys->print("%s", strSrc);
}	
	
	
	





