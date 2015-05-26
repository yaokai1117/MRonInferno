implement JobTrackerServer;

include "sys.m";
include "draw.m";
include "mrutil.m";
include "jobtracker.m";

JobTrackerServer : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string) {

};

