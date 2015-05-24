implement testLogger;

include "sys.m";
include "draw.m";
include "filelogger.m";

sys : Sys;
logger : FileLogger;

testLogger : module {
	init: fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	logger = load FileLogger FileLogger->PATH;

	logger->init();

	logger->log("This is the test message.");
	logger->log("This is the test message2.");

	logger->setFileName("new log.txt");

	logger->log("This is the new test message for new log.");
}