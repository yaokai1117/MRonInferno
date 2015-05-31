implement testLogger;

include "sys.m";
include "draw.m";
include "logger.m";

sys : Sys;
logger : Logger;

testLogger : module {
	init: fn(nil : ref Draw->Context, nil : list of string);
};

init(nil : ref Draw->Context, nil : list of string)
{
	sys = load Sys Sys->PATH;
	logger = load Logger Logger->PATH;

	logger->init();

	logger->logInfo("This is the test message.");
	logger->logInfo("This is the test message2.");
	logger->sepaLine();
	logger->logInfo("This is the test message3 below the separation line.");

	logger->sepaLine();
	logger->log("This is the warning message.", Logger->WARN);
	logger->blankLine();
	logger->log("This is the error message.", Logger->ERROR);

	logger->setFileName("new log.txt");
	logger->logInfo("This is the new test message for new log.");
}