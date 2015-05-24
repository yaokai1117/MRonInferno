implement FileLogger;

include "sys.m";
include "draw.m";
include "scrlogger.m";
include "filelogger.m";

sys : Sys;
fileName : string;
FD : import sys;
log_fd : ref Sys->FD;

scrlogger : ScrLogger;

init() 
{
	fileName = DEFAULT_LOG_FILE_NAME;
	sys = load Sys Sys->PATH;
	scrlogger = load ScrLogger ScrLogger->PATH;
}

setFileName(newFileName : string) 
{
	fileName = newFileName;
	log_fd = nil;

	scrlogger->log("Log file changing to : \"" + fileName + "\"");
}

log(message : string) : int
{
	if(log_fd == nil) {
		log_fd = sys->open(fileName, Sys->OWRITE);

		if(log_fd == nil) {	# File Not Found
			log_fd = sys->create(fileName, sys->ORDWR, 8r777);
			scrlogger->log("Explicit-name log file \"" + fileName + "\" NOT found. Automatically creating...\n");

			if(log_fd == nil) {
				scrlogger->log("Log file \"" + fileName + "\" creation failed.");
				return ERR;
			}
			else {
				scrlogger->log("Log file \"" + fileName + "\" creation successful");
			}
		}
	}

	sys->fprint(log_fd, "%s\n", message);
	return OK;
}