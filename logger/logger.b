implement Logger;

include "sys.m";
include "draw.m";
include "logger.m";

sys : Sys;
fileName : string;
FD : import sys;
log_fd : ref Sys->FD;

init() 
{
	fileName = DEFAULT_LOG_FILE_NAME;
	sys = load Sys Sys->PATH;
}

setFileName(newFileName : string) 
{
	fileName = newFileName;
	log_fd = nil;

	scrlogInfo("Log file changing to : \"" + fileName + "\"");
}

bl() : int
{
	return blankLine();
}

sln() : int
{
	return sepaLine();
}

li(message : string) : int
{
	return logInfo(message);
}

l(message : string, property : int) : int
{
	return log(message, property);
}

sbl() : int
{
	return scrBlankLine();
}

ssl() : int
{
	return scrSepaLine();
}

sli(message : string) : int
{
	return scrlogInfo(message);
}

sl(message : string, property : int) : int
{
	return scrlog(message, property);
}

scrlogInfo(message : string) : int
{
	return scrlog(message, INFO);
}

scrlog(message : string, property : int) : int
{
	case property
	{
		INFO => sys->print("  [INFO] ");
		WARN => sys->print(" +[WARNING] ");
		ERROR => sys->print("**[ERROR] ");
	}
	sys->print("%s\n", message);
	return OK;
}

scrBlankLine() : int
{
	scrlog("\n", BLANK);
	return OK;
}

scrSepaLine() : int
{
	scrlog("\n  ------------------------------------------\n", BLANK);
	return OK;
}

logInfo(message : string) : int
{
	return log(message, INFO);
}

log(message : string, property : int) : int
{
	if(log_fd == nil) {
		log_fd = sys->open(fileName, Sys->OWRITE);

		if(log_fd == nil) {	# File Not Found
			log_fd = sys->create(fileName, sys->ORDWR, 8r777);
			scrlogInfo("Explicit-name log file \"" + fileName + "\" NOT found. Automatically creating...");

			if(log_fd == nil) {
				scrlogInfo("Log file \"" + fileName + "\" creation failed.");
				return ERR;
			}
			else {
				scrlogInfo("Log file \"" + fileName + "\" creation successful");
			}
		}
	}

	case property
	{
		INFO => sys->fprint(log_fd, "  [INFO] ");
		WARN => sys->fprint(log_fd, " +[WARNING] ");
		ERROR => sys->fprint(log_fd, "**[ERROR] ");
	}
	sys->fprint(log_fd, "%s\n", message);
	return OK;
}

blankLine() : int
{
	log("\n", BLANK);
	return OK;
}

sepaLine() : int
{
	log("\n  ------------------------------------------\n", BLANK);
	return OK;
}