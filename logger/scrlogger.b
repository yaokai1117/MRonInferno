implement ScrLogger;

include "sys.m";
include "draw.m";
include "scrlogger.m";

sys : Sys;

log(message : string)
{
	sys = load Sys Sys->PATH;
	sys->print("[DEBUG INFO] %s\n", message);
}
