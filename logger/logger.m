########################################
#
#	Logger module set the log file and write logs into the file and on the screen
#	when the DFS and MapReduce jobs are working.
#	The log file can record the state of the work and recover the work if it ends in failure.
#	
#	@author Xinyang Shao(daniel)
#
########################################

Logger : module {
	PATH : con "/appl/MR/logger/logger.dis";
	
	OK : con 1;
	ERR : con 0;

	BLANK : con 0;
	INFO : con 1;
	WARN : con 2;
	ERROR : con 3;

	DEFAULT_LOG_FILE_NAME : con "log.txt";

	init : fn();

	setFileName : fn(newFileName : string);

	bl : fn() : int;
	blankLine : fn() : int;

	sln : fn() : int;
	sepaLine : fn() : int;

	li : fn(message : string) : int;
	logInfo : fn(message : string) : int;

	l : fn(message : string, property : int) : int;
	log : fn(message : string, property : int) : int;

	sbl : fn() : int;
	scrBlankLine : fn() : int;

	ssl : fn() : int;
	scrSepaLine : fn() : int;

	sli : fn(message : string) : int;
	scrlogInfo : fn(message : string) : int;

	sl : fn(message : string, property : int) : int;
	scrlog : fn(message : string, property : int) : int;
};
