FileLogger : module {
	PATH : con "/usr/daniel/MRonInferno-master/filelogger.dis";
	OK : con 1;
	ERR : con 0;

	DEFAULT_LOG_FILE_NAME : con "log.txt";

	init : fn();

	setFileName : fn(newFileName : string);

	log : fn(message : string) : int;
};