implement TestMapper;

include "sys.m";
include "draw.m";
include "mrutil.m";
include "ioutil.m";
include "mapperworker.m";

sys : Sys;
mapperworker : MapperWorker;
mrutil : MRUtil;
ioutil : IOUtil;

MapperTask : import mrutil;
FileBlock : import ioutil;

mapperTask : ref MapperTask;

TestMapper : module
{
	init:	fn(ctxt: ref Draw->Context, args: list of string);
};

init(ctxt: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	mapperworker = load MapperWorker MapperWorker->PATH;
	mrutil = load MRUtil MRUtil->PATH;
	ioutil = load IOUtil IOUtil->PATH;

	mrutil->init();
	ioutil->init();

	mapperTask = ref MapperTask(12,0,mrutil->PENDING,0,"home",122,"wordcount.dis",ref FileBlock(hd(tl args), big 0, 1000),5);
	
	mapperworker->init(mapperTask);
}