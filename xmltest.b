implement XmlTest;

include "sys.m";
include "draw.m";
include "xml.m";
include "bufio.m";

Parser : import xml;
Item : import xml;

sys : Sys;
xml : Xml;

XmlTest : module {
	init : fn(ctxt : ref Draw->Context, args : list of string);
};

init(ctxt : ref Draw->Context, args : list of string)
{
	sys = load Sys Sys->PATH;
	xml = load Xml Xml->PATH;

	xml->init();
	warning := chan of (Xml->Locator, string);
	preelem : string;
	(parser, err) := xml->open("k.xml", warning, preelem);
	if (parser == nil)
		sys->print("error : %s", err);
	item := parser.next();
	pick it := item {
		Tag => {
			sys->print("%s",it.name);
		}
		* => {
			sys->print("not Tag\n");
		}
	}

	sys->create("kkk", sys->ORDWR, 8r600);
	sys->remove("kkk");
	sys->remove("k.xml");
}
