implement Client;

include "sys.m";
include "draw.m";
include "tk.m";
include "tkclient.m";
include "client.m";

#include "dfsnodeserver.m";

sys : Sys;
draw : Draw;
tk : Tk;
tkclient : Tkclient;

toplevel : ref Tk->Toplevel;
winctl : chan of string;

perc := 0;

users : array of UserState[MAX_USER_NUM];
usernum := 0;

init(ctxt : ref Draw->Context, nil : list of string)
{
	sys = load Sys Sys->PATH;
	draw = load Draw Draw->PATH;
	tk = load Tk Tk->PATH;
	tkclient = load Tkclient Tkclient->PATH;

	userstate : ref UserState;

	tkclient->init();
	(toplevel, winctl) = tkclient->toplevel(ctxt, nil, "Map-Reduce System", Tkclient->Appl);

	cmd := chan of string;
	tk->namechan(toplevel, cmd, "cmd");

	tk->cmd(toplevel, "frame .info -height 400 -width 200");
	tk->cmd(toplevel, "label .info.self -height 200 -width 200 -text {Name : Test}");
	tk->cmd(toplevel, "label .info.all -height 200 -width 200 -text {IP : 192.168.1.43}");
	tk->cmd(toplevel, "pack .info.self .info.all -side top -fill x");

	tk->cmd(toplevel, "frame .show -height 350 -width 400");
	tk->cmd(toplevel, "label .show.prog -height 50 -width 400 -text {Progress Percentage : 0%}");
	tk->cmd(toplevel, "canvas .show.perc -height 50 -width 400 -background white");

	tk->cmd(toplevel, "frame .show.text");
	tk->cmd(toplevel, "text .t -yscrollcommand {.scroll set} -bg white -height 250 -width 385");
	tk->cmd(toplevel, "scrollbar .scroll -command {.t yview}");
	tk->cmd(toplevel, "pack .scroll -side left -fill y -in .show.text");
	tk->cmd(toplevel, "pack .t -side right -in .show.text -expand 1 -fill both");


	tk->cmd(toplevel, "frame .show.cmd1 -height 25 -width 400");
	tk->cmd(toplevel, "frame .show.cmd2 -height 25 -width 400");
	tk->cmd(toplevel, "button .show.cmd1.connect -text {Connect} -command {send cmd connect}");
	tk->cmd(toplevel, "button .show.cmd1.submit -text {Submit} -command {send cmd submit}");
	tk->cmd(toplevel, "button .show.cmd2.start -text {Start} -command {send cmd start}");
	tk->cmd(toplevel, "button .show.cmd2.quit -text {Quit} -command {send cmd quit}");

	changePerc(99);

	tk->cmd(toplevel, "pack .show.cmd1.connect .show.cmd1.submit -side left -fill x -expand 1");
	tk->cmd(toplevel, "pack .show.cmd2.start .show.cmd2.quit -side left -fill x -expand 1");
	tk->cmd(toplevel, "pack .show.prog .show.perc .show.text .show.cmd1 .show.cmd2 -side top -fill x");

	tk->cmd(toplevel, "pack .info .show -side left -fill x -expand 1");
	tk->cmd(toplevel, "update");

	tkclient->startinput(toplevel, "ptr" :: "kbd" :: nil);
	tkclient->onscreen(toplevel, nil);

	for (;;) {
		alt {
			s := <-cmd =>
			(nil, cmdstr) := sys->tokenize(s, " \t\n");

			case (hd cmdstr)
 			{
				"quit" =>
					exit;

				"connect" =>
					connect();

				"submit" =>
					submit();

				"start" =>
					start();
            }

			p := <-toplevel.ctxt.ptr =>
				tk->pointer(toplevel, *p);

			c := <-toplevel.ctxt.kbd =>
				tk->keyboard(toplevel, c);

            ctl := <-winctl or
        	ctl = <-toplevel.ctxt.ctl or
        	ctl = <-toplevel.wreq =>
            	tkclient->wmctl(toplevel, ctl);
        }
        tk->cmd(toplevel, "update");
    }
}

UserState.set(userstate : self ref UserState, name : string, id : int, regtime : big, ip : string, pic : ref Draw->Image)
{
	userstate.name = name;
	userstate.id = id;
	userstate.regtime = regtime;
	userstate.ip = ip;
	userstate.pic = pic;
}

start()
{
	sys->print("Start button is pressed\n");	# debug info
	appendLog("[INFO] Start button is pressed\n");
}

connect()
{
	sys->print("Connect button is pressed\n");	# debug info
	appendLog("[INFO] Connect button is pressed\n");
}

submit()
{
	sys->print("Submit button is pressed\n");		# debug info
	appendLog("[INFO] Submit button is pressed\n");
}

changePerc(newPerc : int)
{
	perc = newPerc;
	perc_string := sys->sprint(".show.prog configure -text {Progress Percentage : %d%%}", perc);
	tk->cmd(toplevel, perc_string);
	loc_string := sys->sprint("0 0 %d 49", 4 * perc);
	tk->cmd(toplevel, ".show.perc create rectangle 0 0 450 49 -outline white -fill white");
	tk->cmd(toplevel, ".show.perc create rectangle " + loc_string + " -outline black -fill black");
}

appendLog(log : string)
{
	if(toplevel == nil)
	{
		sys->print("Unexpected Error!");
		exit;
	}
	if(log == nil)		return;
	tk->cmd(toplevel, ".t insert end '" + log);
}

syncUsers() 
{
	
}