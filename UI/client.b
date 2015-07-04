#########################
#
#  A Graphical user interface for our framework, still in implementation
#
#  @author: XingYang Shao(Daniel)
#####################

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

users := array [MAX_USER_NUM] of ref UserState;
usernum := 0;

hasAdmin := 0;

init(ctxt : ref Draw->Context, nil : list of string)
{
	sys = load Sys Sys->PATH;
	draw = load Draw Draw->PATH;
	tk = load Tk Tk->PATH;
	tkclient = load Tkclient Tkclient->PATH;

	userstate := ref UserState;
	userstate.set("Daniel", 10000, big 1999, "192.168.43.99");

	tkclient->init();
	(toplevel, winctl) = tkclient->toplevel(ctxt, nil, "Client - Map-Reduce System", Tkclient->Appl);

	cmd := chan of string;
	tk->namechan(toplevel, cmd, "cmd");

	tk->cmd(toplevel, "frame .info -height 400 -width 200");
	tk->cmd(toplevel, "frame .info.self -height 100 -width 200");
	tk->cmd(toplevel, "frame .info.all -height 300 -width 200");

	tk->cmd(toplevel, "label .info.self.nm -height 20 -width 200 -text {Name : " + userstate.name + "}");
	tk->cmd(toplevel, "label .info.self.id -height 20 -width 200 -text {ID : " + sys->sprint("%d", userstate.id) + "}");
	tk->cmd(toplevel, "label .info.self.rt -height 20 -width 200 -text {Register time : " + sys->sprint("%bd", userstate.regtime) + "}");
	tk->cmd(toplevel, "label .info.self.ip -height 20 -width 200 -text {IP : " + userstate.ip + "}");
	tk->cmd(toplevel, "label .info.self.sepaln -height 20 -width 200 -text {------------------------------}");

	tk->cmd(toplevel, "pack .info.self.nm .info.self.id .info.self.rt .info.self.ip .info.self.sepaln -side top -fill x");

	for(i := 0; i < MAX_USER_NUM; i++)
	{
		cmdstr_fr := sys->sprint("frame .info.all.m%d -height 30 -width 200", i);
		cmdstr_nm := sys->sprint("label .info.all.m%d.nm -height 15 -width 200 -text {No User}", i);
		cmdstr_ip := sys->sprint("label .info.all.m%d.ip -height 15 -width 200 -text { }", i);
		cmdstr_pack := sys->sprint("pack .info.all.m%d.nm .info.all.m%d.ip -side top -fill x", i, i);
		tk->cmd(toplevel, cmdstr_fr);
		tk->cmd(toplevel, cmdstr_nm);
		tk->cmd(toplevel, cmdstr_ip);
		tk->cmd(toplevel, cmdstr_pack);
	}
	tk->cmd(toplevel, "pack .info.all.m0 .info.all.m1 .info.all.m2 .info.all.m3 .info.all.m4 .info.all.m5 .info.all.m6 .info.all.m7 .info.all.m8 .info.all.m9 -side top -fill x");

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

	changePerc(0);

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


##########################################################################################################
#
# Usage : Declare one new variable of type UserState, and then this method is used to set its values.
#
##########################################################################################################
UserState.set(userstate : self ref UserState, name : string, id : int, regtime : big, ip : string, admin : int)
{
	userstate.name = name;
	userstate.id = id;
	userstate.regtime = regtime;
	userstate.ip = ip;
	userstate.admin = admin;
	#userstate.pic = pic;
}

###########################################################################################################
#
# Interfaces for inner programs
#
###########################################################################################################
start()
{
	sys->print("Start button is pressed\n");	# debug info
	changePerc(perc + 1);
	# appendLog("[INFO] Start button is pressed\n");
}

connect()
{
	sys->print("Connect button is pressed\n");	# debug info
	newUser : ref UserState;											# for debug
	newUser = ref ("Daniel", 10000, big 1999, "192.168.43.99", 1);		# for debug

	## connect to real dfsservers


	addUser(newUser);	# for UI
	# appendLog("[INFO] Connect button is pressed\n");
}

submit()
{
	sys->print("Submit button is pressed\n");		# debug info
	deleteUser(0);
	# appendLog("[INFO] Submit button is pressed\n");
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

#######################################################################################################
# 
# Used to synchronize user information from master.
#
#######################################################################################################
syncUsers() 
{
	## Variables to be synchronized : users[MAX_USER_NUM], hasAdmin


}

#######################################################################################################
#
# Used to deal with user changes (FOR UI CHANGES, NOT FOR SYNCHRONIZATION).
#
#######################################################################################################
addUser(userstate : ref UserState)
{
	if(usernum == MAX_USER_NUM)
	{
		sys->print("Warning : Overloading users, only %d users are accepted, others will be ignored.\n", MAX_USER_NUM);
		return;
	}

	users[usernum++] = userstate;

	sys->print("%s\t%d\t%bd\t%s\n", userstate.name, userstate.id, userstate.regtime, userstate.ip);
	sys->print("%s\t%d\t%bd\t%s\n", users[usernum-1].name, users[usernum-1].id, users[usernum-1].regtime, users[usernum-1].ip);

	UpdUsrLbls();
}

deleteUser(index : int)
{
	if(usernum == 0)
	{
		sys->print("Warning : No users in list.\n");
		return;
	}
	else if(index >= usernum || index < 0)
	{
		sys->print("Error : delete user index out of range. Action ignored\n");
	}
	usernum--;

	for(i := 0; i < usernum; i++)
	{
		if(i < index)	continue;
		else
		{
			users[i] = users[i + 1];
		}
	}
	UpdUsrLbls();
}

# Update User Labels
UpdUsrLbls()
{
	for(i := 0; i < MAX_USER_NUM; i++)
	{
		if(i < usernum)
		{
			cmdstr_nm := sys->sprint(".info.all.m%d.nm configure -text {%s}", i, users[i].name);
			sys->print("%s\n", cmdstr_nm);
			tk->cmd(toplevel, cmdstr_nm);
			tk->cmd(toplevel, sys->sprint(".info.all.m%d.ip configure -text {%s}", i, users[i].ip));
		}
		else
		{
			tk->cmd(toplevel, sys->sprint(".info.all.m%d.nm configure -text {No User}", i));
			tk->cmd(toplevel, sys->sprint(".info.all.m%d.ip configure -text { }", i));
		}
	}

	tk->cmd(toplevel, "update");
}
