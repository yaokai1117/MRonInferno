###########################
#  
#   Api for ui;
# 	@author: Xing Yang Shao
#
##########################

Client : module {
	PATH : con "usr/daniel/cli/client.dis";

	MAX_USER_NUM : con 10;

	UserState : adt {
		name : string;
		id : int;
		regtime : big;
		ip : string;
		admin : int;
		#	pic : ref Draw->Image;

		set : fn(userstate : self ref UserState, name : string, id : int, regtime : big, ip : string, admin : int);
	};

	init : fn(ctxt : ref Draw->Context, nil : list of string);

	changePerc : fn(perc : int);
};
