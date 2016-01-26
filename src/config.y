%{
#include <stdio.h>
#include <ctype.h>
#include "newconfig.h"

int yylex(void);
int yyerror(char const *);
extern int yylineno;

HostConfig *currhost;
ConfigState *parser_cs;

%}

%start spec

%union {
   int ival;
   double dval;
   char  *sval;
}

%token <ival> T_INT
%token <ival> T_NSERVERS T_STRIPE_METHOD T_STRIPE_PARM T_SERVER T_HOST T_PORT T_LOGFILE T_STOREDIR T_BEGIN T_END T_PREFER_IP T_PREFER_IP_MASK
%token <dval> T_FLOAT
%token <sval> T_STR

%%


spec		:	/* empty */
		|	spec specitem
		;

specitem	:	T_PREFER_IP T_STR { parser_cs->setPreferredIP($2); }
		|	T_PREFER_IP_MASK T_STR { parser_cs->setPreferredIPMask($2); }
		|	T_NSERVERS T_INT { parser_cs->Nservers = $2; }
		|	T_STRIPE_METHOD T_INT { parser_cs->setStripeMethod($2); }
		|	T_STRIPE_PARM T_INT { parser_cs->setStripeParm($2); }
		|	T_SERVER T_INT T_HOST T_STR T_PORT T_INT { parser_cs->addServer($2,$4,$6, parser_cs->PreferredIP, parser_cs->PreferredIPMask); }
		|	T_SERVER T_INT T_HOST T_STR { parser_cs->addServer($2,$4,0, parser_cs->PreferredIP, parser_cs->PreferredIPMask); }
		|	host { parser_cs->addHost(currhost); }
		
		;

host		:	T_HOST T_STR { currhost = new HostConfig();
				       currhost->hostname=$2;
				       currhost->port=0; }
			T_BEGIN hostbody T_END
		|	T_HOST T_STR T_PORT T_INT { currhost = new HostConfig();
						    currhost->hostname=$2;
						    currhost->port=$4; }
			T_BEGIN hostbody T_END
		;

hostbody	:	/* empty */
		|	hostbody hitem
		;

hitem		:	T_LOGFILE T_STR { currhost->logfile = $2; }
		|	T_STOREDIR T_STR { currhost->storedir = $2; }
		;

%%

int yyerror(char const *msg){
   fprintf(stderr, "line %d: %s\n", yylineno, msg);
   return 1;
}

