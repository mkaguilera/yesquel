%option yylineno

%{
#include <string.h>
#include <math.h>
#include "config.tab.h"
%}

DIGIT	[0-9]
ID	[a-z_][a-z0-9_]*

%%

"{"      {  return T_BEGIN; }
"}"      {  return T_END; }

{DIGIT}+ {  yylval.ival = atoi(yytext); return T_INT; }

{DIGIT}+"."{DIGIT}* {  yylval.dval = atof(yytext); return T_FLOAT; }

nservers		{ return T_NSERVERS; }
stripe_method		{ return T_STRIPE_METHOD; }
stripe_parm		{ return T_STRIPE_PARM; }
server			{ return T_SERVER; }
host			{ return T_HOST; }
port			{ return T_PORT; }
logfile			{ return T_LOGFILE; }
storedir		{ return T_STOREDIR; }
prefer_ip               { return T_PREFER_IP; }
prefer_ip_mask          { return T_PREFER_IP_MASK; }

{ID}        { fprintf(stderr, "line %d: unrecognized token %s\n", yylineno, yytext); }


\"[^"]*\"      { int mylen = strlen(yytext);
                 yylval.sval = new char[mylen-1];
                 strncpy(yylval.sval, yytext+1, mylen-2); // skip ""'s
                 yylval.sval[mylen-2] = 0;
                 return T_STR; }

"#"[^\n]*    /* eat up comments */
[ \t\n]+       /* eat up whitespace */

.           printf( "Unrecognized character: %s\n", yytext );

%%


int yywrap(void){ return 1; }
