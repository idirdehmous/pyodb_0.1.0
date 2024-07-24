%{ 

/* Important:
   If compiling with flex, please *DO COMPILE* with lex-compatibility option
   "-l" in order to get effect of %array instead of %pointer i.e. 
   extern char yytext[] instead of extern char *yytext !! ;-(
 */

#include "defs.h" 

#if defined(YYLMAX)
#undef YYLMAX
#define YYLMAX 65536
#endif

extern FILE *fpsrc;
extern int optlevel;
extern Boolean verbose;
extern int include_nesting;
extern FILE *yyin;
extern char *odb_source;
extern int ODB_lineno;

int LEX_convflag = 0;
double LEX_unconv_value = 0;

int LEX_in_where = 0;
int LEX_auto_usd_count = 0;

int LEX_create_index = 0;
Boolean LEX_binary_index = false;

PRIVATE Boolean in_comment = 0;

#define ZEROSTR  "\0"
#define DBLSLASH "// "

#define SRC(s) { if (fpsrc) { fprintf(fpsrc,"%s%s ",s,yytext); } }
#define ZSRC   SRC(ZEROSTR)
#define NEWLINE  { if (fpsrc) { fprintf(fpsrc,"\n%s",in_comment ? DBLSLASH : ZEROSTR); } }

PUBLIC  int ODB_lineno = 1;

PRIVATE int first_time = 1;

#define NSTATE_STACK_DEPTH 32
PRIVATE int state[NSTATE_STACK_DEPTH] = { 0 };
PRIVATE int *p_state = state;

#ifdef FLEX_SCANNER
/* Specific to flex scanner */
#define MAX_INCLUDE_DEPTH NSTATE_STACK_DEPTH
PRIVATE YY_BUFFER_STATE include_stack[MAX_INCLUDE_DEPTH];
PRIVATE int include_stack_ptr = 0;
#endif

/* Protos only here */

PRIVATE void LEX_next_state(int next);
PUBLIC void LEX_prev_state();
PRIVATE void LEX_all_states(FILE *fp);
PUBLIC void LEX_print_state(FILE *fp);
PUBLIC FILE *LEX_open_include(const char *filename);
PUBLIC void LEX_close_include();
PUBLIC double Atof(char *s);

%}

%p 30000
%a 15000
%n 5000
%e 7000
%o 8000
%k 2500

%s LEX_NORMAL
%s LEX_INCLUDE
%s LEX_SET
%s LEX_TYPE
%s LEX_TABLE
%s LEX_VIEW
%s LEX_FROM
%s LEX_ORDERBY
%s LEX_EXCLUDED_BY_IFDEF
%s LEX_ALIGN
%s LEX_USING

Number	((([0-9]+)|([0-9]*\.([0-9]+)?))([eEdD][-+]?[0-9]+)?)
Name	[_@A-Za-z]([@\._A-Za-z0-9]*)([#][_]?[0-9]+)?
USDName	[$](([_A-Za-z0-9]+\.[_A-Za-z0-9]+[#])|([_A-Za-z0-9]*([#])?))
HASHName [#][_A-Za-z0-9]+
BSnum   [\\][0-9]+
String	((\"[^"\n]*\")|(\'[^'\n]*\')|(\{[^\}\n]*\}))
OptLevel [ \t]*[/][/][ \t]*[\@]OPT[ \t]*=[ \t]*[0-9]*[ \t]*[\n]
Comment  (([/][/].*)|([-][-][ \t]).*)[\n]
Define  [#][ \t]*define[ \t]+[_A-Za-z0-9]*[ \t]*[0-9]*[ \t]*[\n]
Undef   [#][ \t]*undef[ \t]+[_A-Za-z0-9]*[ \t]*[0-9]*[ \t]*[\n]
Ifdef   [#][ \t]*ifdef[ \t]+[_A-Za-z0-9]*[ \t]*[\n]
Ifndef  [#][ \t]*ifndef[ \t]+[_A-Za-z0-9]*[ \t]*[\n]
Endif   [#][ \t]*endif[ \t]*.*[\n]
Include ([#][ \t]*include)|(INCLUDE)|(include)

%%

%{
	if (first_time) {
   	  /* LEX_all_states(stderr); */
	  LEX_next_state(LEX_NORMAL);
	  first_time = 0;
	}

%}

\n		{ ODB_lineno++; NEWLINE; }

^[ \t]*\n	{ ODB_lineno++; }

[ \t]+	;

^{Ifdef}	{
			double value = ODB_get_define(yytext);
			ODB_lineno++; 
			if (value == 0) {
				LEX_next_state(LEX_EXCLUDED_BY_IFDEF);
				/* if (verbose) fprintf(stderr,"==> Entering %s",yytext); */
			}
			/* if (fpsrc) fprintf(fpsrc,"// %s",yytext); */
		}

^{Ifndef}	{
			double value = ODB_get_define(yytext);
			ODB_lineno++; 
			if (value != 0) {
				LEX_next_state(LEX_EXCLUDED_BY_IFDEF);
				/* if (verbose) fprintf(stderr,"==> Entering %s",yytext); */
			}
			/* if (fpsrc) fprintf(fpsrc,"// %s",yytext); */
		}

^{Endif}	{
			/* if (fpsrc) fprintf(fpsrc,"// %s",yytext); */
			ODB_lineno++; 
			if (*p_state == LEX_EXCLUDED_BY_IFDEF) {
				/* if (verbose) fprintf(stderr,"==> Leaving %s",yytext); */
				LEX_prev_state();
			}
		}

<LEX_EXCLUDED_BY_IFDEF>.*[\n] {
			/* if (fpsrc) fprintf(fpsrc,"//!!! %s",yytext); */
			ODB_lineno++; 
		}

^{Define}	{ 
			char *p = strstr(yytext, "#define");
			if (p) {
			  p += 7;
			  ODB_put_define(p,1);
			}
			ODB_lineno++; 
		}

^{Undef}	{ 
			char *p = strstr(yytext, "#undef");
			if (p) {
			  p += 6;
			  ODB_put_define(p,0);
			}
			ODB_lineno++; 
		}

";"		{ 
		  Boolean do_newline = 1;
		  if (	 *p_state == LEX_SET
		      || *p_state == LEX_TYPE
		      || *p_state == LEX_TABLE
		      || *p_state == LEX_VIEW
		      || *p_state == LEX_FROM
		      || *p_state == LEX_ORDERBY
		      || *p_state == LEX_ALIGN
		      || *p_state == LEX_USING
		      ) 
		  { 
			if (*p_state == LEX_SET) do_newline = 0;
			LEX_prev_state();
			if (*p_state == LEX_VIEW) LEX_prev_state();
		  } 
		  if (fpsrc) { 
			ZSRC; 
			if (do_newline) NEWLINE;
			in_comment = 0; 
		  }
		  LEX_in_where = 0;
		  return ODB_SEMICOLON;
		}


^{OptLevel}	{ 
			char *p = strchr(yytext, '=');
			if (p) optlevel = atoi(p+1);
			optlevel = MAX(0,optlevel);
			if (verbose) {
			   fprintf(stderr,
				"*** Warning: Optlevel = %d\n",optlevel);
			}
			ODB_lineno++; 
		}

{Comment}	{ 
			if (fpsrc) fprintf(fpsrc,"%s%s",yytext,
					   in_comment ? DBLSLASH : ZEROSTR);
			ODB_lineno++; 
		}

<LEX_NORMAL>^{Include} {
	LEX_next_state(LEX_INCLUDE);
	/* LEX_print_state(stderr); */
	SRC(DBLSLASH);
	in_comment = 1;
	return ODB_INCLUDE; }

<LEX_NORMAL>SET |
<LEX_NORMAL>set {
	LEX_next_state(LEX_SET);
	ZSRC;
	return ODB_SET; }

READONLY	|
readonly	{ ZSRC; return ODB_READONLY; }

READ_ONLY	|
read_only	{ ZSRC; return ODB_READONLY; }

READ[ \t]*ONLY	|
read[ \t]*only	{ ZSRC; return ODB_READONLY; }

UPDATED		|
updated		{ ZSRC; return ODB_UPDATED; }

UPDATABLE	|
updatable	{ ZSRC; return ODB_UPDATED; }

<LEX_NORMAL>USING |
<LEX_NORMAL>using |
<LEX_NORMAL>USE   |
<LEX_NORMAL>use   {
  LEX_next_state(LEX_USING);
  ZSRC; return ODB_USING; }

<LEX_NORMAL>DATABASE |
<LEX_NORMAL>database |
<LEX_USING>DATABASE  |
<LEX_USING>database  { ZSRC; return ODB_DATABASE; }

<LEX_NORMAL>SRCPATH |
<LEX_NORMAL>srcpath |
<LEX_USING>SRCPATH  |
<LEX_USING>srcpath  { ZSRC; return ODB_SRCPATH; }

<LEX_NORMAL>DATAPATH |
<LEX_NORMAL>datapath |
<LEX_USING>DATAPATH  |
<LEX_USING>datapath  { ZSRC; return ODB_DATAPATH; }

<LEX_NORMAL>IDXPATH |
<LEX_NORMAL>idxpath |
<LEX_USING>IDXPATH  |
<LEX_USING>idxpath  { ZSRC; return ODB_IDXPATH; }

<LEX_NORMAL>POOLMASK |
<LEX_NORMAL>poolmask |
<LEX_USING>POOLMASK  |
<LEX_USING>poolmask  { ZSRC; return ODB_POOLMASK; }

<LEX_NORMAL>INDEX |
<LEX_NORMAL>index |
<LEX_USING>INDEX  |
<LEX_USING>index  { ZSRC; LEX_binary_index = false; return ODB_INDEX; }

<LEX_NORMAL>TEXT[ \t]*INDEX |
<LEX_NORMAL>TEXT[ \t]*index |
<LEX_NORMAL>text[ \t]*INDEX |
<LEX_NORMAL>text[ \t]*index |
<LEX_USING>TEXT[ \t]*INDEX  |
<LEX_USING>TEXT[ \t]*index  |
<LEX_USING>text[ \t]*INDEX  |
<LEX_USING>text[ \t]*index  { ZSRC; LEX_binary_index = false; return ODB_INDEX; }

<LEX_NORMAL>BINARY[ \t]*INDEX |
<LEX_NORMAL>BINARY[ \t]*index |
<LEX_NORMAL>binary[ \t]*INDEX |
<LEX_NORMAL>binary[ \t]*index |
<LEX_USING>BINARY[ \t]*INDEX  |
<LEX_USING>BINARY[ \t]*index  |
<LEX_USING>binary[ \t]*INDEX  |
<LEX_USING>binary[ \t]*index  { ZSRC; LEX_binary_index = true; return ODB_INDEX; }

<LEX_NORMAL>NO[ \t]*INDEX |
<LEX_NORMAL>NO[ \t]*index |
<LEX_NORMAL>no[ \t]*index |
<LEX_NORMAL>no[ \t]*INDEX { ZSRC; return ODB_NOINDEX; }

<LEX_NORMAL>REORDER |
<LEX_NORMAL>reorder { ZSRC; return ODB_REORDER; }

<LEX_NORMAL>NO[ \t]*REORDER |
<LEX_NORMAL>NO[ \t]*reorder |
<LEX_NORMAL>no[ \t]*reorder |
<LEX_NORMAL>no[ \t]*REORDER { ZSRC; return ODB_NOREORDER; }

<LEX_NORMAL>INSERT |
<LEX_NORMAL>insert { ZSRC; return ODB_INSERT; }

<LEX_NORMAL>NO[ \t]*INSERT |
<LEX_NORMAL>NO[ \t]*insert |
<LEX_NORMAL>no[ \t]*insert |
<LEX_NORMAL>no[ \t]*INSERT { ZSRC; return ODB_NOINSERT; }

<LEX_NORMAL>SAFE[ \t]*GUARD |
<LEX_NORMAL>SAFE[ \t]*guard |
<LEX_NORMAL>safe[ \t]*GUARD |
<LEX_NORMAL>safe[ \t]*guard { ZSRC; return ODB_SAFEGUARD; }

<LEX_NORMAL>NO[ \t]*SAFE[ \t]*GUARD |
<LEX_NORMAL>NO[ \t]*SAFE[ \t]*guard |
<LEX_NORMAL>NO[ \t]*safe[ \t]*GUARD |
<LEX_NORMAL>NO[ \t]*safe[ \t]*guard { ZSRC; return ODB_NOSAFEGUARD; }

<LEX_NORMAL>no[ \t]*SAFE[ \t]*GUARD |
<LEX_NORMAL>no[ \t]*SAFE[ \t]*guard |
<LEX_NORMAL>no[ \t]*safe[ \t]*GUARD |
<LEX_NORMAL>no[ \t]*safe[ \t]*guard { ZSRC; return ODB_NOSAFEGUARD; }

<LEX_NORMAL>RESET	|
<LEX_NORMAL>reset	{
  LEX_next_state(LEX_ALIGN);
  ZSRC; return ODB_RESET; }

<LEX_NORMAL>ALIGN	|
<LEX_NORMAL>align	{ 
  LEX_next_state(LEX_ALIGN);
  ZSRC; return ODB_ALIGN; }

<LEX_ALIGN>ALIGN	|
<LEX_ALIGN>align	{ 
  ZSRC; return ODB_ALIGN; }

<LEX_NORMAL>ONELOOPER	|
<LEX_NORMAL>onelooper	{ 
  LEX_next_state(LEX_ALIGN);
  ZSRC; return ODB_ONELOOPER; }

<LEX_ALIGN>ONELOOPER	|
<LEX_ALIGN>onelooper	{ 
  ZSRC; return ODB_ONELOOPER; }

<LEX_NORMAL>SHAREDLINK	|
<LEX_NORMAL>sharedlink	{ 
  LEX_next_state(LEX_ALIGN);
  ZSRC; return ODB_SHAREDLINK; }

<LEX_ALIGN>SHAREDLINK	|
<LEX_ALIGN>sharedlink	{ 
  ZSRC; return ODB_SHAREDLINK; }

<LEX_NORMAL>CREATE[ \t]*TYPE |
<LEX_NORMAL>CREATE[ \t]*type |
<LEX_NORMAL>create[ \t]*TYPE |
<LEX_NORMAL>create[ \t]*type |
<LEX_NORMAL>typedef	       |
<LEX_NORMAL>TYPEDEF	       {
	LEX_next_state(LEX_TYPE);
	ZSRC;
	return ODB_TYPE; }

<LEX_NORMAL>CREATE[ \t]*TABLE |
<LEX_NORMAL>CREATE[ \t]*table |
<LEX_NORMAL>create[ \t]*TABLE |
<LEX_NORMAL>create[ \t]*table {
	LEX_next_state(LEX_TABLE);
	ZSRC;
	return ODB_TABLE; }

<LEX_TABLE>TYPE[ \t]*OF |
<LEX_TABLE>TYPE[ \t]*of |
<LEX_TABLE>type[ \t]*of |
<LEX_TABLE>type[ \t]*OF { 
		 ZSRC;
		 return ODB_TYPEOF;  }

<LEX_NORMAL>CREATE[ \t]*VIEW |
<LEX_NORMAL>CREATE[ \t]*view |
<LEX_NORMAL>create[ \t]*VIEW |
<LEX_NORMAL>create[ \t]*view {
	LEX_next_state(LEX_VIEW);
	SRC(DBLSLASH);
	in_comment = 1;
	return ODB_VIEW; }

<LEX_NORMAL>DROP[ \t]*INDEX |
<LEX_NORMAL>DROP[ \t]*index |
<LEX_NORMAL>drop[ \t]*INDEX |
<LEX_NORMAL>drop[ \t]*index {
	LEX_next_state(LEX_VIEW);
	SRC(DBLSLASH);
	in_comment = 1;
	LEX_create_index = -1; /* DROP INDEX [a quick hack] */
	return ODB_DROPINDEX; }

<LEX_NORMAL>CREATE[ \t]*INDEX |
<LEX_NORMAL>CREATE[ \t]*index |
<LEX_NORMAL>create[ \t]*INDEX |
<LEX_NORMAL>create[ \t]*index {
	LEX_next_state(LEX_VIEW);
	SRC(DBLSLASH);
	in_comment = 1;
	LEX_create_index = 1; /* 0=Normal SELECT stmt, 1=CREATE UNIQUE INDEX, 2=CREATE BITMAP INDEX */
	return ODB_CREATEINDEX; }

<LEX_NORMAL>CREATE[ \t]*UNIQUE[ \t]*INDEX |
<LEX_NORMAL>CREATE[ \t]*UNIQUE[ \t]*index |
<LEX_NORMAL>create[ \t]*UNIQUE[ \t]*INDEX |
<LEX_NORMAL>create[ \t]*UNIQUE[ \t]*index |
<LEX_NORMAL>CREATE[ \t]*unique[ \t]*INDEX |
<LEX_NORMAL>CREATE[ \t]*unique[ \t]*index |
<LEX_NORMAL>create[ \t]*unique[ \t]*INDEX |
<LEX_NORMAL>create[ \t]*unique[ \t]*index {
	LEX_next_state(LEX_VIEW);
	SRC(DBLSLASH);
	in_comment = 1;
	LEX_create_index = 1; /* 0=Normal SELECT stmt, 1=CREATE UNIQUE INDEX, 2=CREATE BITMAP INDEX */
	return ODB_CREATEINDEX; }

<LEX_NORMAL>CREATE[ \t]*DISTINCT[ \t]*INDEX |
<LEX_NORMAL>CREATE[ \t]*DISTINCT[ \t]*index |
<LEX_NORMAL>create[ \t]*DISTINCT[ \t]*INDEX |
<LEX_NORMAL>create[ \t]*DISTINCT[ \t]*index |
<LEX_NORMAL>CREATE[ \t]*distinct[ \t]*INDEX |
<LEX_NORMAL>CREATE[ \t]*distinct[ \t]*index |
<LEX_NORMAL>create[ \t]*distinct[ \t]*INDEX |
<LEX_NORMAL>create[ \t]*distinct[ \t]*index {
	LEX_next_state(LEX_VIEW);
	SRC(DBLSLASH);
	in_comment = 1;
	LEX_create_index = 1; /* 0=Normal SELECT stmt, 1=CREATE UNIQUE INDEX, 2=CREATE BITMAP INDEX */
	return ODB_CREATEINDEX; }

<LEX_NORMAL>CREATE[ \t]*BITMAP[ \t]*INDEX |
<LEX_NORMAL>CREATE[ \t]*BITMAP[ \t]*index |
<LEX_NORMAL>create[ \t]*BITMAP[ \t]*INDEX |
<LEX_NORMAL>create[ \t]*BITMAP[ \t]*index |
<LEX_NORMAL>CREATE[ \t]*bitmap[ \t]*INDEX |
<LEX_NORMAL>CREATE[ \t]*bitmap[ \t]*index |
<LEX_NORMAL>create[ \t]*bitmap[ \t]*INDEX |
<LEX_NORMAL>create[ \t]*bitmap[ \t]*index {
	LEX_next_state(LEX_VIEW);
	SRC(DBLSLASH);
	in_comment = 1;
	LEX_create_index = 2; /* 0=Normal SELECT stmt, 1=CREATE UNIQUE INDEX, 2=CREATE BITMAP INDEX */
	return ODB_CREATEINDEX; }

<LEX_VIEW>"("[ \t]*SELECT |
<LEX_VIEW>"("[ \t]*select { ZSRC; return ODB_SELECT; /* sub-query */ }

<LEX_VIEW>QUERY[ \t]*"(" |
<LEX_VIEW>query[ \t]*"(" { ZSRC; return ODB_QUERY; /* sub-query */ }

<LEX_VIEW>MATCH[ \t]*"(" |
<LEX_VIEW>match[ \t]*"(" { ZSRC; return ODB_MATCH; /* match-keyword */ }

<LEX_NORMAL>RETURN |
<LEX_NORMAL>return |
<LEX_NORMAL>QUIT |
<LEX_NORMAL>quit |
<LEX_NORMAL>EXIT |
<LEX_NORMAL>exit { SRC(DBLSLASH); return ODB_EXIT; }

<LEX_NORMAL>SELECT[ \t]*UNIQUE    |
<LEX_NORMAL>SELECT[ \t]*unique    |
<LEX_NORMAL>select[ \t]*unique    |
<LEX_NORMAL>select[ \t]*UNIQUE    |
<LEX_NORMAL>SELECT[ \t]*DISTINCT  |
<LEX_NORMAL>SELECT[ \t]*distinct  |
<LEX_NORMAL>select[ \t]*distinct  |
<LEX_NORMAL>select[ \t]*DISTINCT  { 
	LEX_next_state(LEX_VIEW);			
	ZSRC; 
	return ODB_SELECT_DISTINCT; }

<LEX_VIEW>SELECT[ \t]*UNIQUE	  |
<LEX_VIEW>SELECT[ \t]*unique	  |
<LEX_VIEW>select[ \t]*unique	  |
<LEX_VIEW>select[ \t]*UNIQUE	  |
<LEX_VIEW>SELECT[ \t]*DISTINCT  |
<LEX_VIEW>SELECT[ \t]*distinct  |
<LEX_VIEW>select[ \t]*distinct  |
<LEX_VIEW>select[ \t]*DISTINCT  { 
	ZSRC; 
	return ODB_SELECT_DISTINCT; }

<LEX_NORMAL>SELECT[ \t]*ALL	|
<LEX_NORMAL>select[ \t]*all	|
<LEX_NORMAL>select[ \t]*ALL	|
<LEX_NORMAL>SELECT[ \t]*all	|
<LEX_NORMAL>SELECT		|
<LEX_NORMAL>select		{
	LEX_next_state(LEX_VIEW);			
	ZSRC; 
	return ODB_SELECT_ALL; }

<LEX_VIEW>SELECT[ \t]*ALL	|
<LEX_VIEW>select[ \t]*all	|
<LEX_VIEW>select[ \t]*ALL	|
<LEX_VIEW>SELECT[ \t]*all	|
<LEX_VIEW>SELECT		|
<LEX_VIEW>select		{
	ZSRC; 
	return ODB_SELECT_ALL; }

<LEX_VIEW>FROM			|
<LEX_VIEW>from			{ 
	LEX_next_state(LEX_FROM);			
	ZSRC;
	return ODB_FROM; }

<LEX_VIEW>ON			|
<LEX_VIEW>on			{ 
  /* Later: Build safeguards here against using 'ON table' elsewhere than with CREATE [UNIQUE|BITMAP] INDEX */
	LEX_next_state(LEX_FROM);			
	ZSRC;
	return ODB_ON; }

<LEX_FROM>WHERE			|
<LEX_FROM>where			{ 
	LEX_prev_state(); /* back to LEX_VIEW */
	LEX_in_where = 1;
	LEX_auto_usd_count = 0;
        ZSRC; 
	return ODB_WHERE; }

<LEX_VIEW>WHERE			|
<LEX_VIEW>where			{ 
	LEX_in_where = 1;
	LEX_auto_usd_count = 0;
        ZSRC; 
	return ODB_WHERE; }

<LEX_VIEW>UNIQUE[ \t]*BY	|
<LEX_VIEW>UNIQUE[ \t]*by	|
<LEX_VIEW>unique[ \t]*by	|
<LEX_VIEW>unique[ \t]*BY	{ ZSRC; return ODB_UNIQUEBY; }

<LEX_VIEW>DISTINCT[ \t]*BY	|
<LEX_VIEW>DISTINCT[ \t]*by	|
<LEX_VIEW>distinct[ \t]*by	|
<LEX_VIEW>distinct[ \t]*BY	{ ZSRC; return ODB_UNIQUEBY; }

<LEX_VIEW>DISTINCT              |
<LEX_VIEW>distinct              |
<LEX_VIEW>UNIQUE                |
<LEX_VIEW>unique                {
	ZSRC; 
	return ODB_DISTINCT;    }

<LEX_VIEW>ORDER[ \t]*BY	|
<LEX_VIEW>ORDER[ \t]*by	|
<LEX_VIEW>order[ \t]*by	|
<LEX_VIEW>order[ \t]*BY	|
<LEX_VIEW>SORT[ \t]*BY	|
<LEX_VIEW>SORT[ \t]*by	|
<LEX_VIEW>sort[ \t]*by  	|
<LEX_VIEW>sort[ \t]*BY  	{ 
	LEX_next_state(LEX_ORDERBY);			
        ZSRC;
	return ODB_ORDERBY; }

<LEX_FROM>ORDER[ \t]*BY	|
<LEX_FROM>ORDER[ \t]*by	|
<LEX_FROM>order[ \t]*by	|
<LEX_FROM>order[ \t]*BY	|
<LEX_FROM>SORT[ \t]*BY	|
<LEX_FROM>SORT[ \t]*by	|
<LEX_FROM>sort[ \t]*by  	|
<LEX_FROM>sort[ \t]*BY  	{ 
	LEX_prev_state(); /* back to LEX_VIEW */
	LEX_next_state(LEX_ORDERBY);			
        ZSRC;
	return ODB_ORDERBY; }

<LEX_VIEW>AND			| 
<LEX_VIEW>and			| 
<LEX_VIEW>"&&"			{ ZSRC; return ODB_AND; }

<LEX_VIEW>"|"			{ ZSRC; return ODB_NORM; }

<LEX_VIEW>OR			| 
<LEX_VIEW>or			| 
<LEX_VIEW>"||"			{ ZSRC; return ODB_OR; }

<LEX_VIEW>BETWEEN		|
<LEX_VIEW>between		{ ZSRC; return ODB_BETWEEN; }

<LEX_VIEW>"!~"		|
<LEX_VIEW>NOT[ \t]*LIKE	|
<LEX_VIEW>NOT[ \t]*like	|
<LEX_VIEW>not[ \t]*LIKE	|
<LEX_VIEW>not[ \t]*like	{ ZSRC; return ODB_NOTLIKE; }

<LEX_VIEW>"=~"			|
<LEX_VIEW>LIKE			|
<LEX_VIEW>like			{ ZSRC; return ODB_LIKE; }

<LEX_VIEW>NOT[ \t]*RLIKE	|
<LEX_VIEW>NOT[ \t]*rlike	|
<LEX_VIEW>not[ \t]*RLIKE	|
<LEX_VIEW>not[ \t]*rlike	{ ZSRC; return ODB_NOTLIKE; }

<LEX_VIEW>RLIKE			|
<LEX_VIEW>rlike			{ ZSRC; return ODB_LIKE; }

<LEX_VIEW>NEAR[ \t]*"("		|
<LEX_VIEW>near[ \t]*"("		{ ZSRC; return ODB_NEAR; }

<LEX_VIEW>INSIDE[ \t]*"("	|
<LEX_VIEW>inside[ \t]*"("	{ ZSRC; return ODB_INSIDE; }

<LEX_VIEW>IN[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>in[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>IN[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>in[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>INSIDE[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>inside[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>INSIDE[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>inside[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>POLYGON[ \t]*"("		|
<LEX_VIEW>polygon[ \t]*"("		{ ZSRC; return ODB_INSIDE_POLYGON; }

<LEX_VIEW>NOT[ \t]*INSIDE[ \t]*"(" |
<LEX_VIEW>not[ \t]*inside[ \t]*"(" |
<LEX_VIEW>NOT[ \t]*inside[ \t]*"(" |
<LEX_VIEW>not[ \t]*INSIDE[ \t]*"(" |
<LEX_VIEW>"!"[ \t]*INSIDE[ \t]*"(" |
<LEX_VIEW>"!"[ \t]*inside[ \t]*"(" |
<LEX_VIEW>OUTSIDE[ \t]*"("	   |
<LEX_VIEW>outside[ \t]*"("	   { ZSRC; return ODB_OUTSIDE; }

<LEX_VIEW>NOT[ \t]*IN[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>NOT[ \t]*in[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>NOT[ \t]*IN[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>NOT[ \t]*in[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>not[ \t]*IN[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>not[ \t]*in[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>not[ \t]*IN[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>not[ \t]*in[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>NOT[ \t]*INSIDE[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>NOT[ \t]*inside[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>NOT[ \t]*INSIDE[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>NOT[ \t]*inside[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>not[ \t]*INSIDE[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>not[ \t]*inside[ \t]*POLYGON[ \t]*"("	|
<LEX_VIEW>not[ \t]*INSIDE[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>not[ \t]*inside[ \t]*polygon[ \t]*"("	|
<LEX_VIEW>NOT[ \t]*POLYGON[ \t]*"(" |
<LEX_VIEW>not[ \t]*polygon[ \t]*"(" |
<LEX_VIEW>NOT[ \t]*polygon[ \t]*"(" |
<LEX_VIEW>not[ \t]*POLYGON[ \t]*"(" |
<LEX_VIEW>OUTSIDE[ \t]*POLYGON[ \t]*"(" |
<LEX_VIEW>outside[ \t]*polygon[ \t]*"(" |
<LEX_VIEW>OUTSIDE[ \t]*polygon[ \t]*"(" |
<LEX_VIEW>outside[ \t]*POLYGON[ \t]*"(" |
<LEX_VIEW>"!"[ \t]*POLYGON[ \t]*"(" |
<LEX_VIEW>"!"[ \t]*polygon[ \t]*"(" { ZSRC; return ODB_OUTSIDE_POLYGON; }

<LEX_VIEW>"<=>"			{ ZSRC; return ODB_CMP; }

<LEX_VIEW>"="[ \t]*{String}  |
<LEX_VIEW>"=="[ \t]*{String} |
<LEX_VIEW>"<>"[ \t]*{String} |
<LEX_VIEW>"!="[ \t]*{String} |
<LEX_VIEW>"/="[ \t]*{String} {
  /* Either "string" or 'string' or {string}' */
  char quote = 0;
  char *p = yytext;
  char *oper = NULL;
  ZSRC; 
  while (*p) {
    if (*p == '"' || *p == '\'' || *p == '{') {
      quote = *p;
      if (quote == '{') quote = '}';
      break;
    }
    ++p;
  }
  yylval.str = STRDUP(p); /* Open quote included; will be replaced with char '1' or '0' */
  {
    char *close_quote = strchr(yylval.str+1,quote);    
    if (!close_quote) {
      YYerror("Unterminated string");
    } else {
      *close_quote = '\0'; /* remove close quote */
      *p = '\0';
      oper = STRDUP(yytext);
      p = strchr(oper,' ');
      if (!p) p = strchr(oper,'\t');
      if (p) *p = '\0';
      if (strequ(oper,"==") || strequ(oper,"=")) *yylval.str = '1';
      else *yylval.str = '0';
    }
  }
  return ODB_EQNE_STRING;
}

<LEX_VIEW>"<>"		| 
<LEX_VIEW>"!="		|
<LEX_VIEW>"/="		{ ZSRC; return ODB_NE; }

<LEX_VIEW>"="		|
<LEX_VIEW>"=="		{ ZSRC; return ODB_EQ; }

<LEX_VIEW>">"		{ ZSRC; return ODB_GT; }
<LEX_VIEW>"<"		{ ZSRC; return ODB_LT; }
<LEX_VIEW>">="		{ ZSRC; return ODB_GE; }
<LEX_VIEW>"<="		{ ZSRC; return ODB_LE; }

<LEX_VIEW>NOT[ \t]*IN[ \t]*FILE	|
<LEX_VIEW>NOT[ \t]*in[ \t]*FILE	|
<LEX_VIEW>NOT[ \t]*IN[ \t]*file	|
<LEX_VIEW>NOT[ \t]*in[ \t]*file	|
<LEX_VIEW>not[ \t]*IN[ \t]*FILE	|
<LEX_VIEW>not[ \t]*in[ \t]*FILE	|
<LEX_VIEW>not[ \t]*IN[ \t]*file	|
<LEX_VIEW>not[ \t]*in[ \t]*file	|
<LEX_VIEW>"!"[ \t]*IN[ \t]*FILE	|
<LEX_VIEW>"!"[ \t]*IN[ \t]*file	|
<LEX_VIEW>"!"[ \t]*in[ \t]*FILE	|
<LEX_VIEW>"!"[ \t]*in[ \t]*file	{ ZSRC; return ODB_NOTINFILE; }

<LEX_VIEW>NOT[ \t]*IN |
<LEX_VIEW>NOT[ \t]*in |
<LEX_VIEW>not[ \t]*in |
<LEX_VIEW>not[ \t]*IN |
<LEX_VIEW>"!"[ \t]*IN	|
<LEX_VIEW>"!"[ \t]*in	{ ZSRC; return ODB_NOTIN; }

<LEX_VIEW>IN[ \t]*FILE	|
<LEX_VIEW>IN[ \t]*file	|
<LEX_VIEW>in[ \t]*FILE	|
<LEX_VIEW>in[ \t]*file	{ ZSRC; return ODB_INFILE; }

<LEX_VIEW>IN		|
<LEX_VIEW>in		{ ZSRC; return ODB_IN; }

<LEX_VIEW>"!" |
<LEX_VIEW>NOT |
<LEX_VIEW>not	{ ZSRC; return ODB_NOT; }

<LEX_ORDERBY>ASC	|
<LEX_ORDERBY>asc	{ ZSRC; return ODB_ASC; }

<LEX_ORDERBY>DESC	|
<LEX_ORDERBY>desc	{ ZSRC; return ODB_DESC; }

<LEX_ORDERBY>ABS	|
<LEX_ORDERBY>abs	{ ZSRC; return ODB_ABS; }

<LEX_VIEW>IS		|
<LEX_VIEW>is		{ ZSRC; return ODB_IS; }

<LEX_VIEW>NULL		|
<LEX_VIEW>null		{ ZSRC; return ODB_NULL; }

<LEX_VIEW>[?]		{ 
  int rc = LEX_in_where ? ODB_USDNAME : ODB_QMARK;
  if (LEX_in_where) {
    ODB_Symbol *psym = NULL;
    char *auto_usd = NULL;
    ALLOC(auto_usd,20);
    sprintf(auto_usd,"$%d",++LEX_auto_usd_count);
    psym = ODB_lookup(ODB_USDNAME,auto_usd,NULL);
    if (!psym) {
      const double auto_value = 0;
      psym = ODB_new_symbol(ODB_USDNAME,auto_usd);
      psym->dval = auto_value;
    }
    yylval.str = STRDUP(auto_usd);
    FREE(auto_usd);
  }
  ZSRC;
  return rc;
}

AS  |
as  { ZSRC; return ODB_AS; }

":" { ZSRC; return ODB_COLON; }

"[" { ZSRC; return ODB_LB; }
"]" { ZSRC; return ODB_RB; }

"(" { ZSRC; return ODB_LP; }
")" { ZSRC; return ODB_RP; }

"," { ZSRC; return ODB_COMMA; }

"**" | 
"^" { ZSRC; return ODB_POWER; }

"=" { ZSRC; return ODB_EQ; }
"+" { ZSRC; return ODB_ADD; }
"-" { ZSRC; return ODB_SUB; }

<LEX_VIEW>".*" { ZSRC; return ODB_DOTP; }

"*" { ZSRC; return ODB_STAR; }
"/" { ZSRC; return ODB_DIV; }
"%" { ZSRC; return ODB_MODULO; }

{String}	{ /* Either "string" or 'string' or {string} */
		  char quote = yytext[0];
		  if (quote == '{') quote = '}';
		  ZSRC; 
		  yylval.str = STRDUP(yytext+1); /* skip open quote */
		  if (yytext[yyleng-1] != quote) {
			YYerror("Unterminated string");
		  } else  
			yylval.str[yyleng-2] = '\0';  /* remove close quote */
		  /*
		  if (strlen(yylval.str) != 8) {
			YYerror("String length not equal to 8");
		  }
		  */
		  return ODB_STRING;
		}

{Name}		{ /* A pure symbol (can be a case-sensitive define);
		     ODB_lowercase will do the STRDUP() as well */
		  ZSRC; 
		  yylval.str = ODB_has_define(yytext) ? STRDUP(yytext) : ODB_lowercase(yytext);
		  return ODB_NAME;
		}

{USDName}	{ /* A symbol starting with a dollar;  
		     ODB_lowercase will do the STRDUP() as well */
		  ZSRC; 
		  yylval.str = ODB_lowercase(yytext);
		  return ODB_USDNAME;
		}

{HASHName}	{ /* A symbol starting with a hash (#);  
		     ODB_lowercase will do the STRDUP() as well */
		  ZSRC; 
		  yylval.str = ODB_lowercase(yytext);
		  return ODB_HASHNAME;
		}

<LEX_VIEW>{BSnum} { /* A symbol starting with backslash (\) and followed by an integer number.
		       Only valid in WHERE-statements (and denotes column# from sub-query outputs) */
		    ZSRC;
		    if (LEX_in_where) {
		      yylval.str = STRDUP(yytext);
		    }
		    else {
		      SETMSG1("Variable '%s' can be used only in WHERE-stmt",yytext);
		      YYerror(msg);
		    }
		    return ODB_BSNUM;
		  }

{Number}([hH][pP][aA]|[ \t]+[hH][pP][aA][ \t\n]+) { /* Hecto-Pascals to Pascals (hPa) (case doesn't matter) */
		  ZSRC; 
		  yytext[strlen(yytext)-1] = '\0';
		  yylval.dval = 100*Atof(yytext);
		  /* Reset the latest conversion (in case of unary-minus) */
		  LEX_convflag = 0;
		  LEX_unconv_value = 0;
		  return ODB_NUMBER; 
		}

{Number}([mM][bB][aA][rR]|[ \t]+[mM][bB][aA][rR][ \t\n]+) { /* mbar's to Pascals (case doesn't matter) */
		  ZSRC; 
		  yytext[strlen(yytext)-1] = '\0';
		  yylval.dval = 100*Atof(yytext);
		  /* Reset the latest conversion (in case of unary-minus) */
		  LEX_convflag = 0;
		  LEX_unconv_value = 0;
		  return ODB_NUMBER; 
		}

{Number}([wWsS]|[ \t]+[wWsS][ \t\n]+) { /* longitude west or latitude south (case doesn't matter) */
		  ZSRC; 
		  yytext[strlen(yytext)-1] = '\0';
		  yylval.dval = -Atof(yytext);
		  /* Reset the latest conversion (in case of unary-minus) */
		  LEX_convflag = 0;
		  LEX_unconv_value = 0;
		  return ODB_NUMBER; 
		}

{Number}([eEnN]|[ \t]+[eEnN][ \t\n]+) { /* longitude east or latitude north (case doesn't matter) */
		  ZSRC; 
		  yytext[strlen(yytext)-1] = '\0';
		  yylval.dval = +Atof(yytext);
		  /* Reset the latest conversion (in case of unary-minus) */
		  LEX_convflag = 0;
		  LEX_unconv_value = 0;
		  return ODB_NUMBER; 
		}

{Number}(C|[ \t]+C[ \t\n]+)	{ /* Celsius (Capital "C" at the end) */
		  double celsius;
		  ZSRC; 
		  yytext[strlen(yytext)-1] = '\0';
		  celsius = Atof(yytext);
		  /* Remember the latest conversion (in case of unary-minus) */
		  LEX_convflag = 1;
		  LEX_unconv_value = celsius;
		  yylval.dval = celsius + ZERO_POINT; /* Value is now in Kelvin */
		  return ODB_NUMBER; 
		}

{Number}(F|[ \t]+F[ \t\n]+) { /* Fahrenheit (Capital "F" at the end) */
		  double fahrenheit, celsius;
		  ZSRC; 
		  yytext[strlen(yytext)-1] = '\0';
		  fahrenheit = Atof(yytext);
		  /* Remember the latest conversion (in case of unary-minus) */
		  LEX_convflag = 2;
		  LEX_unconv_value = fahrenheit;
		  celsius = ((fahrenheit - 32)*5)/9;
		  yylval.dval = celsius + ZERO_POINT; /* Value is now in Kelvin */
		  return ODB_NUMBER; 
		}

{Number}(K|[ \t]+K[ \t\n]+) { /* Kelvin (Capital "K" at the end) ; 
		     just for convenience; no conversions performed */
		  ZSRC; 
		  yytext[strlen(yytext)-1] = '\0';
		  yylval.dval = Atof(yytext);
		  /* Reset the latest conversion (in case of unary-minus) */
		  LEX_convflag = 0;
		  LEX_unconv_value = 0;
		  return ODB_NUMBER;  /* Value is now in Kelvin */
		}

{Number}(km|[ \t]+km[ \t\n]+) { /* Kilometres (lowercase "km") */
		     ZSRC; 
		     yytext[strlen(yytext)-1] = '\0';
		     yylval.dval = 1000 * Atof(yytext);
		     /* Reset the latest conversion (in case of unary-minus) */
		     LEX_convflag = 0;
		     LEX_unconv_value = 0;
		     return ODB_NUMBER;  /* Value is now in metres */
		   }

{Number}(miles|[ \t]+miles[ \t\n]+) { /* Kilometres (lowercase word "miles") */
		     ZSRC; 
		     yytext[strlen(yytext)-1] = '\0';
		     yylval.dval = ((double)1609.34e0) * Atof(yytext);
		     /* Reset the latest conversion (in case of unary-minus) */
		     LEX_convflag = 0;
		     LEX_unconv_value = 0;
		     return ODB_NUMBER;  /* Value is now in metres */
		   }

{Number}	{
		  ZSRC; 
		  yylval.dval = Atof(yytext);
		  /* Reset the latest conversion (in case of unary-minus) */
		  LEX_convflag = 0;
		  LEX_unconv_value = 0;
		  return ODB_NUMBER; 
		}

.		{ ZSRC; return yytext[0]; }

%%

PRIVATE void
LEX_next_state(int next) {
	BEGIN(next); 
	/* fprintf(stderr,"==> Entering LEX_next_state(%d)\n",next); */
	*++p_state = next; 
	LEX_in_where = 0;
}

PUBLIC void
LEX_prev_state() {
	int lstate = *p_state;
	if (lstate == LEX_INCLUDE && fpsrc) { in_comment = 0; NEWLINE;  }
	p_state--; 
	/* fprintf(stderr,"<== Leaving LEX_prev_state(): %d : %d\n",
	           lstate, *p_state); */
	LEX_in_where = 0;
	BEGIN(*p_state); 
}

PRIVATE void
LEX_all_states(FILE *fp)
{
	fprintf(fp,"*** LEX state numbers:\n");
	fprintf(fp,"LEX_NORMAL            = %d\n",LEX_NORMAL);
	fprintf(fp,"LEX_INCLUDE           = %d\n",LEX_INCLUDE);
	fprintf(fp,"LEX_SET               = %d\n",LEX_SET);
	fprintf(fp,"LEX_TYPE              = %d\n",LEX_TYPE);
	fprintf(fp,"LEX_TABLE             = %d\n",LEX_TABLE);
	fprintf(fp,"LEX_VIEW              = %d\n",LEX_VIEW);
	fprintf(fp,"LEX_FROM              = %d\n",LEX_FROM);
	fprintf(fp,"LEX_ORDERBY           = %d\n",LEX_ORDERBY);
	fprintf(fp,"LEX_EXCLUDED_BY_IFDEF = %d\n",LEX_EXCLUDED_BY_IFDEF);
}

PUBLIC void 
LEX_print_state(FILE *fp)
{
	int level = 0;
	int *p = NULL;
	LEX_all_states(fp);
	do {
	  p = &state[level];
	  fprintf(fp,"LEX-state nesting level#%d : %d\n",level,*p);
	  level++;
	} while ( p != p_state && level < NSTATE_STACK_DEPTH );
}

PUBLIC FILE *
LEX_open_include(const char *filename)
{
  FILE *fp = FOPEN(filename, "r");
  int lineno = 0;

  if (verbose) fprintf(stderr,"*** Including \"%s\"\n",filename);

  if (!fp) {
    SETMSG1("Can't open include-file '%s'",filename);
    YYerror(msg);
  }

#ifdef FLEX_SCANNER
  /* Specific to flex scanner */
  if (include_stack_ptr >= MAX_INCLUDE_DEPTH) {
    SETMSG1("Includes nested too deeply. Maximum depth = %d",MAX_INCLUDE_DEPTH);
    YYerror(msg);
  }
  include_stack[include_stack_ptr++] = YY_CURRENT_BUFFER;
#endif

  lineno = ODB_lineno;
  
  ODB_pushFILE(yyin);
  ODB_pushstr(odb_source);
  ODB_pushi(lineno);
  
  yyin = fp;
  odb_source = STRDUP(filename);
  ODB_lineno = 1;
  include_nesting++;

#ifdef FLEX_SCANNER
  /* Specific to flex scanner */
  yy_switch_to_buffer(yy_create_buffer(yyin, YY_BUF_SIZE));
  BEGIN(*p_state);
#endif

  return fp;
}


PUBLIC void
LEX_close_include()
{
  FILE *tmp = yyin;
  int lineno = ODB_popi();
  extern int ODB_lineno;
  
  if (verbose) 
    fprintf(stderr,"*** End of Include with \"%s\"***\n",odb_source);
    
  ODB_lineno = lineno;
  odb_source = ODB_popstr();
  yyin = ODB_popFILE();
  
  FCLOSE(tmp);

  include_nesting--;

#ifdef FLEX_SCANNER
  /* Specific to flex scanner */
  if ( --include_stack_ptr < 0 ) {
    YYerror("Programming error: Include stack underflow");
  }
  else {
    yy_delete_buffer(YY_CURRENT_BUFFER);
    yy_switch_to_buffer(include_stack[include_stack_ptr]);
  }
#endif

  if (verbose) 
    fprintf(stderr,"*** Proceeding with \"%s\"***\n",odb_source);
}


PUBLIC
double Atof(char *s) /* Modifies the argument (length unchanged) */
{
  double res = 0;
  if (s) {
    char *p = s;
    while (*p) { if (*p == 'd' || *p == 'D') *p = 'e'; ++p; }
    res = atof(s);
  }
  return res;
}


#undef yywrap

PUBLIC 
int yywrap(void) 
{
  int rc = 1; /* No more input to come */
  if (include_nesting > 0) {
    LEX_close_include();
    rc = 0; /* There IS indeed more input to come */
  } 
  return rc;
}

