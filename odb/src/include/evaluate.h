#ifndef _EVALUATE_H_
#define _EVALUATE_H_

/* evaluate.h */

#include "alloc.h"
#include "symtab.h"

typedef enum {
  EMPTY = -255, INCR = -1, DECR = -2  /* not supported */
  , UNKNOWN = 0
  , NAME, NUMBER, FUNC
  , UNARY_PLUS, UNARY_MINUS
  , LE, GE, EQ, NE, AND, OR
  , LSHIFT, RSHIFT
  , INCR_ASSIGN, DECR_ASSIGN
  , MUL_ASSIGN, DIV_ASSIGN, MOD_ASSIGN, POWER_ASSIGN
  , CMP
  , LT = '<', GT = '>', NOT = '!'
  , PLUS = '+', MINUS = '-', MUL = '*', DIV = '/', MOD = '%', POWER = '^'
  , SEMI = ';', ASSIGN = '=', LP = '(', RP = ')'
  , COMMA = ',', COLON = ':', QMARK='?'
  , DOT = '.'
} Kind_t;

typedef struct _thsafe_parse_t {
  double lastres;
  int recursion;
  int nparens;
  int in_func;
  int numelems;
  int aggr_argno;
  Bool unary;
  int (*prtfunc)(void *, const char *, ...); /* like fprintf */
  void *chan_out; /* like stdout */
  void *chan_err; /* like stderr */
} thsafe_parse_t;

#define Aggr_argnO thsp[IT].aggr_argno

#define FprintF  thsp[IT].prtfunc
#define StdouT   thsp[IT].chan_out
#define StderR   thsp[IT].chan_err

/* for now : ERROR("string") */
#define ERROR(s,fatal) { \
  FprintF(StderR,"***Error#%d: %s (%s:%d)\n",fatal,s,__FILE__,__LINE__); \
  if (fatal > 0) { RAISE(SIGABRT); } else { rc = fatal; goto finish; } }

#define ERROR1(s,fatal,arg1) { \
  FprintF(StderR,"***Error#%d: ",fatal); FprintF(StderR,s,arg1); \
  FprintF(StderR," (%s:%d)\n",__FILE__,__LINE__); \
  if (fatal > 0) { RAISE(SIGABRT); } else { rc = fatal; goto finish; } }

#define ERROR2(s,fatal,arg1,arg2) { \
  FprintF(StderR,"***Error#%d: ",fatal); FprintF(StderR,s,arg1,arg2); \
  FprintF(StderR," (%s:%d)\n",__FILE__,__LINE__); \
  if (fatal > 0) { RAISE(SIGABRT); } else { rc = fatal; goto finish; } }

#define ERROR3(s,fatal,arg1,arg2,arg3) { \
  FprintF(StderR,"***Error#%d: ",fatal); FprintF(StderR,s,arg1,arg2,arg3); \
  FprintF(StderR," (%s:%d)\n",__FILE__,__LINE__); \
  if (fatal > 0) { RAISE(SIGABRT); } else { rc = fatal; goto finish; } }

/* from funcs.c */

extern void initfunc();
extern const void *getfunc(const char *s, int numargs);
extern Bool checkfunc(const char *s, int numargs);
extern double callfunc(void *pnode, int *retcode);
extern void printfunc(void *fp);

/* from evaluate.c */

extern const char *KindStr(Kind_t kind);
extern void *ParseTree(const char *cmds, int *retcode);
extern int DelParseTree(void *ptree);
extern void InitTHSP();
extern const void *GetTHSP();
extern void SetTHSPio(int it
		      , int (*prtfunc)(void *, const char *, ...) /* like fprintf */
		      , void *chan_out                            /* like stdout */
		      , void *chan_err                            /* like stderr */
		      );
extern void PrintNode(void *fp, void *pnode);
extern void PrintTree(void *fp, void *ptree);

extern double RunTree(void *ptree, void *fp, int *retcode);
extern double RunNode(void *pnode, int *retcode);

extern double Run(const char *cmds
		  , int *retcode
		  , int (*prtfunc)(void *, const char *, ...) /* like fprintf */
		  , void *chan_out /* like stdout */
		  , void *chan_err /* like stderr */
		  , Bool output2devnull
		  );

extern char *ReplaceSubStrings(const char *haystack, 
			       const char *needle, 
			       const char *repl_with, 
			       Bool all_occurences,
			       Bool remove_white_space,
			       Bool ignore_case);

extern char *ReplaceSubStringsBetween(const char *str, 
				      const char *mark_begin, const char *mark_end,
				      const char *repl_with, 
				      Bool all_occurences,
				      Bool remove_white_space,
				      Bool ignore_case);

extern const char *
StrCaseStr(const char *haystack, const char *needle, Bool ignore_case);

extern char ToLower(char ch);
extern char ToUpper(char ch);

extern char *GetLHSwhenSimpleExpr(const char *exprstr, int *oper, double *rhsvalue,
				  const void *Set, int nset);

/* from generic.c */

extern char *S2D_fix(const void *Info, const char *s);

/* from stack.c */

extern void initOpStack();
extern int OpStackLen();
extern int OpStackMaxLen();
extern void PushOpStack(Kind_t kind);
extern Kind_t PopOpStack();
extern void PrintOpStack(void *fp, const char *msg);

/* from curses.c */

extern int
curse_this(const char *progname, 
	   const char *prompt, 
	   const char *initfile, 
	   int lenHistory,
	   double (*ExecCmds)(const char *cmds
			      , int *retcode
			      , int (*prtfunc)(void *, const char *, ...) /* like fprintf */
			      , void *chan_out /* like stdout */
			      , void *chan_err /* like stderr */
			      , Bool output2devnull
			      ),
	   Bool intermed
	   );

extern double
RunShell(const char *cmds
	 , int *retcode
	 , int (*prtfunc)(void *, const char *, ...) /* like fprintf */
	 , void *chan_out /* like stdout */
	 , void *chan_err /* like stderr */
	 , Bool output2devnull
	 );

#endif /* _EVALUATE_H_ */
