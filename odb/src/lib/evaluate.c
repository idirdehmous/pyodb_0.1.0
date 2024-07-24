
/* evaluate.c */

#include <ctype.h>
#include <math.h>

#include "evaluate.h"
#include "node.h"
#include "pcma_extern.h"

static FILE *devnull = NULL;

static thsafe_parse_t *thsp = NULL;

#define LastreS   thsp[IT].lastres
#define RecursioN thsp[IT].recursion
#define NparenS   thsp[IT].nparens
#define In_funC   thsp[IT].in_func
#define NumelemS  thsp[IT].numelems
#define UnarY     thsp[IT].unary

#define FOLLOW(s,c) ((s) && *((s)+1) == (c)) 

PRIVATE Bool IsWordBegin(char ch)
{ return (isalpha(ch) || ch == '_' || ch == '$' || ch == '#' || ch == '\\') ? true : false; }

PRIVATE Bool IsWord(char ch)
{ return (isalnum(ch) || ch == '_' || ch == '@' || ch == '.' || ch == '#') ? true : false; }

PRIVATE Bool IsBlank(char ch)
{ return (ch == ' ' || ch == '\t' || ch == '\n') ? true : false; }

PRIVATE Bool IsNumberBegin(char ch)
{ return (isdigit(ch) || ch == '.') ? true : false; }

PRIVATE Bool IsSemi(char ch)
{ return (ch == ';') ? true : false; }

PRIVATE Bool IsSemiOrBlank(char ch)
{ return (ch == ';' || IsBlank(ch)) ? true : false; }

PUBLIC char ToLower(char ch)
{ return isupper(ch) ? tolower(ch) : ch; }

PUBLIC char ToUpper(char ch)
{ return islower(ch) ? toupper(ch) : ch; }

PRIVATE Bool IsOper(char ch)
{
  return (ch  == '*' || ch  == '+' || ch == '=' ||
	  ch  == '/' || ch  == '%' || ch == ':' ||
	  ch  == '|' || ch  == '&' || 
	  ch  == '>' || ch  == '<' || ch == '!' ||
	  ch  == '-' || ch  == '^') ? true : false;
}

PRIVATE Bool IsDot(const char *s)
{
  return (s && s[0] == '.' && !isalnum(s[1])) ? true : false;
}

PRIVATE Bool IsAssignOper(const char *s)
{
  if (s && *s == '=' && !FOLLOW(s,'=')) return true;
  if (FOLLOW(s,'=')) {
    return (*s == ':' || 
	    *s == '+' ||
	    *s == '-' ||
	    *s == '*' ||
	    *s == '/' ||
	    *s == '%' ||
	    *s == '^' ) ? true : false;
  }
  return false;
}


#define SKIP_WHITE_SPACE(s)  while (s < s_end && IsBlank(*(s))) (s)++

#define ISFUNC(s) (*(s) == '`' && IsWordBegin(*((s)+1)))

PUBLIC const char *
KindStr(Kind_t kind)
{
  static char *s = NULL;
  switch (kind) {
  case NAME: s="NAME"; break;
  case NUMBER: s="NUMBER"; break;
  case FUNC: s="FUNC"; break;
  case UNARY_PLUS: s="`+"; break;
  case UNARY_MINUS: s="`-"; break;
  case PLUS: s="+"; break;
  case MINUS: s="-"; break;
  case MUL: s="*"; break;
  case DIV: s="/"; break;
  case MOD: s="%"; break;
  case POWER: s="**"; break;
  case LT: s="<"; break;
  case LE: s="<="; break;
  case GT: s=">"; break;
  case GE: s=">="; break;
  case EQ: s="=="; break;
  case NE: s="!="; break;
  case NOT: s="!"; break;
  case AND: s="&&"; break;
  case OR: s="||"; break;
  case LSHIFT: s="<<"; break;
  case RSHIFT: s=">>"; break;
  case SEMI: s=";"; break;
  case ASSIGN: s=":="; break;
  case LP: s="("; break;
  case RP: s=")"; break;
  case INCR: s="++"; break;
  case DECR: s="--"; break;
  case INCR_ASSIGN: s="+="; break;
  case DECR_ASSIGN: s="-="; break;
  case MUL_ASSIGN: s="*="; break;
  case DIV_ASSIGN: s="/="; break;
  case MOD_ASSIGN: s="%="; break;
  case POWER_ASSIGN: s="^="; break;
  case COMMA: s=","; break;
  case COLON: s=":"; break;
  case QMARK: s="?"; break;
  case EMPTY: s="EMPTY"; break;
  case CMP: s="<=>"; break;
  case DOT: s="."; break;
  default: s = "UNKNOWN"; break;
  }
  return s;
}

PRIVATE Kind_t 
WhatKind(const char *s, int *inc, Bool unary, int *nargs, Bool *left_assoc)
{
  Kind_t kind = UNKNOWN;
  int Inc = 0;
  int Nargs = 0;
  Bool La = true; /* left associativity */
  if (s) {
    Nargs = 2;
    switch (*s) {
    case '*':
      if (FOLLOW(s,'*')) { kind = POWER; Inc += 2; }           /*  X ** Y */
      else if (FOLLOW(s,'=')) { kind = MUL_ASSIGN; Inc += 2; } /*  X *= Y i.e. X = X * Y */
      else { kind = MUL; Inc++; }                              /*  X * Y  */
      break;
    case '/':
      if (FOLLOW(s,'=')) { kind = DIV_ASSIGN; Inc += 2; }  /* X /= Y  i.e. X = X / Y */
      else { kind = DIV; Inc++; }                          /* X / Y  */
      break;
    case '%':
      if (FOLLOW(s,'=')) { kind = MOD_ASSIGN; Inc += 2; }  /* X %= Y  i.e. X = X % Y */
      else { kind = MOD; Inc++; }                          /* X % Y  */
      break;
    case '!':
      if (FOLLOW(s,'=')) { kind = NE; Inc += 2; } /* X != Y */
      else { kind = NOT; Inc++; }                 /* !X     */
      break;
    case '+':
      if (FOLLOW(s,'=')) { kind = INCR_ASSIGN; Inc += 2; } /* X += Y i.e. X = X + Y */
      else if (FOLLOW(s,'+')) { kind = INCR; }             /* X++ or ++X : not supported */
      else { kind = unary ? UNARY_PLUS : PLUS; Inc++; }    /* +X or X-Y  */
      break;
    case '-':
      if (FOLLOW(s,'=')) { kind = DECR_ASSIGN; Inc += 2; } /* X -= Y     */
      else if (FOLLOW(s,'-')) { kind = DECR; }             /* X-- or --X : not supported */
      else { kind = unary ? UNARY_MINUS : MINUS; Inc++; }  /* -X or X-Y  */
      break;
    case '`':
      if (FOLLOW(s,'-')) { kind = UNARY_MINUS; Inc += 2; }
      else if (FOLLOW(s,'+')) { kind = UNARY_PLUS; Inc += 2; }
      break;
    case '^':
      if (FOLLOW(s,'=')) { kind = POWER_ASSIGN; Inc += 2; }  /* X ^= Y  i.e. X = X ^ Y */
      else { kind = POWER; Inc++; }                          /* X ^ Y  i.e. X ** Y */
      break;
    case ':': 
      if (FOLLOW(s,'=')) { kind = ASSIGN; Inc += 2; } /* X := Y */
      break;
    case '=': 
      if (FOLLOW(s,'=')) { kind = EQ; Inc += 2; } /* X == Y     */
      else { kind = ASSIGN; Inc++; }              /* X = <expr> */
      break;
    case '>':
      if (FOLLOW(s,'=')) { kind = GE; Inc += 2; }          /* X >= Y */
      else if (FOLLOW(s,'>')) { kind = RSHIFT; Inc += 2; } /* X >> Y */
      else { kind = GT; Inc++; }                           /* X > Y  */
      break;
    case '<':
      if (FOLLOW(s,'=') && FOLLOW(s+1,'>')) { kind = CMP; Inc += 3; } /* X <=> Y */
      else if (FOLLOW(s,'=')) { kind = LE; Inc += 2; }                /* X <= Y  */
      else if (FOLLOW(s,'<')) { kind = LSHIFT; Inc += 2; }            /* X << Y  */
      else if (FOLLOW(s,'>')) { kind = NE; Inc += 2; }                /* X <> Y  */
      else { kind = LT; Inc++; }                                      /* X < Y   */
      break;
    case '|': 
      if (FOLLOW(s,'|')) { kind = OR; Inc += 2; }  /* X || Y */
      break;
    case '&': 
      if (FOLLOW(s,'&')) { kind = AND; Inc += 2; } /* X && Y */
      break;
    default:
      Nargs = 0;
      break;
    } /* switch (*s) */
    if (nargs) {
      if (kind == UNARY_MINUS || kind == UNARY_PLUS || kind == NOT) Nargs = 1;
    }
    if (left_assoc) {
      if (kind == UNARY_MINUS || kind == UNARY_PLUS  || kind == NOT  ||
	  kind == POWER       || kind == DECR        || kind == INCR ||
	  kind == ASSIGN      || kind == DECR_ASSIGN || kind == INCR_ASSIGN ||
	  kind == MUL_ASSIGN  || kind == DIV_ASSIGN  || kind == MOD_ASSIGN) La = false;
    }
  }    
  if (inc) *inc = Inc;
  if (nargs) *nargs = Nargs;
  if (left_assoc) *left_assoc = La;
  return kind;
}


PRIVATE Bool
cmp_priorities(Bool o1_left_assoc, double o1prio, double o2prio)
{ /* Shunting yard algorithm by Edsger Dijkstra */
  if (o1_left_assoc && (o1prio <= o2prio)) return true;
  if (!o1_left_assoc && (o1prio < o2prio)) return true;
  return false;
}


PRIVATE double
priority(Kind_t kind)
{
  if (kind == UNARY_PLUS || kind == UNARY_MINUS || kind == NOT)          return 11;
  else if (kind == POWER)                                                return 10;
  else if (kind == MUL || kind == DIV || kind == MOD)                    return 9;
  else if (kind == PLUS || kind == MINUS)                                return 8;
  else if (kind == RSHIFT || kind == LSHIFT)                             return 7;
  else if (kind == LT || kind == LE || kind == GE || kind == GT)         return 6;
  else if (kind == EQ || kind == NE || kind == CMP)                      return 5;
  else if (kind == AND)                                                  return 4;
  else if (kind == OR)                                                   return 3;
  else if (kind == INCR_ASSIGN || kind == DECR_ASSIGN)                   return 2;
  else if (kind == MUL_ASSIGN  || kind == DIV_ASSIGN)                    return 2;
  else if (kind == MOD_ASSIGN || kind == POWER_ASSIGN)                   return 2;
  else if (kind == ASSIGN)                                               return 1;
  else                                                                   return 0;
}


typedef struct _cmd_list_t {
  char *cmd;
  char *rpn;
  int rpn_alloc;
  struct _cmd_list_t *next;
} cmd_list_t;


typedef struct _THSAFE_NodeStack_t {
  Node_t **stack;
} THSAFE_NodeStack_t;

static THSAFE_NodeStack_t *node = NULL;
static int  *top_node_stack = NULL;
static int  *max_node_stack = NULL;


PRIVATE cmd_list_t *
ScanCmds(const char *s)
{
  cmd_list_t *cl = NULL;
  if (s) {
    cmd_list_t *plast = NULL;
#if 0
    FprintF(StderR,"ScanCmds(%s)\n",s);
#endif
    while (IsSemiOrBlank(*s)) s++;
    while (*s) {
      char *p, *out;
      int outlen;
      outlen = STRLEN(s);
      ALLOC(out, outlen + 1);
      p = out;
      while (*s && !IsSemi(*s)) { *p++ = ToLower(*s++); }
      *p = '\0';
      if (STRLEN(out) > 0) {
	cmd_list_t *pc;
	CALLOC(pc,1);
	if (!plast) { cl = plast = pc; } 
	else { plast->next = pc; plast = pc; }
	pc->cmd = out;
      }
      else {
	FREE(out);
      }
      while (IsSemiOrBlank(*s)) s++;
    }
  }
  return cl;
}

PUBLIC void
SetTHSPio(int it
	  , int (*prtfunc)(void *, const char *, ...) /* like fprintf */
	  , void *chan_out                            /* like stdout */
	  , void *chan_err                            /* like stderr */
	  )
{
  if (!thsp) InitTHSP(); /* Should never be called from here */
  FprintF = prtfunc;
  StdouT = chan_out;
  StderR = chan_err; 
}

PUBLIC void
InitTHSP()
{
  if (!thsp) {
    DEF_INUMT;
    int it;
    CALLOC(thsp, inumt);
    for (it=1; it<=inumt; it++) {
      Aggr_argnO = 1; /* by default return the 1st argument of multi-arg. aggr. func ;
		         See also include/funcs.h */
    }
  }
  { /* Set I/O defaults */
    DEF_INUMT;
    int it;
    for (it=1; it<=inumt; it++) {
      SetTHSPio(it,
		(int (*)(void *, const char *, ...))fprintf,
		stdout,
		stderr);
    }
    if (!devnull) devnull = fopen("/dev/null", "w");
  }
  if (!node) {
    DEF_INUMT;
    CALLOC(node, inumt);
    CALLOC(top_node_stack, inumt);
    CALLOC(max_node_stack, inumt);
  }
}

PUBLIC const void *
GetTHSP()
{
  return thsp;
}



PRIVATE int
ParseInput(const char *s_in, char *rpn, int *rpnlen)
{
  DEF_IT;
  const char *s = s_in;
  int rc = 0;
  int init_stack_len = OpStackLen();

  if (RecursioN == 0) {
    NparenS = 0;
    In_funC = 0;
    NumelemS = 0;
    UnarY = true;
  }

  if (s_in) {
    int nparens_in = NparenS;
    const char *s_begin = s;
    int slen = STRLEN(s);
    const char *s_end = s + slen;

#if 0
    FprintF(StderR,
	    "ParseInput(%s) [RecursioN=%d]: init_stack_len = %d, rpn='%s', *rpnlen=%d\n",
	    s,RecursioN,init_stack_len,rpn,*rpnlen);
    FprintF(StderR,
	    "\tnparens_in = %d, In_funC = %d, LastreS = %.14g\n",nparens_in,In_funC,LastreS);
#endif

    RecursioN++;
    SKIP_WHITE_SPACE(s);

    while (s < s_end) {
      Bool inword = false;
      Bool assign = false;

      if (IsBlank(*s)) {
	SKIP_WHITE_SPACE(s);
	continue; /* while (s < s_end) */
      }

#if 0
      PrintOpStack(StderR,s);
#endif

      if (*s == ',') { /* Comma found: function call argument list */
	if (!In_funC) {
	  ERROR("A comma-character (',') found while outside function call",-1);
	}
	s++;
	SKIP_WHITE_SPACE(s);
	UnarY = true;
	NumelemS++;
	break; /* while (s < s_end) */
      }

      if (IsDot(s)) { /* Remember the last result (as in "bc" : . ) */
	*rpnlen -= snprintf(rpn,*rpnlen,". ");
	rpn += 2;
	s++;
	UnarY = false;
	goto eof_while;
      }

      if (IsNumberBegin(*s)) { /* Operands : (flp) numbers */
        char *endptr = NULL;
	double value = strtod(s, &endptr);
	int dlen = endptr - s;
	*rpnlen -= snprintf(rpn,*rpnlen,"%*.*s ",dlen,dlen,s);
	rpn += dlen+1;
	s = endptr;
	UnarY = false;
	goto eof_while;
      }

      if (IsWordBegin(*s)) { /* Operands : words */
	int wlen = 0;
        const char *save_s = s++;
	while (IsWord(*s)) s++;
	wlen = s-save_s;
	SKIP_WHITE_SPACE(s);
	if (*s == '(') {
	  char *funcname;
	  int j, nargs = 0;
	  int save_nparens = NparenS++;
	  inword = false;
	  In_funC++;
	  s++;
	  ALLOC(funcname,wlen+1);
	  snprintf(funcname,wlen+1,"%*.*s",wlen,wlen,save_s);
	  SKIP_WHITE_SPACE(s);
	  UnarY = true;
	  while (s < s_end) {
	    SKIP_WHITE_SPACE(s);
	    if (*s == ')') {
	      NparenS--;
	      s++;
	      break; /* while (s < s_end) */
	    }
	    nargs++;
	    {
	      int save_rpnlen = *rpnlen;
	      slen = ParseInput(s,rpn,rpnlen);
	      if (slen == 0) { 
		ERROR1("No progress when processing function arg. list for '%s'",
		       -2, funcname); }
	      else if (slen < 0) { 
		ERROR1("Error encountered while processing function arg. list for '%s'",
		       -3,funcname); 
	      }
	      rpn += (save_rpnlen - (*rpnlen));
	      s += slen;
	    }
	  } /* while (s < s_end) */
	  In_funC--;
	  if (!checkfunc(funcname,nargs)) {
	    ERROR1("# of args mismatch or function '%s' not installed",-4,funcname);
	  }
	  if (NparenS != save_nparens) {
	    ERROR1("Mismatch in function '%s' closing parenthesis",-5,funcname);
	  }
	  slen = snprintf(rpn,*rpnlen,"`%s %d ",funcname,nargs);
	  *rpnlen -= slen;
	  rpn += slen;
	  FREE(funcname);
	  goto eof_while;
	}
	else {
	  *rpnlen -= snprintf(rpn,*rpnlen,"%*.*s ",wlen,wlen,save_s);
	  rpn += wlen+1;
	  inword = true;
	  UnarY = false;
	}
	SKIP_WHITE_SPACE(s);
      }

      if (IsAssignOper(s)) assign = true;

      if (assign && (!inword || NumelemS != 0)) {
	ERROR("Invalid assignment statement; "
	      "Expecting word followed by one of the "
	      "{ '=', ':=', '+=', '-=', '*=', '/=', '%=', '^=' }",-6);
      }

      if (*s  == '(') {/* Opening Parenthesis */
        NparenS++;
	PushOpStack(LP);
	s++;
	UnarY = true;
	goto eof_while; /* while (*s) */
      }
      else if (IsOper(*s)) {
	int nargs = 0;
	int inc = 0;
	Bool left_assoc = true;
	Kind_t kind = WhatKind(s, &inc, UnarY, NULL, &left_assoc);
	if (kind <= UNKNOWN || inc == 0) {
	  const char *skind = KindStr(kind);
	  char errmsg[100];
	  int errlen = sizeof(errmsg);
	  snprintf(errmsg,errlen,"Invalid operator '%s' found near character '%c'",skind,*s);
	  ERROR(errmsg,-7);
	}
	if (OpStackLen() > init_stack_len) {
	  double priokind = priority(kind);
	  Kind_t oprkind = PopOpStack();
	  while (cmp_priorities(left_assoc, priokind, priority(oprkind))) {
	    slen = snprintf(rpn,*rpnlen,"%s ",KindStr(oprkind));
	    *rpnlen -= slen;
	    rpn += slen;
	    if (OpStackLen() > init_stack_len) {
	      oprkind = PopOpStack();
	    }
	    else {
	      oprkind = UNKNOWN;
	      break;
	    }
	  }
	  PushOpStack(oprkind);
	}
	PushOpStack(kind);
	s += inc;
	if (kind == ASSIGN || 
	    kind == LT || kind == LE || kind == GE || kind == GT ||
	    kind == EQ || kind == NE || kind == CMP ||
	    kind == INCR_ASSIGN || kind == DECR_ASSIGN ||
	    kind == MUL_ASSIGN  || kind == DIV_ASSIGN  ||
	    kind == MOD_ASSIGN  || kind == POWER_ASSIGN ) {
	  UnarY = true;
	}
	goto eof_while;
      }
      else if (*s  == ')') { /* Closing Parenthesis */
#if 0
	FprintF(StderR,"Closing Parenthesis found where s='%s'\n",s);
#endif
	if (OpStackLen() > init_stack_len) {
	  Kind_t oprkind = PopOpStack();
	  while (oprkind != LP) {
	    slen = snprintf(rpn,*rpnlen,"%s ",KindStr(oprkind));
	    *rpnlen -= slen;
	    rpn += slen;
	    if (OpStackLen() == init_stack_len) break;
	    oprkind = PopOpStack();
	  }
	}
	NparenS--;
	if (NparenS < nparens_in) {
	  if (In_funC) {
	    NparenS++;
#if 0
	    FprintF(StderR,
		    "--> Before the break while In_funC = %d : nparens_in = %d"
		    ", NparenS now = %d, where s='%s'\n",
		    In_funC, nparens_in, NparenS,s);
#endif
	    break;
	  }
	  else {
	    ERROR("Mismatch in closing parenthesis",-8);
	  }
	}
	s++;
	goto eof_while;
      }
      else if (inword) {
	goto eof_while;
      }
      else if (*s) { /* Syntax error ? */
	FprintF(StderR,"Why I am here ? where s='%s'\n",s);
	ERROR("Why I am here ? Syntax/programming error ?",-9);
      }

    eof_while:
#if 0
      FprintF(StderR,"[eof_while: s='%s']\n",s);
      PrintOpStack(StderR,NULL);
#endif
      NumelemS++;
      continue;

    } /* while (*s) */

    while (OpStackLen() > init_stack_len) {/* While stack is not empty */
      Kind_t oprkind = PopOpStack();
      slen = snprintf(rpn,*rpnlen,"%s ",KindStr(oprkind));
      *rpnlen -= slen;
      rpn += slen;
    }
      
    rc = s - s_begin;
    RecursioN--;
  } /* if (s) */
  if (RecursioN == 0) {
    if (NparenS > 0) { ERROR("Mismatch in opening parenthesis",-10); }
    if (OpStackLen() > 0) { ERROR("OpStack not emptied",-11); }
  }
 finish:
  return rc;
}


PRIVATE int
CreateRPN(cmd_list_t *cmdlist)
{
  DEF_IT;
  int rc = 0;
  cmd_list_t *cl = cmdlist;
  int maxdepth = 0;
  while (cl) {
    int rpnlen = cl->rpn_alloc = (STRLEN(cl->cmd)+1) * 4;
    CALLOC(cl->rpn, rpnlen);
    initOpStack();
#if 0
    FprintF(StderR,"CreateRPN: Going to ParseInput(s=%s, rpn=%p, rpnlen=%d);\n",
	    cl->cmd, cl->rpn, rpnlen);
#endif
    RecursioN = 0; /* Must reset or otherwise doesn't recover from previous errors properly */
    rc = ParseInput(cl->cmd, cl->rpn, &rpnlen);
#if 0
    FprintF(StderR,"CreateRPN: Out. rc=%d; rpnlen=%d;\n",rc,rpnlen);
#endif
    if (rc < 0) goto finish;
    maxdepth = MAX(maxdepth,OpStackMaxLen());
    cl = cl->next;
  } /* while (cl) */
 finish:
  initOpStack();
  if (rc >= 0) {
    maxdepth += 100;
  }
  else {
    maxdepth = rc;
  }
#if 0
    FprintF(StderR,"CreateRPN: maxdepth = %d\n",maxdepth);
#endif
  return maxdepth;
}


PRIVATE void
PushNodeStack(Node_t *p, int *retcode)
{
  int rc = 0;
  DEF_IT;
  if (top_node_stack[IT] == max_node_stack[IT] - 1) {
    ERROR("Node stack is full",-1);
  }
  else {
    node[IT].stack[++top_node_stack[IT]] = p;
  }
 finish:
  if (retcode) *retcode = rc;
  return;
}


PRIVATE Node_t *
PopNodeStack(int *retcode)
{
  int rc = 0;
  DEF_IT;
  Node_t *p = NULL;
  if (top_node_stack[IT] == -1) {
    ERROR("Node stack is empty",-2);
  }
  else {
    p = node[IT].stack[top_node_stack[IT]--];
  }
 finish:
  if (retcode) *retcode = rc;
  return p;
}


PRIVATE Node_t *
CreateExprTree(const char *s_in, int *retcode)
{
  int rc;
  Node_t *p = NULL;
  const char *s = s_in;
  if (s) {
    const char *s_begin = s;
    int slen = STRLEN(s);
    const char *s_end = s + slen;
    Node_t *pfirst = NULL;
    Node_t *plast = NULL;
    int k = 0;
#if 0
    FprintF(StderR,"CreateExprTree(%s)\n",s);
#endif
    while (s < s_end) {
      if (IsBlank(*s)) {
	SKIP_WHITE_SPACE(s);
	continue; /* while (s < s_end) */
      }
      
      CALLOC(p, 1);
      s_begin = s;

      if (IsDot(s)) { /* Last result (LastreS) */
	s++;
	p->kind = DOT;
	goto push_it;
      }
      
      if (IsWordBegin(*s)) { /* Operands : words */
	const char *save_s = s++;
	while (IsWord(*s)) s++;
	slen = s - save_s;
	ALLOC(p->name, slen+1);
	strncpy(p->name,save_s,slen);
	p->name[slen] = '\0';
	p->kind = NAME;
	goto push_it;
      }

      if (ISFUNC(s)) { /* Operands : function followed by # of args */
	const char *save_s = ++s;
	while (IsWord(*s)) s++;
	slen = s - save_s;
	ALLOC(p->name, slen+1);
	strncpy(p->name,save_s,slen);
	p->name[slen] = '\0';
	p->kind = FUNC;
	p->funcptr = NULL;
	SKIP_WHITE_SPACE(s);
	/* fall through : the next MUST BE an integer == # of args */
      }

      if (IsNumberBegin(*s)) { /* Operands : (flp) numbers */
	char *endptr = NULL;
	double value = strtod(s, &endptr);
	s = endptr;
	p->value = value;
	if (p->kind == FUNC) {
	  int j, nargs = value;
	  ALLOC(p->args, nargs);
	  for (j=nargs-1; j>=0; j--) {
	    p->args[j] = PopNodeStack(&rc);
	    if (rc != 0) goto finish;
	  }
	  p->numargs = nargs;
	  p->funcptr = getfunc(p->name, nargs); 
	}
	else {
	  p->kind = NUMBER;
	}
      }
      else {
	int inc = 0;
	int unary = false;
	int j, nargs = 0;
	Kind_t kind = WhatKind(s, &inc, false, &nargs, NULL);
	p->kind = kind;
	ALLOC(p->args, nargs);
	for (j=nargs-1; j>=0; j--) {
	  p->args[j] = PopNodeStack(&rc);
	  if (rc != 0) goto finish;
	}
	p->numargs = nargs;
	s += inc;
      }

    push_it:
      slen = s - s_begin;
#if 0
      k++;
      FprintF(StderR,"CreateExprTree: push_it#%d, kind=%s, s='%*.*s'\n",
	      k,KindStr(p->kind),slen,slen,s_begin);
#endif
      PushNodeStack(p,&rc);
      if (rc != 0) goto finish;
      if (plast) { plast->next = p; plast = p; }
      else { pfirst = plast = p; }
      s++;
    } /* while (s < s_end) */

    p = PopNodeStack(&rc);
  } /* if (s) */
 finish:
  if (retcode) *retcode = rc;
  return p;
}


PUBLIC void
PrintNode(void *fp, void *pnode)
{
  if (fp) {
    static int ntabs = 0;
    int j;
    Node_t *p = pnode;
    if (p) {
      DEF_IT;
      for (j=0; j<ntabs; j++) FprintF(fp," ");
      FprintF(fp,"PrintNode : %p, kind=%s, name=%s, value=%.14g, numargs=%d\n",
	      p, KindStr(p->kind), 
	      p->name ? p->name : "<undef>",
	      p->value, p->numargs);
      if (p->numargs > 0 && p->args) {
	ntabs++;
	for (j=0; j<p->numargs; j++) PrintNode(fp, p->args[j]);
	ntabs--;
      } /* if (p->numargs > 0 && p->args) */
    } /* if (p) */
  } /* if (fp) */
}


PUBLIC void
PrintTree(void *fp, void *ptree)
{
  if (fp) {
    Node_t *p = ptree;
    while (p) {
      PrintNode(fp, p);
      p = p->next;
    } /* while (p) */
  } /* if (fp) */
}


PRIVATE Node_t *
RPNtoTree(cmd_list_t *cmdlist, int maxdepth, int *retcode)
{
  int rc = 0;
  Node_t *p = NULL;
  if (maxdepth >= 0) {
    DEF_IT;
    cmd_list_t *cl = cmdlist;
    Node_t *plast = NULL;
    while (cl) {
      CALLOC(node[IT].stack,maxdepth);
      top_node_stack[IT] = -1;
      max_node_stack[IT] = maxdepth;
      {
	Node_t *root = CreateExprTree(cl->rpn,&rc);
	if (rc == 0) {
	  if (plast) { plast->next = root; plast = root; }
	  else { p = plast = root; }
	}
      }
      FREE(node[IT].stack);
      if (rc != 0) break;
      cl = cl->next;
    }
  }
  else {
    rc = maxdepth;
  }
 finish:
  if (retcode) *retcode = rc;
  return p;
}


#define ASSIGN_MACRO_1(what,assop,err) \
  double oldvalue; \
  char *name = (p->args && p->args[0]) ? p->args[0]->name : NULL; \
  if (!name) { \
    ERROR("Invalid syntax : Expecting " #what " assignment in 'variable " #assop " expression'",err); \
  } \
  oldvalue = getsym(name,&on_error); \
  if (on_error) { \
    ERROR1("Attempt to use uninitialized variable '%s' in 'variable " #assop " expression'",err-1,name); \
  } \
  value = RunNode(p->args[1], &rc); \
  if (rc != 0) goto finish; \
  { double *newvalue = p->args[0]->symval; \
    if (!newvalue) p->args[0]->symval = newvalue = putsym(name,oldvalue assop value); \
    else *newvalue = oldvalue assop value; \
    value = *newvalue; }

#define ASSIGN_MACRO_2(what,assop,func,err) \
  double oldvalue; \
  char *name = (p->args && p->args[0]) ? p->args[0]->name : NULL; \
  if (!name) { \
    ERROR("Invalid syntax : Expecting " #what " assignment in 'variable " #assop " expression'",err); \
  } \
  oldvalue = getsym(name,&on_error); \
  if (on_error) { \
    ERROR1("Attempt to use uninitialized variable '%s' in 'variable " #assop " expression'",err-1,name); \
  } \
  value = RunNode(p->args[1], &rc); \
  if (rc != 0) goto finish; \
  { double *newvalue = p->args[0]->symval; \
    if (!newvalue) p->args[0]->symval = newvalue = putsym(name,func(oldvalue,value)); \
    else *newvalue = func(oldvalue,value); \
    value = *newvalue; }

PUBLIC double
RunNode(void *pnode, int *retcode)
{
  DEF_IT;
  int rc = 0;
  Node_t *p = pnode;
  double value = RMDI;
  if (retcode) *retcode = rc; /* All ok */
  if (p) {
    Kind_t kind = p->kind;
    Bool on_error = false;
    Bool is_fun = (kind == FUNC  ) ? true : false;
    Bool is_sym = (kind == NAME  ) ? true : false;
    Bool is_num = (kind == NUMBER) ? true : false;
    Bool is_dot = (kind == DOT   ) ? true : false;
    if (is_fun) {
      value = callfunc(p, &rc); /* Neat, uh ? */
    }
    else if (is_sym) {
      double *testvalue = p->symval;
      if (!testvalue) {
	testvalue = getsymaddr(p->name);
	if (testvalue) p->symval = testvalue;
      }
      if (testvalue) {
	value = *testvalue;
      }
      else {
	ERROR1("Attempt to use uninitialized variable '%s'",-101,p->name?p->name:NIL);
      }
    }
    else if (is_num) {
      value = p->value;
    }
    else if (is_dot) {
      value = LastreS;
    }
    else if (kind == ASSIGN) {
      char *name = (p->args && p->args[0]) ? p->args[0]->name : NULL;
      if (!name) {
	ERROR("Invalid syntax : Expecting assignment 'variable = expression'",-102);
      }
      value = RunNode(p->args[1], &rc);
      if (rc != 0) goto finish;
      if (!p->args[0]->symval) {
	p->args[0]->symval = putsym(name,value);
      }
      else {
	*p->args[0]->symval = value;
      }
    }
    else if (kind == INCR_ASSIGN) {
      ASSIGN_MACRO_1(increment,+=,-103);
    }
    else if (kind == DECR_ASSIGN) {
      ASSIGN_MACRO_1(decrement,-=,-105);
    }
    else if (kind == MUL_ASSIGN) {
      ASSIGN_MACRO_1(multiplicative,*=,-107);
    }
    else if (kind == DIV_ASSIGN) { /* no FPE checks at the moment */
      ASSIGN_MACRO_1(division,/=,-109);
    }
    else if (kind == MOD_ASSIGN) { /* no FPE checks at the moment */
      ASSIGN_MACRO_2(modulo,%=,fmod,-111);
    }
    else if (kind == POWER_ASSIGN) { /* no FPE checks at the moment */
      ASSIGN_MACRO_2(modulo,*=,pow,-113);
    }
    else if (p->numargs == 1) {
      double y = RunNode(p->args[0], &rc);
      if (rc != 0) goto finish;
      switch(kind) {
      case UNARY_PLUS  : value =  y;  break;
      case UNARY_MINUS : value = -y;  break;
      case NOT         : value =(y == 0); break;
      default: { ERROR("Unrecognized unary-operation",-114); } ; break;
      }
    }
    else if (p->numargs == 2 && kind == OR) {
      value = (RunNode(p->args[0], &rc) != 0) ? 1 : 0;
      if (rc != 0) goto finish;
      if (value == 0) {
	/* Run only if value in previous expression was 0 */
	value = (RunNode(p->args[1], &rc) != 0) ? 1 : 0;
      }
    }
    else if (p->numargs == 2 && kind == AND) {
      value = (RunNode(p->args[0], &rc) != 0) ? 1 : 0;
      if (rc != 0) goto finish;
      if (value == 1) {
	/* Run only if value in previous expression was 1 */
	value = (RunNode(p->args[1], &rc) != 0) ? 1 : 0;
      }
    }
    else if (p->numargs == 2) {
      double x, y;
      x = RunNode(p->args[0], &rc);
      if (rc != 0) goto finish;
      y = RunNode(p->args[1], &rc);
      if (rc != 0) goto finish;
      switch(kind) {
      case PLUS   : value=x+y;       break;
      case MINUS  : value=x-y;       break;
      case MUL    : value=x*y;       break;
      case DIV    : value=x/y;       break; /* no FPE checks at the moment */
      case MOD    : value=fmod(x,y); break; /* no FPE checks at the moment  */
      case POWER  : value=pow(x,y);  break; /* no FPE checks at the moment */
      case EQ     : value=(x==y);    break;
      case NE     : value=(x!=y);    break;
      case CMP    : value=((x < y) ? -1 : ((x > y) ? +1 : 0)); break;
      case LT     : value=(x<y);     break;
      case LE     : value=(x<=y);    break;
      case GT     : value=(x>y);     break;
      case GE     : value=(x>=y);    break;
      case LSHIFT : value=((u_ll_t)x << (u_ll_t)y);  break;
      case RSHIFT : value=((u_ll_t)x >> (u_ll_t)y);  break;
      default: { ERROR("Unrecognized binary-operation",-115); } ; break;
      }
    }
    else {
      ERROR("Unrecognized command in RunNode",-116); 
    }
  }
  else {
    ERROR("An attempt to run an empty command in RunNode",-117);
  }
 finish:
  if (retcode) *retcode = rc;
  return value;
}


PUBLIC double
RunTree(void *ptree, void *fp, int *retcode)
{
  DEF_IT;
  int rc = 0;
  double value = 0;
  Node_t *p = ptree;
#if 0
  PrintTree(StderR,p);
#endif
  while (p) {
    rc = 0;
    value = RunNode(p, &rc);
    if (rc != 0) break;
    if (fp) FprintF(fp,"%.14g\n",value);
    LastreS = value; /* Remember the last result (as in "bc" : . ) */
    p = p->next;
  }
  if (retcode) *retcode = rc;
  return value;
}


PUBLIC void *
ParseTree(const char *cmds, int *retcode)
{
  int rc = 0;

  /* Split cmds into a semi-colon separated list of commands */
 
  cmd_list_t *cmdlist = ScanCmds(cmds);

  /* Create RPN (Reverse Polish Notation) for each command */

  int maxdepth = CreateRPN(cmdlist);

  /* 
     Create parse-tree out of RPN 
     Note : 
     If maxdepth were < 0, then RPNtoTree assigns maxdepth to rc
     and ptree points to NULL 
  */

  Node_t *ptree = RPNtoTree(cmdlist, maxdepth, &rc);

  /* Purge cmdlist */

  int cmdno = 0;
  cmd_list_t *p = cmdlist;
  while (p) {
    cmd_list_t *save_next = p->next;
#if 0
    FprintF(StderR,"#%d:[%s] rpn=[%s] (alloc=%d,actual=%d)\n",
	    ++cmdno,p->cmd,p->rpn,
	    p->rpn_alloc,STRLEN(p->rpn));
#endif
    FREE(p->cmd);
    FREE(p->rpn);
    FREE(p);
    p = save_next;
  }

  /* Return parse tree */

 finish:
  if (rc != 0) {
    DEF_IT;
    FprintF(StderR,"***Error#%d in ParseTree(%s:%d): Unable to parse '%s'\n",
	    rc,__FILE__,__LINE__,cmds?cmds:NIL);
    (void) DelParseTree(ptree);
    ptree = NULL;
  }
  if (retcode) *retcode = rc;
  return ptree;
}


PUBLIC int
DelParseTree(void *ptree)
{
  Node_t *p = ptree;
  int cnt = 0;
  while (p) {
    Node_t *save_next = p->next;
    FREE(p->name);
    FREE(p);
    cnt++;
    p = save_next;
  } /* while (p) */
  return cnt;
}


PUBLIC double
Run(const char *cmds
    , int *retcode
    , int (*prtfunc)(void *, const char *, ...) /* like fprintf */
    , void *chan_out /* like stdout */
    , void *chan_err /* like stderr */
    , Bool output2devnull
    )
{
  DEF_IT;
  int rc = 0;
  double value = 0;
  InitTHSP();
  if (output2devnull) {
    prtfunc = (int (*)(void *, const char *, ...))fprintf;
    chan_out = chan_err = devnull;
  }
  else {
    if (!prtfunc) prtfunc = (int (*)(void *, const char *, ...))fprintf;
    /* if (!chan_out) chan_out = stdout; */
    if (!chan_err) chan_err = stderr;
  }
  SetTHSPio(it, prtfunc, chan_out, chan_err);
  extern void init_RANDOM(); /* odb/lib/random_odb.c */
  init_RANDOM();
  initsym();
  initfunc();
  initOpStack();
  if (cmds && STRLEN(cmds) > 0) {
    void *ptree = ParseTree(cmds, &rc);
    if (rc == 0) {
      value = RunTree(ptree, chan_out, &rc);
    }
    initOpStack();
    (void) DelParseTree(ptree);
  }
 finish:
  if (retcode) *retcode = rc;
  return value;
}


PRIVATE char *
StripBlanks(const char *s, Bool remove_white_space)
{
  char *out = (char *)s;
  if (s && remove_white_space) {
    char *p = out = STRDUP(s);
    while (*s) {
      if (!IsBlank(*s)) *p++ = *s++;
      else s++;
    }
    *p = '\0';
  }
  return out;
}


PUBLIC const char *
StrCaseStr(const char *haystack, const char *needle, Bool ignore_case)
{ /* Case insensitive strstr(), when ignore_case == true  */
  const char *rc = NULL;
  if (!needle || !*needle) {
    rc = haystack;
  }
  else if (!ignore_case) {
    rc = strstr(haystack, needle);
  }
  else {
    /* Case insensitive -part */
    if (haystack) {
      for ( ; *haystack ; ++haystack ) {
	if ( ToUpper(*haystack) == ToUpper(*needle) ) {
	  /*
	   * Matched starting char -- loop through remaining chars.
	   */
	  const char *h = haystack, *n = needle;
	  for ( ; *h && *n ; ++h, ++n ) {
	    if ( ToUpper(*h) != ToUpper(*n) ) break; /* for ( ; *h && *n ; ++h, ++n ) */
	  } /* for ( ; *h && *n ; ++h, ++n ) */
	  if ( !*n ) {
	    /* matched all of 'needle' to null termination */
	    rc = haystack; /* return the start of the match */
	    break; /* for ( ; *haystack ; ++haystack ) */
	  }
	}
      } /* for ( ; *haystack ; ++haystack ) */
    }
  }
  return rc;
}


PUBLIC char *
ReplaceSubStrings(const char *haystack, 
		  const char *needle, 
		  const char *repl_with, 
		  Bool all_occurences,
		  Bool remove_white_space,
		  Bool ignore_case)
{ 
  /* 
     Finds substring "needle" from the input string "haystack" and replaces
     all or just the first occurence(s) with the "repl_with" string 

     For example: 
     - Replace the FIRST substring "count(*)" with "#1" in "count(*)/4 + cOUNt( * )"
     - Remove white space (' ', '\t', '\n') as well, but BEFORE the replacement
     - Ignore case

     ReplaceSubStrings("count(*)/4 + cOUNt( * )", "count(*)", "#1", false, true, true) 
     --> "#1/4+count(*)"
  */

  char *out = NULL;
  if (haystack && needle && repl_with) {
    char *phaystack = StripBlanks(haystack,remove_white_space);
    int cnt = 0;
    int lenhs = STRLEN(phaystack);
    int len = lenhs;
    int lenndl = STRLEN(needle);
    int lenwith = STRLEN(repl_with);
    const char *p = StrCaseStr(phaystack, needle, ignore_case);
    while (p) {
      cnt++;
      len += (lenwith - lenndl);
      if (!all_occurences) break;
      p = StrCaseStr(p+lenndl, needle, ignore_case);
    }
    if (cnt == 0) { /* No match ==> no change */
      out = STRDUP(phaystack);
    }
    else {
      const char *x = phaystack;
      char *s;
      ALLOC(out,len+1);
      s = out;
      while (cnt-- >= 0) {
	p = StrCaseStr(x, needle, ignore_case);
	if (!p) p = phaystack + lenhs;
	while (x < p) *s++ = *x++;
	if (p == phaystack + lenhs) break;
	strncpy(s,repl_with,lenwith);
	s += lenwith;
	x = p+lenndl;
      }
      *s = '\0';
    }
    if (phaystack != haystack) FREE(phaystack);
  }
  return out;
}


PUBLIC char *
ReplaceSubStringsBetween(const char *str, 
			 const char *mark_begin, const char *mark_end,
			 const char *repl_with, 
			 Bool all_occurences,
			 Bool remove_white_space,
			 Bool ignore_case)
{
  /* 
     Replaces all (or only the first) occurences between
     "str_begin" and "str_end" (inclusive) in "str" with string "repl_with" 

     For example : 
     - Replace text between all "<{" and "}>" markers in string "<{count(*)>}/4 + <{count( * )}>"
     - Do NOT remove the white space (that would have removed BEFORE substitution)
     - Be case sensitive i.e. do NOT ignore the case

     ReplaceSubStringsBetween("<{count(*)>}/4 + <{count( * )}>", "<{", "}>", "#1", true, false, false) 
     --> "#1/4 + #1"
  */
  
  char *out = NULL;
  if (str && mark_begin && mark_end && repl_with) {
    char *pstr = StripBlanks(str,remove_white_space);
    int cnt = 0;
    int lenstr = STRLEN(pstr);
    int len = lenstr;
    int lenbegin = STRLEN(mark_begin);
    int lenend = STRLEN(mark_end);
    int lenwith = STRLEN(repl_with);
    const char *p1 = StrCaseStr(pstr,mark_begin, ignore_case);
    const char *p2 = p1 ? StrCaseStr(p1+lenbegin,mark_end, ignore_case) : NULL;
    while (p1 && p2) {
      cnt++;
      len += (lenwith - (p2 + lenend - p1));
      if (!all_occurences) break;
      p2 += lenend;
      p1 = StrCaseStr(p2,mark_begin, ignore_case);
      p2 = p1 ? StrCaseStr(p1+lenbegin,mark_end, ignore_case) : NULL;
    }
    if (cnt == 0) {
      out = STRDUP(pstr);
    }
    else {
      const char *x = pstr;
      char *s;
      ALLOC(out,len+1);
      s = out;
      p2 = x;
      while (cnt-- >= 0) {
	p1 = StrCaseStr(p2, mark_begin, ignore_case);
	if (p1) while (x < p1) *s++ = *x++;
	p2 = p1 ? StrCaseStr(p1+lenbegin,mark_end, ignore_case) : NULL;
	if (!p2) {
	  p2 = pstr + lenstr;
	  while (x < p2) *s++ = *x++;
	  break;
	}
	strncpy(s,repl_with,lenwith);
	s += lenwith;
	x = (p2 += lenend);
      }
      *s = '\0';
    }
    if (pstr != str) FREE(pstr);
  }
  return out;
}

#include "info.h"

PUBLIC char *
GetLHSwhenSimpleExpr(const char *exprstr, int *oper, double *rhsvalue, 
		     const void *Set, int nset)
{
  char *lhsstr = NULL;
  if (oper) *oper = UNKNOWN;
  if (rhsvalue) *rhsvalue = 0;
  if (exprstr && oper && rhsvalue) {
    int iret = 0;
    Node_t *expr = ParseTree(exprstr, &iret);

    if (expr && iret == 0) {
      Node_t *p = expr;

      if (p->numargs == 2 && p->args) { 
	Node_t *lhs = p->args[0];
	Node_t *rhs = p->args[1];

	if (lhs && rhs &&
	    lhs->numargs == 0 && rhs->numargs == 0) {
	  Bool swap = false;

	  if (p->kind != LE && p->kind != LT &&
	      p->kind != GE && p->kind != GT &&
	      p->kind != EQ && p->kind != NE) {
	    /* Operator not one of (LE, GE, EQ, NE, LT, GT) */
	    goto bailout;
	  }

	  if (Set && nset > 0) {
	    const set_t *set = Set;
	    int j;
	    const char *s = NULL;

	    s = lhs->name;
	    if (lhs->kind == NAME && IS_DOLLAR(s) && !IS_USDHASH(s)) {
	      for (j=0; j<nset; j++) {
		if (strequ(set[j].name, s)) {
		  lhs->value = set[j].value;
		  lhs->kind = NUMBER;
		  break;
		}
	      } /* for (j=0; j<nset; j++) */
	    }

	    s = rhs->name;
	    if (rhs->kind == NAME && IS_DOLLAR(s) && !IS_USDHASH(s)) {
	      for (j=0; j<nset; j++) {
		if (strequ(set[j].name, s)) {
		  rhs->value = set[j].value;
		  rhs->kind = NUMBER;
		  break;
		}
	      } /* for (j=0; j<nset; j++) */
	    }
	  } /* if (set && nset > 0) */

	  if (lhs->kind == NUMBER && rhs->kind == NAME) {
	    /* Interchange "NUMBER oper NAME" to "NAME oper NUMBER" */
	    Node_t *tmp = lhs;
	    lhs = rhs;
	    rhs = tmp;
	    swap = true;
	  }

	  if (lhs->kind != NAME || rhs->kind != NUMBER) {
	    /* Expression not simple enough */
	    goto bailout;
	  }

	  if (swap) {
	    switch(p->kind) {
	    case LE: p->kind = GE; break;
	    case LT: p->kind = GT; break;
	    case GE: p->kind = LE; break;
	    case GT: p->kind = LT; break;
	    default: /* do nothing */ break;
	    } /* switch(p->kind) */
	  }

	  /* Plain sailing !! */
	  lhsstr = STRDUP(lhs->name);
	  *oper = p->kind;
	  *rhsvalue = rhs->value;
	} /* if (lhs && rhs && ... ) */

      } /* if (p->numargs == 2 && p->args) */
    } /* if (expr && iret == 0) */
  bailout:
    (void) DelParseTree(expr);
  } /* if (exprstr && oper && rhsvalue) */
  return lhsstr;
}

