
/* stack.c */

#include "evaluate.h"

typedef struct _opstk_t {
  Kind_t kind;
  struct _opstk_t *prev;
  struct _opstk_t *next;
} opstk_t;

static opstk_t **OpStack_begin = NULL;
static opstk_t **OpStack_end = NULL;
static int *OpStack_len = NULL;
static int *OpStack_maxlen = NULL;

PUBLIC void
initOpStack()
{
  Bool initialized = false;
  if (!initialized) {
    DEF_INUMT;
    CALLOC(OpStack_begin, inumt);
    CALLOC(OpStack_end, inumt);
    CALLOC(OpStack_len, inumt);
    CALLOC(OpStack_maxlen, inumt);
    initialized = true;
  }
  {
    DEF_IT;
    opstk_t *p = OpStack_begin[IT];
    while (p) {
      opstk_t *save_next = p->next;
      FREE(p);
      p = save_next;
    }
    OpStack_begin[IT] = OpStack_end[IT] = NULL;
    OpStack_len[IT] = -1;
    OpStack_maxlen[IT] = -1;
    PushOpStack(EMPTY);
  }
}


PUBLIC int
OpStackLen()
{
  DEF_IT;
  return OpStack_len[IT];
}


PUBLIC int
OpStackMaxLen()
{
  DEF_IT;
  return OpStack_maxlen[IT];
}


PUBLIC void
PushOpStack(Kind_t kind)
{
  if (kind != UNKNOWN) {
    DEF_IT;
    opstk_t *p;
    CALLOC(p,1);
    p->kind = kind;
    if (!OpStack_begin[IT]) {
      OpStack_begin[IT] = OpStack_end[IT] = p;
    }
    else {
      p->prev = OpStack_end[IT];
      OpStack_end[IT]->next = p;
      OpStack_end[IT] = p;
    }
    OpStack_len[IT]++;
    if (OpStack_len[IT] > OpStack_maxlen[IT]) OpStack_maxlen[IT] = OpStack_len[IT];
  }
}


PUBLIC Kind_t
PopOpStack()
{
  opstk_t *p = NULL;
  if (OpStackLen() > 0) { 
    DEF_IT;
    p = OpStack_end[IT];
    OpStack_end[IT] = p->prev;
    OpStack_len[IT]--;
  }
  return p ? p->kind : UNKNOWN;
}


PUBLIC void
PrintOpStack(void *fp, const char *msg)
{
  if (fp) {
    DEF_IT;
    const thsafe_parse_t *thsp = GetTHSP();
    opstk_t *p = OpStack_begin[IT];
    int j = -1;
    FprintF(fp,"PrintOpStack : len=%d : %s :",OpStackLen(),msg?msg:"");
    while (p) {
      if (j >= OpStackLen()) break;
      if (j >= 0) FprintF(fp,"[%d]='%s' ",j,KindStr(p->kind));
      j++;
      p = p->next;
    }
    if (j > -1) FprintF(fp,"\n");
  }
}
