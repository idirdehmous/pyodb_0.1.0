
/* history.c */

#include "history.h"


typedef struct _hist_tt {
  char *cmd;
  int Date;
  int Time;
  struct _hist_tt *prev;
  struct _hist_tt *next;
} hist_tt;


static hist_tt History = { "", 0, 0, NULL, NULL };
static hist_tt *curHistory = &History;
static hist_tt *lastHistory = &History;


PUBLIC void
ResetHistory()
{
  curHistory = lastHistory;
}


PUBLIC const char *
AddHistory(const char *cmd, const int *Date, const int *Time)
{
  if (cmd && STRLEN(cmd) > 0) {
    char *p;
    char *hcmd = STRDUP(cmd);
    hist_tt *h = NULL;
    CALLOC(h, 1);

    /* Change non-printable chars into blanks */
    p = hcmd;
    while (*p) {
      if (!isprint(*p)) *p = ' ';
      p++;
    }

    /* Strip leading blanks */
    p = hcmd;
    while (isspace(*p)) {
      p++;
    }

    h->cmd = STRDUP(p);
    FREE(hcmd);

    if (Date && Time) {
      h->Date = *Date;
      h->Time = *Time;
    }
    else {
      char buf[80];
      time_t tp;
      time(&tp);
      strftime(buf, sizeof(buf), "%Y%m%d %H%M%S", localtime(&tp));
      sscanf(buf,"%d %d", &h->Date, &h->Time);
    }
    h->prev = lastHistory;
    lastHistory->next = h;
    lastHistory = h;
    ResetHistory();
    return h->cmd;
  }
  else
    return NULL;
}


PUBLIC const char *
GetHistory(int next)
{
  hist_tt *h = NULL;
  if (next > 0) {
    h = curHistory->next;
    if (h) curHistory = h;
  }
  else {
    h = curHistory;
    if (curHistory->prev) curHistory = curHistory->prev;
  }
  return h ? h->cmd : NULL;
}


PUBLIC void
PrintHistory(FILE *fp)
{
  const hist_tt *h = &History;
  if (h && fp) {
    h = h->next; /* Skip the first one (from static memory) */
    while (h) {
      if (STRLEN(h->cmd) > 0) {
	fprintf(fp, "%8.8d %6.6d %s\n", h->Date, h->Time, h->cmd);
      }
      h = h->next;
    }
    fflush(fp);
  }
}


#define SET_FILENAME(name) \
  FILE *fp; \
  char *filename; \
  char *env = getenv("HOME"); \
  if (!env) env = "."; \
  ALLOC(filename, STRLEN(env) + STRLEN(name) + 2); \
  sprintf(filename,"%s/%s",env,name)


PUBLIC char *
LoadHistory(const char *name)
{
  SET_FILENAME(name);
  fp = fopen(filename,"r");
  if (fp) {
    int nlines = 0;
    char buf[4096];
    while (fgets(buf, sizeof(buf), fp)) {
      int Date, Time;
      char *p = strchr(buf,'\n');
      if (p) *p = 0;
      if (sscanf(buf,"%d %d", &Date, &Time) == 2) {
	p = buf + 16;
	if (AddHistory(p,&Date,&Time)) nlines++;
      }
    }
    fclose(fp);
  }
  return filename;
}


PUBLIC char *
SaveHistory(const char *name, int nlastlines)
{
  SET_FILENAME(name);
  fp = fopen(filename,"w");
  if (fp) {
    const hist_tt *h = NULL;
    int *num = NULL;
    int maxcnt;
    int cnt = 0;
    h = &History;
    if (h) {
      h = h->next; /* Skip the first one (from static memory) */
      while (h) {
	if (STRLEN(h->cmd) > 0) {
	  cnt++;
	}
	h = h->next;
      } /* while (h) */
    } /* if (h) */
    maxcnt = cnt;
    ALLOC(num, maxcnt);
    cnt = 0;
    h = &History;
    if (h) {
      h = h->next; /* Skip the first one (from static memory) */
      while (h) {
	if (STRLEN(h->cmd) > 0) {
	  num[cnt] = maxcnt - cnt;
	  cnt++;
	}
	h = h->next;
      } /* while (h) */
    } /* if (h) */
    h = &History;
    cnt = 0;
    if (h) {
      h = h->next; /* Skip the first one (from static memory) */
      while (h) {
	if (STRLEN(h->cmd) > 0) {
	  if (num[cnt] <= nlastlines) {
	    fprintf(fp, "%d %d %s\n", h->Date, h->Time, h->cmd);
	  }
	  cnt++;
	}
	h = h->next;
      } /* while (h) */
    } /* if (h) */
    FREE(num);
    fclose(fp);
  }
  return filename;
}


PUBLIC void
DelHistory()
{
  hist_tt *h = &History;
  if (h) {
    h = h->next; /* Skip the first one (from static memory) */
    while (h) {
      hist_tt *save_h = h;
      FREE(h->cmd);
      FREE(h);
      h = save_h->next;
    } /* while (h) */
    lastHistory = curHistory = &History;
  } /* if (h) */
}
