#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#ifdef _OPENMP
#include <omp.h>
#else
static int 
omp_get_max_threads(void)
{
  return 1;
}
static int 
omp_get_thread_num(void)
{
  return 0;
}
#endif

#define MAXLINELEN 65535

typedef struct list_ {
  char *cmd;
  struct list_ *next;
} List_t;

int main(int argc, char *argv[])
{
  int num_threads = omp_get_max_threads();
  int i, j, thread, rc, numtasks;
  int errcnt = 0;
  int *ret = NULL;
  char **task = NULL;
  char oneline[MAXLINELEN+1];
  List_t *list = NULL;
  List_t *prev = NULL;
  List_t *curr = NULL;
  FILE *fp = (argc > 1) ? fopen(argv[1], "r") : stdin;

  if (!fp) {
    perror((argc > 1) ? argv[1] : "<STDIN>");
    return(1);
  }

  numtasks = 0;
  while (fgets(oneline, sizeof(oneline), fp)) {
    char *p = strchr(oneline,'\n');
    if (p) *p = '\0';
    if (strlen(oneline) == 0) continue;
    curr = malloc(sizeof(*curr));
    curr->cmd = strdup(oneline);
    curr->next = NULL;
    if (!list) {
      list = prev = curr;
    }
    else {
      prev->next = curr;
      prev = curr;
    }
    numtasks++;
  } /* while (fgets(oneline, sizeof(oneline), fp)) */

  if (numtasks == 0) return(0);

  task = malloc(numtasks * sizeof(*task));
  curr = list;
  for (j=0; j<numtasks; j++) {
    task[j] = curr->cmd;
    /* fprintf(stderr,"task#%d='%s'\n",j+1,task[j]); */
    curr = curr->next;
  }

  ret = malloc(num_threads * sizeof(*ret));
  for (i=0; i<num_threads; i++) ret[i] = 0;

#ifdef _OPENMP
#pragma omp parallel private(j, thread, rc) shared(ret, task, errcnt)
#pragma omp for schedule(dynamic,1)
#endif
  for (j=0; j<numtasks; j++) {
    thread = omp_get_thread_num();
#ifdef _OPENMP
#pragma omp critical
#endif
    {
      fprintf(stderr,"(task#%d_%d|tid#%d_%d) %s\n",
	      j+1,numtasks,thread+1,num_threads,task[j]);
    }

    rc = system(task[j]);

#ifdef _OPENMP
#pragma omp critical
#endif
    {
      ret[thread] += rc;
      if (rc != 0) {
	errcnt++;
	fprintf(stderr,"***Errors found in task#%d, thread#%d ; rc=%d : '%s'\n",
		j+1,thread+1,rc,task[j]);
      }
    }
  } /* for (j=0; j<numtasks; j++) */

  rc = 0;
  for (i=0; i<num_threads; i++) rc += ret[i];

  if (rc != 0 || errcnt > 0) return(1);
}
