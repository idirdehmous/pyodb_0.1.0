#include "odb.h"

/* routine to change the default sorting functions nearly on the fly */

#define MAX_KNOWN_FUNC 6

#ifdef RS6K
static int prev_fun = 4;
#elif defined(VPP) || defined(NECSX)
/* .. or any other vector machine */
static int prev_fun = 0;
#else
/* scalar prozezzorz */
static int prev_fun = 1;
#endif

/* From IFSAUX */

extern void 
rsort32_setup_(void (*func)(const    int *Mode,
			    const    int *N,
			    const    int *Inc,
			    const    int *Start_addr,
			    unsigned int Data[],
			    int      index[],
			    const    int *Index_adj,
			    int     *retc),
	       int *speedup);

/* Known function  0 & 1 */

extern void 
rsort32_(const    int *Mode,
	 const    int *N,
	 const    int *Inc,
	 const    int *Start_addr,
	 unsigned int  Data[],
	          int  index[],
	 const    int *Index_adj,
	          int *retc);

/* From ODB */

/* Known function  2 & 3 */

extern void 
rsort32_odb_(const    int *Mode,
	     const    int *N,
	     const    int *Inc,
	     const    int *Start_addr,
	     unsigned int  Data[],
	              int  index[],
	     const    int *Index_adj,
	              int *retc);

/* Known function 4 & 5 (rsort32_ibm_ genuinely invokes jsort.F only on RS6K) */

extern void 
rsort32_ibm_(const    int *Mode,
	     const    int *N,
	     const    int *Inc,
	     const    int *Start_addr,
	     unsigned int  Data[],
	              int  index[],
	     const    int *Index_adj,
	              int *retc);

extern int rsort32_odb_SpeedUp; /* for now */

void
setup_sort_(const int *func_no,
	    int *prev_func_no)
{
  int Func_no = prev_fun;

  if (func_no) {
    Func_no = *func_no;
  }
  else {
    char *p = getenv("ODB_SORTING_FUNCTION");
    if (p) Func_no = atoi(p);
  }

  if (prev_func_no) *prev_func_no = prev_fun;

  if (Func_no >= 0 && Func_no <= MAX_KNOWN_FUNC) {
    boolean changed = 0;
    int speedup;
    int Myproc;

    codb_procdata_(&Myproc, NULL, NULL, NULL, NULL);

    changed = (Func_no != prev_fun) ? 1 : 0;

    switch (Func_no) {
    case 0: /* link-time default (on non-IBM/RS600 machines & vector machines) */
      speedup = 0;
      rsort32_setup_(rsort32_, &speedup);
      break;
    case 1:/* link-time default (on non-IBM/RS600 machines & scalar machines) */
      speedup = 1;
      rsort32_setup_(rsort32_, &speedup);
      break;
    case 2:
      rsort32_odb_SpeedUp = speedup = 0;
      rsort32_setup_(rsort32_odb_, &speedup);
      break;
    case 3:
      rsort32_odb_SpeedUp = speedup = 1;
      rsort32_setup_(rsort32_odb_, &speedup);
      break;
    case 4: /* link-time default (on IBM/RS600 machines only) */
      speedup = 0;
      rsort32_setup_(rsort32_ibm_, &speedup);
      break;
    case 5:
      speedup = 1;
      rsort32_setup_(rsort32_ibm_, &speedup);
      break;
    default:
      changed = 0;
      break;
    } /* switch (Func_no) */

    if (Myproc == 1 && changed) {
      fprintf(stderr, 
	      "setup_sort_(): ODB_SORTING_FUNCTION changed from %d to %d (speedup param.=%d)\n",
	      prev_fun, Func_no, speedup);
    }

    prev_fun = Func_no;
  }
}
