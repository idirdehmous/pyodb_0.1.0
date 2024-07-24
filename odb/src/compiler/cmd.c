#include "defs.h"

PUBLIC int ODB_ncmds = 0;

PRIVATE ODB_Cmd *first_cmd = NULL;
PRIVATE ODB_Cmd *last_cmd = NULL;

PUBLIC ODB_Cmd *
ODB_start_cmd() { return first_cmd; }

PUBLIC ODB_Cmd *
ODB_new_cmd(ODB_Tree *node)
{
  extern int ODB_lineno;
  ODB_Cmd *pcmd;

  ALLOC(pcmd,1);

  if (first_cmd)  
    last_cmd->next = pcmd;
  else 
    first_cmd = pcmd;

  last_cmd = pcmd;

  pcmd->node = node;
  pcmd->lineno = ODB_lineno;
  pcmd->next = NULL;

  /*
  fprintf(stderr,"ODB_new_cmd(#%d) : %p\n",
	  ODB_lineno, pcmd);
	  */

  ODB_ncmds++;

  return pcmd;
}
