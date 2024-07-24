#ifndef HAS_MYSQL
void dummy_odb2mysql() {}
#else

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mysql.h"

#include "alloc.h"

void codb2mysql_do_(unsigned long long int *connid,
		    const char *query,
		    int *retcode
		    /* Hidden arguments */
		    , int query_len
		    )
{
  DECL_FTN_CHAR(query);
  MYSQL *conn = (MYSQL *)(*connid);
  static int first_time = 1;
  if (first_time) {
    fprintf(stderr,"do(): connection id=%llu %p\n",*connid,conn);
    first_time = 0;
  }

  *retcode = 0;
  ALLOC_FTN_CHAR(query);
  {
    int state = mysql_query(conn, p_query);
    if (state != 0) {
      fprintf(stderr,mysql_error(conn));
      *retcode = -1;
      goto finish;
    }
  }
 finish:
  FREE_FTN_CHAR(query);
}

void codb2mysql_close_(unsigned long long int *connid,
		       int *retcode)
{
  MYSQL *conn = (MYSQL *)(*connid);
  fprintf(stderr,"close(): connection id=%llu %p\n",*connid,conn);
  *retcode = 0;
  mysql_close(conn);
}

void codb2mysql_open_(const char *dbname,
		      const char *mode,
		      const char *server,
		      const char *user,
		      const char *password,
		      unsigned long long int *connid
		      /* Hidden arguments */
		      , int dbname_len
		      , int mode_len
		      , int server_len
		      , int user_len
		      , int password_len
		      )
{
  DECL_FTN_CHAR(dbname);
  DECL_FTN_CHAR(mode);
  DECL_FTN_CHAR(server);
  DECL_FTN_CHAR(user);
  DECL_FTN_CHAR(password);

  ALLOC_FTN_CHAR(dbname);
  ALLOC_FTN_CHAR(mode);
  ALLOC_FTN_CHAR(server);
  ALLOC_FTN_CHAR(user);
  ALLOC_FTN_CHAR(password);

  *connid = -1;

  {
    MYSQL *conn, mysql;

    mysql_init(&mysql);
    conn = mysql_real_connect(&mysql,
			      p_server, p_user, NULL,
			      p_dbname, 0, NULL, 0
			      );

    if (conn == NULL) {
      fprintf(stderr,mysql_error(&mysql));
      goto finish;
    }

    if (strcmp(p_mode,"NEW") == 0 || strcmp(p_mode,"new") == 0) {
      int state;
      char *cmd;
      int cmdlen = strlen(p_dbname) + 100;
      ALLOC(cmd, cmdlen);
      sprintf(cmd, "drop database if exists %s", p_dbname);
      state = mysql_query(conn, cmd);
      if (state != 0) {
	fprintf(stderr,mysql_error(conn));
	goto finish;
      }
      FREE(cmd);
      ALLOC(cmd, cmdlen);
      sprintf(cmd, "create database %s", p_dbname);
      state = mysql_query(conn, cmd);
      if (state != 0) {
	fprintf(stderr,mysql_error(conn));
	goto finish;
      }
      FREE(cmd);
    }
    
    *connid = (unsigned long long int) conn;
    fprintf(stderr,"open(): connection id=%llu %p\n",*connid,conn);
   }

 finish:

  FREE_FTN_CHAR(dbname);
  FREE_FTN_CHAR(mode);
  FREE_FTN_CHAR(server);
  FREE_FTN_CHAR(user);
  FREE_FTN_CHAR(password);
}
		      
		      


#endif
