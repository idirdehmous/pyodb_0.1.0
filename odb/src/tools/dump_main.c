#include <stdio.h>

//#include "odb_mod.c"


int main(){

char *database ="CCMA"; 
char *sql_query="select statid, lat , lon from hdr, body";  //= "select statid, lat,lon , obstype,codetype , varno, fg_depar, from hdr,body";

//pyodb ( database) ; //, sql_query ) ; 

return 0 ; 
}
