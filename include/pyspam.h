#define PY_SSIZE_T_CLEAN
#define mdi 2147483647
#include  <stdio.h> 	
#include  "Python.h"
#include  "odbdump.h"

#define SMAX 50 



/*extern PyObject* pyodb_Error;
extern PyObject* pyodb_Warning;
extern PyObject* pyodb_InterfaceError;
extern PyObject* pyodb_DatabaseError;
extern PyObject* pyodb_InternalError;
extern PyObject* pyodb_OperationalError;
extern PyObject* pyodb_ProgrammingError;
extern PyObject* pyidb_IntegrityError;
extern PyObject* pyodb_DataError;
extern PyObject* pyodb_NotSupportedError;*/




/*extern  int  IOstat  (  int id )   {
typedef unsigned int UINT     ;
struct IOstatus  {
      UINT  ODB_OK    ;
      UINT  ODB_ERROR ;
   };
//typedef struct IOstatus {
//   ODB_OK    = 0 ; 
//   ODB_ERROR = 1
//   ODB_BUSY  = 5
//   ODB_ROW   = 100
//   ODB_DONE  = 101
//   ODB_METADATA_CHANGED = 102
//   ODB_STATIC = ctypes.c_void_p(0)
//} status  ;
typedef struct IOstatus  io  ; 
return  0  ;
}*/



/*static PyObject* BuildString ( PyObject*  d    ){
*                long unsigned int js  ;
                char cc[sizeof(double)+1];
                char *scc = cc ;
                union {
                  char s[sizeof(double)] ;
                  //double d;
                } u;

                u.d = d;

                for (js=0; js<sizeof(double); js++) {
		printf( "------>  %d\n"  , js)   ; 
                  char c = u.s[js];

               *scc++ = isprint(c) ? c : '8' ; 
                } 
                *scc = '\0';
//return  cc  ; 
}*/


/*static int CheckMdi ( Bool print_mdi , double d , uint pci  ) {
if (print_mdi  &&  pci != DATATYPE_STRING  && ABS( d ) == mdi ) {
return 0   ;  
} else {
return 1 ; 
  }
}   to be used later ! */ 



// Formatting float 
double  format(double value , char *fmt ) {
static  char str [SMAX ] ;
double    x  ;
sprintf(str, fmt , value );
x = atof(str );
return   x ;

}

char* concat(const char *s1, const char *s2)
{
    char *result = malloc(strlen(s1) + strlen(s2) + 1); // +1 for the null-terminator
    strcpy(result, s1);
    strcat(result, s2);
    return result;
}


void tostring(char [], int);
void tostring(char str[], int num)
{
    int i, rem, len = 0, n;
 
    n = num;
    while (n != 0)
    {
        len++;
        n /= 10;
    }
    for (i = 0; i < len; i++)
    {
        rem = num % 10;
        num = num / 10;
        str[len - (i + 1)] = rem + '0';
    }
    str[len] = '\0';
}


extern  int get_strlen( const char*    string  ) ;


int get_strlen (  const char *  string  ) {
int str_len  ;

if ( string  ) {
str_len=  strlen( string );
} else  {
str_len=0   ;
};

return str_len ;
}





// Update lists items through pointers 
//static PyObject*  list_reset(PyObject* *list_col , Py_ssize_t ncols )
//{    *list_col = PyList_New (ncols) ;
//     return *list_col ;
//}

//static PyObject* reset_counter  (PyObject*  nrows ) {
//    PyObject* n ;
//    return PyLong_FromLong(n) ; 
//}
