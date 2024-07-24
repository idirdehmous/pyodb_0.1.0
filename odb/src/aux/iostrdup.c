#include "iostuff.h"
#include "cmaio.h"
#include <stdbool.h>

char *
IOstrdup(const char *str, const int *len_str)
{
  char *salloc, *s;
  char *p = NULL;
  const char *invalid_filename = ":invalid_filename:";

  if (len_str) {
    int len = *len_str;
    ALLOC(salloc, len + 1);
    strncpy(salloc,str,len);
    salloc[len] = '\0';
  }
  else {
    salloc = STRDUP(str);
  }

  s = salloc;
  while (*s) { /* Strip off any leading blanks */
    if (*s != ' ') break;
    s++;
  }

  /* Strip off any trailing blanks */
  p = strchr(s,' ');
  if (p) *p = '\0';
  
  if (*s) {
    p = (strlen(s) > 0) ? STRDUP(s) : STRDUP(invalid_filename);
  }
  else {
    p = STRDUP(invalid_filename);
  }

  FREE(salloc);

  return p;
}

char *
IOstrdup_int(const char *str, int binno)
{
  char *p = NULL;
  char *salloc = IOstrdup(str,NULL);
  int len = strlen(salloc) + 20;

  ALLOC(p, len);
  snprintf(p,len,"%s,%d",salloc,binno);
  FREE(salloc);

  return p;
}

char *
IOstrdup_fmt(const char *fmt, int nproc)
{
  char *p = NULL;
  size_t len = strlen(fmt) + 20;

  ALLOC(p, len);
  snprintf(p, len, fmt, nproc);

  return p;
}

char *
IOstrdup_fmt2(const char *fmt, int nproc, char *table_name)
{
  // TODO: deal with case where table_name comes before nproc in format string
  char *p = NULL;
  size_t len = strlen(fmt) + 100; // table names can be large

  ALLOC(p, len);
  snprintf(p, len, fmt, nproc, table_name);

  return p;
}


char *
IOstrdup_fmt3(const char *fmt, char *table_name)
{
  // TODO: deal with case where table_name comes before nproc in format string
  char *p = NULL;
  size_t len = strlen(fmt) + 100; // table names can be large

  ALLOC(p, len);
  snprintf(p, len, fmt, table_name);

  return p;
}


int count_format_specifiers_in_string(char *mystring){
   // count how many format specifiers
    int count=0;
    char *p = strchr(mystring,'%'); /* location of first '%' */
    while (p != NULL) {
       count += 1;
       p = strchr(p+1,'%'); /* location of next '%' */
    }
    return count;
}


/* return a new string with every instance of orig replaced by new_char */
char *replace_char_with(const char *s, char *orig_char, char *new_char) {
    size_t len = strlen(s)+1;
    char *new_string = malloc(len);
    memcpy(new_string, s,len);
    int i;
    for (i=0;i<len;i++){
        if(s[i] == *orig_char) new_string[i]=*new_char;
    }
    return new_string;
}


int calc_local_pool_number(int extracted_nproc, int fromproc, int toproc, int *nproc) {
     int match = 0;
     if((fromproc != 0)&&(toproc != 0)){ // if filled in then use them to check valid range
       if (extracted_nproc>=fromproc && extracted_nproc<=toproc){
            if (nproc) *nproc = extracted_nproc - (fromproc-1);
            match = 1;
       }
     }
     else {
          match = 1;
          if (nproc) *nproc = extracted_nproc;
     }
    return match;
}


int IOstrequ(const char *fmt,
             const char *str,
             int *nproc,
             char *table_name,
             int fromproc,
             int toproc)
{
    // Initialize
    bool is_percent_d = false;
    bool is_percent_s = false;
    bool contains_dot = false;
    int match = 0;
    int i,j,j2;
    int extracted_nproc;
    int index_in_char;
    int index_of_start_of_fmt;
    int nelem;
    char *p;
    
    if (nproc) *nproc = 0;

    if ((p = strchr(fmt,'%')) != NULL) {

        // Remove all delimiter '.' from the strings
        char* a = replace_char_with(fmt,"."," ");
        char* b = replace_char_with(str,"."," ");
        // Does everything up to the first format specifier match?
        p = strchr(a,'%'); /* location of first '%' */
        int len = p-a;
        bool possible_match = (p && (len == 0 || (len > 0 && strnequ(fmt,str,len))));
        bool s_first = false;
        bool d_first = false;


        if (possible_match) {
           
            size_t lena = strlen(a);
            size_t lenb = strlen(b);

            // Count how many format specifiers
            int n_fmt = count_format_specifiers_in_string(a);

            // How long is the string?
            len = strlen(a);

            is_percent_d = false;
            is_percent_s = false;

            // Loop over all format specifiers 
            for (i=1;i<=n_fmt;i++){
                j=1;

                // Loop over characters after the '%' until either a 'd' or an 's' is reached
                contains_dot = false;
                index_of_start_of_fmt = p-a;
                while (j<(len-(p-a))) {
                   index_in_char = p+j-a;

                   // do not allow spaces inside format specifiers
                   if (a[index_in_char] == ' ') {
                        a[index_in_char] = '.';
                   }

                   if (a[index_in_char] == '.') {
                        contains_dot = true;
                   }

                   // If %..d
                   if (a[index_in_char] == 'd') { /* is next character a 'd'  */
                        is_percent_d = true; 
                        if (!s_first) d_first = true;
                        
                        // Special case for %X.Yd format specifiers: %3.3d -> %d
                        if (contains_dot) {
                            a[index_of_start_of_fmt+1]='d';
                            for (j2=2;j2<index_in_char-index_of_start_of_fmt;j2++) {
                                a[index_of_start_of_fmt+j2] = ' ';
                            }
                        }
                        break;
                   }

                   // If %s
                   if( a[index_in_char] == 's') { /* is next character an 's' */
                        is_percent_s = true;
                        if (!d_first) s_first = true;

                        break;
                   }
                   j++;
                }
                p = strchr(p+1,'%'); /* move on to next '%' */
            }

            if (is_percent_d && is_percent_s && s_first) {
                // Extract table name and pool number
                nelem = sscanf(b,a,table_name,&extracted_nproc);
                if (nelem == 2) match = calc_local_pool_number(extracted_nproc, fromproc, toproc, nproc);
            }       
            else if (is_percent_d && is_percent_s && d_first) {
                // Extract table name and pool number
                nelem = sscanf(b,a,&extracted_nproc,table_name);
                if (nelem == 2) match = calc_local_pool_number(extracted_nproc, fromproc, toproc, nproc);
            }       
            else if (is_percent_d && !is_percent_s) {
                // Extract table name and pool number
                nelem = sscanf(b,a,&extracted_nproc);
                if (nelem == 1) match = calc_local_pool_number(extracted_nproc, fromproc, toproc, nproc);
            }
            else if (is_percent_s && !is_percent_d) {
                nelem = sscanf(b,a,table_name);
                if (nelem ==1) match = 1;
            }
        }   // if probable match
        FREE(a);
        FREE(b);
  }
  else {
        match = strequ(fmt, str);
  }
  return match;
}

/* Crack enviroment variables from a given string */

static int 
getit(const char *p, int *step, char **out, const char *target)
{
  char *salloc = STRDUP(p);
  char *s      = salloc;
  int count = 0;
  int Step = 0;
  int finish = 0;
  int curl = ( *s == '{' );
  char *e;
  char *env;

  if (curl) {
    Step++;
    s++;
  }

  if (out) *out = NULL;

  e = s;

  while ( *s && !finish ) {
    if (curl) {
      if (*s == '}') {
   Step++;
   finish = 1;
      }
    }
    else {
      int c = *s;
      if (!isalnum(c) && !(c == '_')) finish = 1;
    }
    if (!finish) {
      Step++;
      s++;
    }
  }

  *s = '\0';

  env = getenv(e);
  if (env) {
    count = strlen(env);
    if (out) *out = STRDUP(env);
  }
  else if (target) {
    int rc = 1;
    cma_get_perror_(&rc);
    if (rc) {
      fprintf(stderr,
         "*** Warning: Cannot resolve the environment variable '%s' in '%s'\n",
         e, target);
    }
  }

  FREE(salloc);

  if (step) *step = Step;

  return count;
}

static char *
crack(const char *s)
{
  char *r = NULL;

  if (strchr(s,'$')) {
    int count = 0;
    char *palloc = STRDUP(s);
    char *p = palloc;
    char *pr;

    /* fprintf(stderr,"(1) s='%s'\n",s); */

    while ( *p ) {
      /* fprintf(stderr,"\t at current *p = '%s'\n",p); */
      if ( *p != '$' ) {
   count++;
   p++;
      }
      else {
   int step;
   count += getit(++p,&step,NULL,s);
   p += step;
      }
    }

    p = palloc;
    ALLOC(r, count + 1);

    pr = r;

    /* fprintf(stderr,"(2) s='%s'\n",s); */
    while ( *p ) {
      /* fprintf(stderr,"\t at current *p = '%s'\n",p); */
      if ( *p != '$' ) {
   *pr++ = *p++;
      }
      else {
   int step;
   char *out;
   count = getit(++p,&step,&out,NULL);
   if (out) {
     strcpy(pr,out);
     pr += count;
     FREE(out);
   }
   p += step;
      }
    }

    *pr = '\0';

    FREE(palloc);
  }
  else {
    r = STRDUP(s);
  }

  return r;
}


char *
IOresolve_env(const char *str)
{
  char *p = NULL;
    
  if (str) {
    if (!strchr(str,'$')) {
      p = STRDUP(str);
    }
    else {
      /* Scan for $'s and resolve (getenv()) them, if possible */
      
      p = crack(str);
    }
  }
    
  return p;
}
