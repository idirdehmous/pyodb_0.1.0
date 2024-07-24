#ifndef SIZE_ROUTINES_H
#define SIZE_ROUTINES_H

static fortint prodsize(fortint, char *, fortint, fortint *,
                        fortint (*fileRead)(), void *);
static fortint gribsize(char * , fortint, fortint * , 
                        fortint (*fileRead)(), void * );
static fortint bufrsize(char * , fortint, fortint * , 
                        fortint (*fileRead)(), void * );
static fortint tide_budg_size(char *, fortint, fortint *,
                        fortint (*fileRead)(), void *);
static fortint lentotal(char *, fortint *, fortint, fortint , fortint ,
                        fortint , fortint (*fileRead)(), void *);
static fortint waveLength(char *);

static fortint crex_size( void * );

#endif /* end of  SIZE_ROUTINES_H */
