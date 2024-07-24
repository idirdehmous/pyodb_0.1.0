#ifndef BUFRGRIB_H
#define BUFRGRIB_H
/*
  bufrgrib.h
*/
#include <stdio.h>
#include <string.h>

/*	defines for BUFR functions */ 
#define ARRSIZE 100
#define TRUE 1
#define FALSE 0
#define BOOL int
#define BOOLEAN int

#ifdef VAX
#define off_t char *
#include <types.h>
#include <file.h>
#endif

#define BUFSIZE 200
#endif /* end of  BUFRGRIB_H */
