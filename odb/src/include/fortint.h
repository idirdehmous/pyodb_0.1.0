#ifndef FORTINT_H
#define FORTINT_H

#ifdef INTEGER_IS_INT
#define fortint int
#define JPointer int *
#else
#if defined hpR64 || defined hpiaR64
#define fortint long long
#define JPointer long long *
#else
#define fortint long
#define JPointer long *
#endif
#endif

#ifdef REAL_8
#define fortreal double
#else
#define fortreal float
#endif

#define fortdouble double

#endif /* End of FORTINT_H */
