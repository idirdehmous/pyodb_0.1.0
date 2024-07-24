#ifdef RS6K

!-- Thanks to John Hague (IBM) and Deborah Salmond (ECMWF) -- 2002
!   real --> real*8 by Sami Saarinen, ECMWF, 16-Jan-2004

      subroutine jdsort(N, istride, iptr, c, index)
!
!**C* *JDSORT* - interface to IBM essl dsorts
!
!**C  INTERFACE.
!     ----------
!
!          *CALL* *JDSORT(N,ISTRIDE, IPTR, C, INDEX)
!           N = number of elements to be sorted
!           ISTRIDE = stride
!           IPTR = start address
!           C = array to be sorted
!           INDEX = index into array
!
      implicit none
      integer N,istride, iptr
      real*8 c(*)
      integer index(N)

! local variables

      integer i,j,ixx(N),ix(N)
      real*8 cc(N)
      real*8 w(N/2+1)

      do i=1,N
        j=(i-1)*istride+1
        cc(i)=c(iptr+(index(i)-1)*istride)
        ixx(i)=index(i)
      enddo
      call dsorts(cc,1,N,ix,w,size(w))
      do i=1,N
        index(i)=ixx(ix(i))
      enddo
      return
      end


      subroutine jisort(N, istride, iptr, c, index)
!
!**** *JISORT* - interface to IBM essl isorts
!
!**   INTERFACE.
!     ----------
!
!          *CALL* *JISORT(N,ISTRIDE, IPTR, C, INDEX)
!           N = number of elements to be sorted
!           ISTRIDE = stride
!           IPTR = start address
!           C = array to be sorted
!           INDEX = index into array
!
      implicit none
      integer N,istride, iptr
      integer c(*),index(N)

! local variables

      integer cc(N)
      integer i,j,ixx(N),ix(N)
      integer w(N/2+1)

      do i=1,N
        j=(i-1)*istride+1
        cc(i)=c(iptr+(index(i)-1)*istride)
        ixx(i)=index(i)
      enddo
      call isorts(cc,1,N,ix,w,size(w))
      do i=1,N
        index(i)=ixx(ix(i))
      enddo
      return
      end
#else
!-- other machines --
      subroutine jsort_dummy()
      end 
#endif

