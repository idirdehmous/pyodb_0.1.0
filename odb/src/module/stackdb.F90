MODULE stackdb

USE PARKIND1  ,ONLY : JPIM     ,JPRB

USE odbshared, only : maxvarlen

implicit none

SAVE
PRIVATE

PUBLIC :: init_stack
PUBLIC :: size_stack
PUBLIC :: push_stack
PUBLIC :: vecpush_stack
PUBLIC :: pop_stack

INTEGER(KIND=JPIM) :: stack_size = 0

TYPE stackdb_t
  character(len=maxvarlen) name
  TYPE(stackdb_t), POINTER :: prev
  TYPE(stackdb_t), POINTER :: next
END TYPE stackdb_t

TYPE(stackdb_t), POINTER :: head
TYPE(stackdb_t), POINTER :: tail

CONTAINS

FUNCTION size_stack() RESULT(isize)
implicit none
INTEGER(KIND=JPIM) :: isize
isize = stack_size
END FUNCTION size_stack


SUBROUTINE init_stack()
implicit none
TYPE(stackdb_t), POINTER :: this, next
if (stack_size > 0) then
  this => head
  do while (associated(this))
    next => this%next
    deallocate(this)
    this => next
  enddo
endif
nullify(head)
nullify(tail)
stack_size = 0
END SUBROUTINE init_stack


SUBROUTINE push_stack(name)
implicit none
character(len=*), intent(in) :: name
TYPE(stackdb_t), POINTER :: this
nullify(this)
if (.not.associated(head)) then
  allocate(head)
  tail => head
  stack_size = 1
else
  this => tail
  allocate(this%next)
  tail => this%next
  stack_size = stack_size + 1
endif
tail%name = name
tail%prev => this
nullify(tail%next)
END SUBROUTINE push_stack


SUBROUTINE vecpush_stack(name, table, k1, k2)
implicit none
character(len=*), intent(in) :: name, table
INTEGER(KIND=JPIM), intent(in) :: k1
INTEGER(KIND=JPIM), intent(in), OPTIONAL :: k2
INTEGER(KIND=JPIM) :: i1, i2
character(len=len(name)+len(table)+20) tmp
INTEGER(KIND=JPIM) j
i1 = k1
if (present(k2)) then
  i2 = k2
else
  i2 = i1
endif
do j=i1,i2
  if (j >= 0 .and. j <= 9) then
    write(tmp,'(a,"_",i1,a)') name, j, table
  else if (j >= 10 .and. j <= 99) then
    write(tmp,'(a,"_",i2,a)') name, j, table
  else
    tmp = '<undefined>'
  endif
  CALL push_stack(tmp)
enddo
END SUBROUTINE vecpush_stack


FUNCTION pop_stack() RESULT(c)
implicit none
character(len=maxvarlen) :: c
TYPE(stackdb_t), POINTER :: this
this => tail
if (associated(this) .and. stack_size > 0) then
  c = this%name
  this => tail%prev
  deallocate(tail)
  tail => this
  stack_size = stack_size - 1
else
  c = ' '
endif
END FUNCTION pop_stack

END MODULE stackdb
