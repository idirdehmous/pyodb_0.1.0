# -*- coding: utf-8 -*- 

# THE pyodb MODULE CAN'T BE IMPORTED DIRECTLY ! IT NEEDS libodb.so AS A DYNAMICAL LIB
# ONE HAS TO LOAD THE SHARED LIBRARY FIRST 

#----------  THIS PROLOGUE HAS TI BE INCLUDED IN EVERY--------------
#----------      PYTHON SCRIPT CALLING THE MODULE     --------------




import os  
from ctypes  import cdll  


# LOOK WHERE THE libodb.so   is INSTALLED ( THE PATH IS ASSUMED TO BE STILL IN THE FILE 
#  odb_install_dir CREATED BY cmake )

odbpath_file="odb_install_dir"
if os.path.isfile( odbpath_file ):
      print("-"*50)
      print( "--odb_install_dir file FOUND !" )
      print(" "  )
else:
     print ( "-- '"+odbpath_file+"' file NOT FOUND.\n--Please check the odb installation" )
     sys.exit(0)

_file_ =  open( odbpath_file  , "r" )
odb_install_dir = _file_.readline()

libname="libodb.so"
libpath="/".join( (odb_install_dir,  "/lib/" , libname ) )
status= cdll.LoadLibrary(   libpath   )
if status is not None:
   print(  "libodb.so successfully loaded " )

# pyodb CAN BE IMPORTED !
try:
    from pyodb_io       import odbConnect , odbClose # OPEN AND CLOSE A CONNECTION
    from pyodb_dca      import pyodbDca              # GENERATE DCA FILES
    from pyodb          import pyodbFetch            # FETCH ROWS FROM ODB USING SQL STATEMENT 
    from pyodb_info     import odbVarno , odbTables  # DISPLAY ODB varno and tables WITH DESCRIPTIONS 
    print( "pyodb has been successfully imported !\nEnjoy "  )
    print ("-"*50)
except:
    ModuleNotFoundError 
    print ("Something went wrong during import" )
    print ("-"*50)



#
quit()


