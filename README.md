## pyodb_0.1.0

## Descrption & prologue 
This is the version 0.1.0  BETA of pyodb.
An interface written in C/Python API to access the ECMWF ODB1 databases

Its aim is to access the ODB tables/columns data and meta data using a direct SQL request
embadded in python scripts.

The main source code is written in pure C and the routines handling the ODB1
format has been 'pruned' from ECMWF ODB_API bundle-0.18.1 to build only the
needed libraries

Reference:
The original C code has been developed by "Sami Saarinen et al" at ECMWF from 1998 --> 2008
Some modifictions have been done to make it compatible with C/Python API ( 3.8 ---> 3.11 ).

## How it works ?


## installation :  

   ```  
        STEP 0 - clone the code:
               git clone https://github.com/idirdehmous/pyodb_0.1.0 
   
        STEP 1 - CREATE A BUILD DIRECORY TO INSTALL THE ODB LIBRARIES FIRST.
               mkdir -p  build_odb  
               cd build_odb 
               cmake -DCMAKE_INSTALL_PREFIX=/path/to/the/odb/install/dir     ../pyodb_0.1.0 
               make -j ncpu    ( has been tested with a maximum of 4  cpus  ) 
               make install  
```
REMARK:
-By the end the configuration, a file called 'odb_install_dir' will be created by cmake for the next steps<br />

-Once the odb binaries , include and libs are installed, The second step consist in builing and installing the python module itself<br />

```
        STEP 2 - cd  /path/to/source/of/.../pyodb_0.1.0  
              python  setup.py   build  
              *Standard installation 
                sudo  python  setup.py   install  

              *Local directory installation 
                 python setup.py   install  --prefix=/../../../your/pythonlibs 
```

## Testing 
-When a module build from C/Python API is imported, the python statement 'import' initialises some functions involved during the compilation (headers,  libraries etc ). One has then firstly to load the shared objects using 'ctypes module'.<br /> 
-The pyodb installation is tested as follow : 
```
from ctypes import  cdll  
cdll.LoadLibrary(  "/path/to/odb/libs/libodb.so"  )

-If the shared library ( libodb.so) is loaded successfully then 
```
import pyodb
print( pyodb.__doc__) 

-For a complete import test script please see tests/test_import.py  


## Epilogue 
-If everything went well,one can use the exampls scripts under  pyodb_0.1.0/tests and read the ODB(s) samples included in pyodb_0.1.0/odb_samples


