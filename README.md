## pyodb_0.1.0

## Description & prologue 
This is the BETA version 0.1.0 of pyodb.<br />
An interface written in C/Python API to access the ECMWF ODB1 databases.<br />

Its aim is to access the ODB tables/columns data and meta data using a direct SQL request
embadded in python scripts.<br />

The main source code is written in pure C and the routines handling the ODB1
format has been 'pruned' from ECMWF ODB_API bundle-0.18.1 to build only the
needed libraries <br />

Reference:
The original C code has been developed by "Sami Saarinen et al" at ECMWF from 1998 --> 2008.  <br />
Some modifictions have been done to make it compatible with C/Python API ( 3.9 ---> 3.11 ).

## How it works ?

## Dependencies :
        gcc compiler >= 8.4.0    
        cmake        >= 3.15.0   
        python       >= 3.10.0 
        BISON        >= 3.0.4 
        FLEX         >= 2.6.0 

==> It has been widely developed and tested on ATOS. <br />
So on ATOS  : <br />
   module load   gcc/8.5.0            <br />
   module load   python3/3.10.10-01   <br />
   module load   cmake/3.25.2         <br />


## installation :  

   ```  
        STEP 0 - clone the code:
               git clone https://github.com/idirdehmous/pyodb_0.1.0 
   
        STEP 1 - FIRST, CREATE A BUILD DIRECORY TO INSTALL THE ODB LIBRARIES.
               mkdir -p  build_odb  
               cd build_odb 
               cmake -DCMAKE_INSTALL_PREFIX=/path/to/the/odb/install/dir     ../pyodb_0.1.0 
               make -j ncpu    ( has been tested with a maximum of 4  cpus  ) 
               make install  
```
REMARK:  <br />
-By the end the configuration, a file called 'odb_install_dir' will be created by cmake for the next step<br />

-Once the odb binaries , include and libs are installed, the second step consist on building and installing the python module itself<br />

```
        STEP 2 - cd  /path/to/source/of/.../pyodb_0.1.0  
              python  setup.py   build  
              *Standard installation 
                sudo  python  setup.py   install  

              *Local directory installation 
                 python setup.py   install  --prefix=/../../../your/pythonlibs 
In this case add the path to PYTHONPATH env variable :
export  PYTHONPATH=$PYTHONPATH:/somewhere/pylibs/lib/python3.x/site-packages/pyodb-0.1.0-py3.x-linux-x86_64.egg
```

## Testing 
-When a C/Python API extension is imported, the python statement 'import' initialises some functions involved during the compilation (headers,  libraries etc ). One has then, firstly to load the shared objects using 'ctypes' module. <br /> 
-The pyodb installation is tested as follow : 
```
from ctypes import  cdll  
cdll.LoadLibrary(  "/path/to/odb/install dir/../../libodb.so"  )

#-If the shared library ( libodb.so) is loaded successfully then !

import pyodb
print( pyodb.__doc__) 
```
-For a complete import test script, please see 'tests/test_import.py'  

## Epilogue 
-If everything went well,one can use the example scripts under 'pyodb_0.1.0/tests' and reading the ODB(s) samples included in 'pyodb_0.1.0/odb_samples'. <br />


General info        <br />
Used languages      : C / python /Fortran  <br />
Needs installation  : YES                  <br />
Needs compilation   : YES                  <br />
Tested with ODB(s)  : RMI ,CHMZ , MetCoOp and CHMI  <br />
Handled observations: Conventional, GNSS , Radar  & Sat Radiances. <br />

Some limitations    <br />
Supports  formula   SQL statement    : NOT YET   ( i.e  SELECT degrees(lat  ), degrees(lon)... ) <br />
	  OpenMP                     : NOT YET    <br />
	  Read/Write to ECMA,CCMA    : NO        ( READ ONLY ) <br />
	  conversion to ODB2         : NOT YET   ( On going  ) <br />
	  conversion to MySQL,SQLite : NOT YET   ( On going  ) <br />


@__DATE              :  2024.07.23    <br />
@__INSTITUTE         :  RMI ( Royale Meteorological Institute )   <br />
@__AUTHOR            :  Idir DEHMOUS    <br />
@__LAST_MODIFICATION :  None.      <br />
