-------------------------------------- PROLOGUE ------------------------------------------
This is the version 0.1.0  BETA of pyodb 
An interface written in C/Python API to access the ECMWF ODB1 databases

Its aim is to access the ODB tables/columns data and meta data using a direct SQL request 
embadded in python scripts. 

The main source code is written in pure C and the routines handling the ODB1 
format has been 'pruned' from ECMWF ODB_API bundle-0.18.1 to build only the 
needed libraries 

Reference:
The original C code has been developed by "Sami Saarinen et al" at ECMWF from 1998 --> 2008 

Some modifictions have been done to make it compatible with C/Python API ( 3.8 ---> 3.11 ). 

 FOR THE TERMS OF LICENCE PLEASE REFER TO THE FILE 'LICENCE.txt'  
-------------------------------------------------------------------------------------------


HOW IT WORKS ?  

-For the moment the module source code is on a personnal github account, however during the 
 last meetings of R.D8 Data Assimilation team, it has been decided to porte it into HIRLAM github
 repository. 
 EVERY ONE WHO IS INTERESTED TO FOR MORE DEVELOPEMENT,  IS WELCOME TO FORK IT FROM THE REPOSITORY BELOW :
 https://github.com/idirdehmous/pyodb_0.1.0


INSTALLATION :  

     STEP 0 - clone the code:
              git clone https://github.com/idirdehmous/pyodb_0.1.0
   
     STEP 1 - CREATE A BUILD DIRECORY TO INSTALL THE ODB LIBRARIES FIRST.
             ` mkdir -p  build_odb  `
             ` cd build_odb `
             ` cmake -DCMAKE_INSTALL_PREFIX=/path/to/the/odb/install/dir     ../pyodb_0.1.0 `
             `  make -j ncpu    ( has been tested with a maximum of 4  cpus  ) `
             `  make install  `

`
