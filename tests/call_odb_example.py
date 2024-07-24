# -*- coding: utf-8 -*-
import os
import sys
import ctypes 
from   ctypes import cdll , CDLL
from   ctypes.util import find_library 
sys.path.insert( 0, "./modules" )
import argparse 
from parser  import ParseString
from environment  import  OdbEnv 
from exceptions import *  


# WHERE libodb.so  is installed  ? 
basedir="/home/micro/Bureau/odb/odb_env"

# GET ARGS from COMMAND LINE 
nargv = len(sys.argv)
if nargv > 1 :
  dbpath   = sys.argv[1]
  if not os.path.exists(dbpath) :
    
      print("--Odb " + dbpath + " not found.")
      exit(1)
else :
  print("--You need to provide the odb path !\n")
  print("--Usage:")
  print("  python   call_odb_example.py   dbpath  (ECMA.<obstype>  or CCMA )\n")
  exit(1)


# INIT ENV 
env= OdbEnv(basedir, "libodb.so")
env.InitEnv ()

# --> NOW pyodb could be imported  !
from pyodb    import  pyodbFetch
from pyodb_io import  odbConnect , odbClose 
from pyodb_dca import pyodbDca  

p=ParseString ()
db_type = p.getDb ( path=dbpath )[0]
db_name = p.getDb ( path=dbpath )[1]


# SQL QUERY 
sql="select  statid, varno , lat, lon ,obsvalue , fg_depar from hdr,body"
p.ParseQuery( sql )

#sqlfile="ccma_view.sql"

iret  = odbConnect ( odbdir=dbpath+"/"+db_name  , mode="r")
dstat = pyodbDca   ( dbpath=dbpath  ,  db=db_name  , ncpu =8 )

if dstat==-1:   
   print("--Failed to create DCA files !" );   
   sys.exit(2)

#sqlfile="sql/ccma_view.sql"
# THE METHOD COULD BE CALLED WITH sql QUERY or  sql file  (ONE IS MANDATORY !)
rows=pyodbFetch( path=dbpath , sql_query = sql)  #, queryfile=sqlfile   )
print("Number of rows fetch from ODB ", db_name  ,  len(rows ))   
#for row in   rows:
    #print( row )

# Close the interface !
odbClose( iret)

