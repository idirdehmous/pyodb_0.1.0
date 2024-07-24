# -*- coding: utf-8 -*-
import os, sys  
from ctypes import cdll  
from exceptions   import  *  


class OdbEnv:
      kwlist={"basedir": None}

      OdbVars= { "ODB_IO_METHOD":"" ,
           "ODB_CMA"           :"" ,
           "ODB_STATIC_LINKING":"",
           "ODB_SRCPATH_ECMA"  :"" ,
           "ODB_DATAPATH_ECMA" :"" ,
           "ODB_SRCPATH_CCMA"  :"" ,
           "ODB_DATAPATH_CCMA" :"" ,
           "ODB_IDXPATH_ECMA"  :"" ,
           "IOASSIGN"          :"", 
           "NPES_CCMA"         :"",
           "SWAPP_ODB_IOASSIGN":"",
           "ODB_CCMA_CREATE_POOLMASK":"",
           "ODB_CCMA_POOLMASK_FILE":"",
           "TO_ODB_FULL"       :"",
           "ODB_REPRODUCIBLE_SEQNO":"",
           "ODB_CTX_DEBUG"     :"",
           "ODB_REPRODUCIBLE_SEQNO":"",
           "ODB_CATCH_SIGNALS" :"",
           "ODB_TRACE_PROC"    :"",
           "ODB_TRACE_FILE"    :""    }



      def __init__ (self , basedir , libname):
         
         self.basedir=basedir
         self.libname=libname 
         self.bindir =self.basedir+"/bin"
         self.libdir =self.basedir+"/lib"


      def InitEnv(self ):
         try:
            cdll.LoadLibrary(self.libdir+"/"+self.libname  )  
         except:
            OSError
            raise pyodbLibError("\n--Can't find the shared library libodb.so\n--Please check that it has been installed and added to your python script\n--Or use export ODB_INSTALL_LIB=/path/to/../../libodb.so !") 
        
         syspath=os.environ.copy()
         path=syspath["PATH"]+":"+self.bindir
         odb_bin={
           "PATH"           :path , 
           "ODB_FEBINPATH"  :self.bindir,
           "ODB_SYSPATH"    :self.bindir,
           "ODB_BEBINPATH"  :self.bindir}
   
         # EXPORT 
         for k , v in odb_bin.items(): 
             try:
                os.environ[k] = v
             except:
                raise pyodbPathError("The path ", v ," not found !")                
         return None 
      
      def OdbUpdateEnv(self, EnvVars =None ):
          if EnvVars != None :
             for k , v in EnvVars.items(): 
                 try:
                    os.environ[k] = v
                 except:
                    raise pyodbEnvError ("The ODB env variable{}name not recongnized !".format( k  )  )

             

