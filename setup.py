#-*-coding utf-8 -*- 
from distutils.core import setup ,Extension
import os  , sys 
import sysconfig
from   Cython.Distutils import build_ext
from   distutils.sysconfig import customize_compiler, get_config_vars
from Cython.Build import cythonize


# IMPORT EXCEPTIONS 
sys.path.insert(0,"./modules"  )
from exceptions import *  


# VERSION 
__version__="0.1.0" 


# COMPILER FLAGS 
extra_compile_args = sysconfig.get_config_var('CFLAGS').split()
extra_compile_args += [ "-fPIC" ,"-Wall", "-Wextra", 
                        "-Wsign-compare","-Waddress",
                        "-Wunused-variable"]
# EXTENSION SUFFIX 
sfx  =  sysconfig.get_config_var('EXT_SUFFIX')

# CWD 
pwd=os.getenv("PWD")

# NAME OF odb lib  &  PATH (Written already by cmake )
odbpath_file="odb_install_dir"
try:
   if os.path.isfile( odbpath_file ):
      print("-"*50)
      print( "--odb_install_dir file FOUND !" )
      print(" "  )
except:
   Exception
   raise pyodbInstallError( "--Cmake failed to find '"+odbpath_file+"' file.\n--Please check the odb installation" )
   sys.exit(0)

try:
   _file_ =  open( odbpath_file  , "r" )
   odb_install_dir = _file_.readline()  
except:
   FileNotFoundError
   raise pyodbInstallError("Problem while reading the odb_install_dir file.\nBuilding the module will FAIL !")  
   sys.exit(0)


# PATHS
odb_env    =odb_install_dir
libname    ="libodb.so"
libpath    =odb_env+"/lib/"+libname

try:
   if os.path.isfile( libpath ):
      print("-"*50)
      print( "--{} FOUND !".format( libpath  )    )
      print(" "  )
except:
     Exception 
     raise pyodbInstallError ("--Path to {} NOT FOUND. Please check the odb installation, or the file ".format( odb_install_dir ) )
     sys.exit(0)

# SOURCE AND INCLUDE 
pyc_src=pwd+"/pycc"
include=pwd+"/include"




# 
class BuildModule:
    def __init__ (self ,name   ):
        self.name = name
        return None 
    def Module ( self , src , include, libs , *args):
        self.src=src 
        self.inc=include 
        self.lib=libs 
        self.language='c'
        self.maj_version=0
        self.min_version=1
        self.patch=0 
        m=Extension( self.name, [ self.src ],
                  include_dirs =[ self.inc ],
                  extra_objects=[ self.lib ], 
                  extra_compile_args=extra_compile_args,
                  language     =self.language )
        return m  


class NoSuffixBuilder(build_ext):
    def get_ext_filename(self, ext_name):
        filename = super().get_ext_filename(ext_name)
        suffix = sysconfig.get_config_var('EXT_SUFFIX')
        ext = os.path.splitext(filename)[1]
        return filename.replace(suffix, "") + ext


# INSTANTIATE  MODULES BY NAME !
m1=BuildModule("pyodb_io")
m2=BuildModule("pyodb_dca")
m3=BuildModule("pyodb_info")
m4=BuildModule("pyodb")


pyodb_io  =m1.Module(  pyc_src+"/io_module.c"     , include , libpath) 
pyodb_dca =m2.Module(  pyc_src+"/dca_module.c"    , include , libpath) 
pyodb_info=m3.Module(  pyc_src+"/info_module.c"   , include , libpath)
pyodb     =m4.Module(  pyc_src+"/pyodb_module.c"  , include , libpath) 

module_list=[ pyodb_io  ,  
              pyodb_dca , 
              pyodb_info, 
              pyodb     ]


# Main pyodb module NAME 
setup( name="pyodb"                              ,
       ext_modules = cythonize( module_list )    ,
       version=__version__                       , 
       description="C/Python interface to access data in ODB1", 
       author="Idir Dehmous"                     , 
       author_email="idehmous@meteo.be"          , 
       cmdclass={"build_ext": NoSuffixBuilder}   ,
       install_requiers=[libpath]                )

# TERMINé !
quit()
