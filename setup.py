from distutils.core import setup, Extension
import os  , sys 
import sysconfig
from   Cython.Distutils import build_ext
from   distutils.sysconfig import customize_compiler, get_config_vars



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

# NAME OF odb lib  & PATH (Written already by cmake )
odbpath_file="odb_install_dir"
if os.path.isfile( odbpath_file ):
   print( "--odb_install_dir file found !" )
else:
   print( "--Cmake failed to create the odb_install_dir file. Please check the odb installation !" )
   sys.exit(0)

_file_ =  open( odbpath_file  , "r" )
odb_install_dir = _file_.readline()  
   

odb_env    =odb_install_dir
libname    ="libodb.so"
libpath    =odb_env+"/lib/"+libname

if os.path.isfile( libpath ):
   print( "--{} found !".format( libpath  )    )
else:
   print( "--Path to {} not found. Please check the odb installation, or the file ".format( odb_install_dir ) )
   sys.exit(0)

# SOURCE AND INCLUDE 
pyc_src=pwd+"/pycc"
include=pwd+"/include"


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


pyodb_io  =m1.Module(  pyc_src+"/io_module.c"   , include , libpath) 
pyodb_dca =m2.Module(  pyc_src+"/dca_module.c"  , include , libpath) 
pyodb_info=m3.Module(  pyc_src+"/info_module.c"  , include ,libpath)
pyodb     =m4.Module(  pyc_src+"/pyodb_module.c" , include ,libpath) 

# Main pyodb module NAME 
setup( name="pyodb",version=__version__, description="C/Python interface to access data in ODB1", author="Idir Dehmous", author_email="idehmous@meteo.be", ext_modules=[ pyodb_dca ,  pyodb , pyodb_io, pyodb_info ], cmdclass={"build_ext": NoSuffixBuilder} ,install_requiers=[libpath] )


