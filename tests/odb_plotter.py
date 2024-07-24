import readline 
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable


import os,sys
import ctypes 
from   ctypes import cdll , CDLL
from   ctypes.util import find_library 
sys.path.insert( 0, "./modules" )
from parser  import ParseString
from environment  import  OdbEnv 
from exceptions import *  


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
  print("  python   odb_plotter.py   dbpath  (ECMA.<obstype>  or CCMA )\n")
  exit(1)


# WHERE libodb.so  is installed  ? 
odb_install_dir=os.getenv( "ODB_INSTALL_DIR" )
if odb_install_dir== None:
   # SET THE PATH EXPLICITY 
   odb_install_dir="/hpcperm/cvah/odb/odb_env"

# INIT ENV 
env= OdbEnv(odb_install_dir, "libodb.so")
env.InitEnv ()

# NOW pyodb modules could be imported !
from pyodb       import   pyodbFetch
from pyodb_io    import   odbConnect , odbClose 

# IN CASE THE DCA FILE HAVE BEEN CREATED pyodb_dca IS NOT NEED !
from pyodb_dca   import   pyodbDca  


p=ParseString ()
db_type = p.getDb ( path=dbpath )[0]  # ECMA or CCMA 
db_name = p.getDb ( path=dbpath )[1]  # ECMA.<obstype> or CCMA 


# SQL QUERY 
sql="select distinct statid, lat, lon ,obsvalue , varno , date , time from hdr,body where varno==39 order by statid"
p.ParseQuery( sql )


iret  = odbConnect ( odbdir=dbpath+"/"+db_name  , mode="r")
dstat = pyodbDca   ( dbpath=dbpath  ,  db=db_name  , ncpu =8 )

if dstat==-1:   
   print("--Failed to create DCA files !" );   
   sys.exit(2)

#sqlfile="sql/ccma_view.sql"
# THE METHOD COULD BE CALLED WITH sql QUERY or  sql file  (ONE IS MANDATORY !)
rows=pyodbFetch( dbpath , sql_query = sql)  #, queryfile=sqlfile   )


# DUMP ROWS 
lats=[]
lons=[]
obs =[]
for row in   rows:
    print (row )
    lats.append(180.*row[1]/3.14)
    lons.append(180.*row[2]/3.14)
    obs.append ( row[3] )

# DOMAIN BOUNDARIES 
if len(lats) != 0: ulat=max(lats)+2 ; llat=min(lats)-2
if len(lons) != 0: ulon=max(lons)+2 ; llon=min(lons)-2

# PLOT 
fig = plt.figure(figsize=(10, 15))
ax  = fig.add_subplot(111,projection=ccrs.Mercator())
ax.autoscale(True)
ax.coastlines()
ax.set_extent([llon, ulon  ,llat ,ulat], crs=ccrs.PlateCarree())
ax.add_feature(cfeature.BORDERS, linewidth=0.5, edgecolor='blue')
ax.gridlines(draw_labels=True)
ax.set_title( "AMDAR temperature ,varno=59 ,CMA type ECMA.synop , datetime = 20211004 1200\nDomain : RC-LACE " )
sc=plt.scatter ( lons, lats ,c=obs , cmap=plt.cm.jet ,marker='o',s=20, zorder =111,transform=ccrs.PlateCarree() )

divider = make_axes_locatable(ax)
ax_cb = divider.new_horizontal(size="5%", pad=0.9, axes_class=plt.Axes)
fig.add_axes(ax_cb)
plt.colorbar(sc, cax=ax_cb)
plt.savefig("ECMA-synop_sample_chmi-lace.pdf" )
plt.show()

