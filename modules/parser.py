import os  , sys 
import re

from exceptions import *



odb_func=["degrees","cos", 
          "sin" , "sqrt","arcsin", 
          "arccos", "tan","cotan", "log", "exp", "speed"]

class ParseString:
      options={"find" :None ,
             "match":None , 
             "path" :None ,
             "dbname":"CCMA" }
      def __init__(self):
          return None

      def CheckPattern ( self,string ,  pattern , **kwargs):
          found=None 
          self.string = string 
          self.pattern= pattern 
          self.find  = kwargs["find"]
          self.match = kwargs["match"]
          self.path  = kwargs["path"]
          self.dbname= kwargs["dbname"]
          if self.find ==re.findall ( self.pattern , self.string    ):
              return True

      def getDb (self ,  **kwarg  ):
          self.string  =kwarg["path"]
          
          if re.search  ( "ECMA" , self.string   ):
             dbtype = os.path.basename(os.path.normpath(self.string) )[0:4]
             dbname = os.path.basename(os.path.normpath(self.string) )
             return dbtype ,dbname 
          elif re.search ( "CCMA" , self.string   ):
             dbtype=os.path.basename(os.path.normpath(self.string) )[0:4]
             dbname=os.path.basename(os.path.normpath(self.string) )
             return  dbtype, dbname 
          else:
             print( "ECMA or CCMA not found in  path: " , self.string)
             print( "If ECMA.obstype or CCMA is changed the file ECMA.sch or CCMA.sch should be changed!")
             sys.exit(0)
      def ParseQuery( self, query):
          for f in odb_func:
              found=re.findall ( f  , query ,flags=re.IGNORECASE)
              if len(found) > 0:
                 print( "--WARNING  :\n--The sql request may contain a function" )
                 print( "--This can create empty columns with NULL pointers")     
                 print( "--Functions in sql request from python not supported yet!\n")

          return None 
                 
