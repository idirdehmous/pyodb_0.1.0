# -*- coding: utf-8 -*-

class pyodbBinError(Exception):
    """
    ODB needed bin executables 
    """
    pass 
class  pyodbLibError(Exception):
    """
    ODB needed libraries 
    """
    pass 

class  pyodbWarning(Exception):
    """
    Some warnings messages if they exist
    """
    pass 

class  pyodbInterfaceError(Exception):
    """
    C/Python API communication errors 
    """
    pass


class  pyodbDatabaseError(Exception):
    """
    ODB internal structure error 
        corrupted odb 
    """
    pass 

class  pyodbProgrammingError(Exception):
    """
    Programming Error sepcially from the backend side (C language)
    """
    pass 
class  pyodbIntegrityError(Exception):
    """
    ODB data integrity error 
    """
    pass 

class  pyodbDataError(Exception):
    """
    Arrays length , data types  etc  
    """
    pass 
class  pyodbNotSupportedError(Exception):
    """
    Versions and modeules 
    """
    pass 

class pyodbPathError (Exception):
    """
    Checks path and raises Errors !
    """
    pass 
class pyodbEnvError(Exception):
    """
    Raises error if a non environmental odb variables is set !
    """ 
    pass 
class pyodbInternalError(Exception):
    """
    Raises error if a problem is found inside the C code or Python/C communication !
    """
    pass
class pyodbInstallError(Exception):
    """
    Raises an error if something wrong occured during building the modules
    """
    pass 

