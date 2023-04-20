import os, re
import pyspark.dbutils as dbutils

#----------------------------------------------------------------------------------    
def db_path_to_local(path):
    """Function returns a local os file path from dbutils file path
    Parameters
    ----------
    path : str
        DataBricks dbutils file storage path
    Returns
    ----------
    file path: str
        local os file path
    """    
    return re.sub(r'^(dbfs:)', r'/dbfs', path)
#----------------------------------------------------------------------------------    
def to_db_path(path):
    """Function converts a local os file path to a dbutils file path
    Parameters
    ----------
    path : str
        local os file path
    Returns
    ----------
    file path: str
        DataBricks dbutils file storage path
    """        
    return re.sub(r'^(/dbfs)', r'dbfs:', path)
#----------------------------------------------------------------------------------    
def path_exists(path):
    """Function returns Boolean, true if a DataBricks/dbutils file path exists or
    false if it does not. 
    Parameters
    ----------
    path : str
        DataBricks dbutils file storage path
    Returns
    ----------
    Boolean
    """            
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise e      
#----------------------------------------------------------------------------------                    
def list_file_paths(dir_path, ext='csv', path_type='os'):
    """Function lists files of a given extension type within a 
    given DataBricks/dbfs file path. 
    Parameters
    ----------
    dir_path : str
        DataBricks dbutils file storage path
    ext : str
        File extension type to search for
        Default, csv
    path_type str
        Type of file paths to return. 
        Allowed options:
            'dbfs' returns databricks file store paths
            'os' returns local os type paths
    Returns
    ----------
    fps : list
        List of file paths
    """      
    try:
        if not path_exists(dir_path):
            print(f'Directory not found: {dir_path}')
            return []
        if path_type='os:
            fps = [db_path_to_local(f.path) 
                    for f in dbutils.fs.ls(dir_path) 
                    if ((f.path).lower()).endswith(f'.{ext.lower()}')]
        elif path_type='dbfs:
            fps = [f.path 
                    for f in dbutils.fs.ls(dir_path) 
                    if ((f.path).lower()).endswith(f'.{ext.lower()}')]
        print(f'Found {len(fps)} {ext} file(s) within {dir_path}')
        return fps
    except Exception as e:
        raise e
#---------------------------------------------------------------------------------- 
