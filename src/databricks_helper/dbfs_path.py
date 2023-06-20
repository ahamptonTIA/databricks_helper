import os, re

#----------------------------------------------------------------------------------    
def db_path_to_local(path):
    """Function returns a local os file path from dbfs file path
    Parameters
    ----------
    path : str
        DataBricks dbfs file storage path
    Returns
    ----------
    file path: str
        local os file path
    """    
    if path.startswith(r'/mnt'):
        path = f"{r'/dbfs'}{path}"
    return re.sub(r'^(dbfs:)', r'/dbfs', path)
#----------------------------------------------------------------------------------    
def to_dbfs_path(path):
    """Function converts a local os file path to a dbfs file path
    Parameters
    ----------
    path : str
        local os file path
    Returns
    ----------
    file path: str
        DataBricks dbfs file storage path
    """        
    if path.startswith(r'/mnt'):
        path = f"{r'dbfs:'}{path}"        
    return re.sub(r'^(/dbfs)', r'dbfs:', path)   
#----------------------------------------------------------------------------------    
def path_exists(dbutils, path):
    """Function returns Boolean, true if a DataBricks/dbfs file path exists or
    false if it does not. 
    Parameters
    ----------
    dbutils: dbutils object
        DataBricks notebook dbutils object
    path : str
        DataBricks dbfs file storage path
    Returns
    ----------
    Boolean
    """            
    try:
        path = to_dbfs_path(path)
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise e      
#----------------------------------------------------------------------------------                    
def list_file_paths(dbutils, dir_path, ext='csv', path_type='os'):
    """Function lists files of a given extension type within a 
    given DataBricks/dbfs file path. 
    Parameters
    ----------
    dbutils: dbutils object
        DataBricks notebook dbutils object
    dir_path : str
        DataBricks dbfs file storage path
    ext : str
        File extension type to search for
        Default, csv
    path_type str
        Type of file paths to return. 
        Allowed options:
            'dbfs' returns databricks file store paths
            'os' returns local os type paths
            Default, 'os'
    Returns
    ----------
    fps : list
        List of file paths
    """      
    try:
        dir_path = to_dbfs_path(dir_path)
        if not path_exists(dbutils, dir_path):
            print(f'Directory not found: {dir_path}')
            return []
        if path_type =='os':
            fps = [db_path_to_local(f.path) 
                    for f in dbutils.fs.ls(dir_path) 
                    if ((f.path).lower()).endswith(f'.{ext.lower()}')]
        elif path_type =='dbfs':
            fps = [f.path 
                    for f in dbutils.fs.ls(dir_path) 
                    if ((f.path).lower()).endswith(f'.{ext.lower()}')]
        print(f'Found {len(fps)} {ext} file(s) within {dir_path}')
        return fps
    except Exception as e:
        raise e
#---------------------------------------------------------------------------------- 
def list_sub_dirs(dbutils, dir_path, recursive=False):
    """Function lists sub directories of a given 
    DataBricks/dbfs file path. 
    Parameters
    ----------
    dbutils: dbutils object
        DataBricks notebook dbutils object
    dir_path : str
        DataBricks dbfs file storage path
    recursive : Boolean
        Boolean value for recursively list sub directories
        Default, False 
    Returns
    ----------
    sub_dirs : list
        Sorted list of sub directories
    """
    sub_dirs = [p.path for p in dbutils.fs.ls(dir_path) 
                if p.isDir() and p.path != dir_path]
    if recursive:
        for sd in sub_dirs:
            sub_dirs = sub_dirs + list_sub_dirs(dbutils, sd, recursive)
    return sorted(sub_dirs)
#---------------------------------------------------------------------------------- 
