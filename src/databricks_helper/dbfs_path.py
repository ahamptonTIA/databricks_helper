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
    # identify and clean malformed volume paths
    if 'Volumes' in path: # unity catalog volume identifier
        # remove legacy prefixes if path is not a valid dbfs directory
        path = re.sub(r'^(dbfs:/|/dbfs/|dbfs:)', '/', path)
        if path.startswith('/Volumes'):
            return path

    # convert legacy mount points
    if path.startswith(r'/mnt'):
        path = f"{r'/dbfs'}{path}" # local mount prefix
    
    # replace dbfs protocol with local root
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
    # sanitize unity catalog volume paths
    if 'Volumes' in path: # unity catalog volume identifier
        # ensure volumes do not contain dbfs protocol prefixes
        return re.sub(r'^(dbfs:/|/dbfs/|dbfs:)', '/', path)

    # prefix legacy mount points with protocol
    if path.startswith(r'/mnt'):
        path = f"{r'dbfs:'}{path}" 
        
    # convert local dbfs root to protocol format
    if not path.startswith(r'/Volumes') and path.startswith(r'/dbfs'):
        path = re.sub(r'^(/dbfs)', r'dbfs:', path)         
    return path 

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
        # standardize input path format
        path = to_dbfs_path(path)
        
        # validate volumes using native os module
        if path.startswith('/Volumes'):
            return os.path.exists(path)
            
        # validate legacy paths using filesystem utilities
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        # handle specific file not found exceptions for both uc and dbfs
        err_msg = str(e) # string representation of java/python error
        if 'FileNotFoundException' in err_msg or 'No such file' in err_msg:
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
        # verify directory existence
        dir_path = to_dbfs_path(dir_path)
        if not path_exists(dbutils, dir_path):
            print(f'Directory not found: {dir_path}')
            return []
            
        # list files via local os scan for volumes
        if dir_path.startswith('/Volumes'):
            fps = [f.path # local file path
                    for f in os.scandir(db_path_to_local(dir_path))
                    if ((f.path).lower()).endswith(f'.{ext.lower()}')]            
        
        # list files via filesystem utilities for legacy paths
        elif path_type =='os':
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
def list_sub_dirs(dbutils, dir_path, recursive=False, ignore=['.parquet']):
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
    ignore : list
        List of file types that are actaully folders to ignore
        Default, ['.parquet']
    Returns
    ----------
    sub_dirs : list
        Sorted list of sub directories
    """
    # standardize path format
    dir_path = to_dbfs_path(dir_path)
    
    # process subdirectories via os module for volumes
    if dir_path.startswith('/Volumes'):
        local_dir = db_path_to_local(dir_path) # native mount path
        sub_dirs = []
        for entry in os.scandir(local_dir):
            if entry.is_dir():
                volume_path = to_dbfs_path(entry.path) 
                if not os.path.abspath(volume_path).lower().endswith(tuple(ignore)):
                    sub_dirs.append(volume_path)
    
    # process subdirectories via filesystem utilities for legacy dbfs
    else:
        sub_dirs = [p.path # dbfs protocol path
                    for p in dbutils.fs.ls(dir_path) 
                    if p.isDir() and p.path != dir_path and
                    not os.path.abspath(p.path).lower().endswith(tuple(ignore))]
    
    # perform recursive search if requested
    if recursive:
        for sd in sub_dirs:
            sub_dirs = sub_dirs + list_sub_dirs(dbutils, sd, recursive, ignore)
            
    return sorted(sub_dirs)

#---------------------------------------------------------------------------------- 
def create_dir(dbutils, out_dir):
    """Function creates a directory if it does not exist. 
    Parameters
    ----------
    out_dir: str
        DataBricks dbfs file storage path or volume path
    Returns
    ----------
    out_dir : Boolean
        True if the directory was created or exists
    """ 
    # normalize directory path
    out_dir = to_dbfs_path(out_dir)
    
    try: 
        # ensure directory exists or create it
        if not path_exists(dbutils, out_dir):
            # use native directory creation for volumes
            if out_dir.startswith('/Volumes'):
                os.makedirs(out_dir, exist_ok=True)
            else:
                # use filesystem utilities for legacy dbfs
                dbutils.fs.mkdirs(out_dir)
            print(f'Created directory: {out_dir}')
        else:
            print(f'Directory already exists: {out_dir}')
        return True
    except Exception as e:
        # log failure details
        print(f"Failed to create directory: {e}")
        return False