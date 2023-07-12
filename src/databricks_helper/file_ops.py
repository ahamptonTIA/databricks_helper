import os, re, math, uuid
import hashlib
import pandas as pd
from datetime import datetime, timezone
from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count
from pyspark.sql import SparkSession
from databricks_helper import dbfs_path

#----------------------------------------------------------------------------------
def get_spark_session():
    """Function creates a new spark session
    with a unique app id.
    Parameters
    ----------
    None:
    Returns
    ----------
    spark session
    """        
    return SparkSession.builder.appName(uuid.uuid4().hex).getOrCreate()
#----------------------------------------------------------------------------------
def get_byte_units(size_bytes):
    """Function converts bytes into the largest 
    possible unit of measure 
    Parameters
    ----------
    size_bytes: int
        numeric of bytes
    Returns
    ----------
    str :
        String representing the value and largest unit size
        Ex. '200 : GB'
    """
    if size_bytes == 0:
        return '0 : B'
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1000))) #1024
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f'{s} : {size_name[i]}'
#----------------------------------------------------------------------------------
def get_md5_hash(file):
    """Function reads a file to generate an md5 hash.
    See hashlib.md5() for full documentation.
    Parameters
    ----------
    file : str
        String file path
    Returns
    ----------
    str :
        String md5 hash string
    """    
    with open(file, "rb") as f:
        f_hash = hashlib.md5()
        while chunk := f.read(8192):
            f_hash.update(chunk)
    return f_hash.hexdigest() 
#----------------------------------------------------------------------------------
def get_csv_file_details(dbutils, file_path, id_col=None, header=True, spark=None):
    """Function returns a dictionary that details
    general metadata for a csv file
    Parameters
    ----------
    dbutils: dbutils object
        DataBricks notebook dbutils object
    file_path : str
        DataBricks file storage path to a csv
    id_col : str
        Column name for a column that holds an ID or
        set of values to count distinct values of. 
    spark : spark session object
        Default, in not supplied a new session will be built
    Returns
    ----------
    file_meta: dict
        dictionary of file metadata
    """
    if not spark:
        spark = get_spark_session()
        
    # ensure dbfs file path
    file_path = dbfs_path.to_dbfs_path(file_path)
 
    # get the local/os file path
    os_fp = dbfs_path.db_path_to_local(file_path)
    
    # get a dbruntime.dbutils.FileInfo object
    f = dbutils.fs.ls(file_path)[0]

    # get date time file metadata
    statinfo = os.stat(os_fp)
    create_date = datetime.fromtimestamp(statinfo.st_ctime).isoformat()
    modified_date = datetime.fromtimestamp(statinfo.st_mtime).isoformat()

    # read the csv data into a spark dataframe
    sdf = spark.read.csv(f.path, header=header)

       
    # create dictionary to store the metadata
    file_meta = {
                    'file_name': f.name,
                    'file_size_bytes': f'{f.size:,}',
                    'file_size_memory_unit': get_byte_units(int(f.size)),
                    'record_qty': f'{sdf.count():,}',   
                    'column_qty': f'{len(sdf.columns):,}',   
                    'file_md5_hash': get_md5_hash(os_fp),
                    'created' : create_date,
                    'modified' : modified_date
                }    

    if bool(id_col) and header:
        file_meta[f'{id_col}_qty'] = f'{sdf.select(id_col).distinct().count():,}'
    return file_meta 
#---------------------------------------------------------------------------------- 
def get_csv_file_details_mcp(dbutils, files, id_col, n_cores=None, spark=None):   
    """Function uses multi-core processing to call regex_file_pattern_sub.
    See regex_file_pattern_sub documentation.
    Parameters
    ----------
     dbutils: dbutils object
        DataBricks notebook dbutils object   
    files : list
        List of string csv file paths
    id_col : str
        Column name for a column that holds an ID or
        set of values to count distinct values of. 
    n_cores : int
        Number of cores to use.  Will not exceed 85% of 
        available cores.
        Default, < 85% of the total cores available      
    spark : spark session object
        Default, in not supplied a new session will be built
    Returns
    ----------
    list of dictionaries
        Dictionary where the key is the file path and the value
        is a nested dictionary of the match patterns, substituted 
        value, and a count of matches.
        The dictionary can be used to track file modifications. 
        [{file_path1: {pattern : substitution, count}}]
    """     

    max_cores = math.floor(cpu_count() * 0.85)
    if not n_cores:
        n_cores = max_cores
    elif n_cores > max_cores:
        n_cores = max_cores

    print(f"Using {n_cores} cores's of {cpu_count()}")
    pool = ThreadPool(n_cores)

    if not spark:
        task_params = [(dbutils, f, id_col, get_spark_session()) for f in files]
    else:
        task_params = [(dbutils, f, id_col, spark) for f in files]
    try:
        result = pool.starmap(get_csv_file_details, task_params)
        pool.close()
        pool.join()
    except Exception as e:
        print(f'Error: {e}')
        pool.terminate()
    return result
#---------------------------------------------------------------------------------- 
def regex_file_pattern_sub(file, sub_dict):
    """Function searches a file for regex string pattern matches
    and replaces/substitutes with new values provided. 
    Parameters
    ----------
    file : str
        String file path
    sub_dict : dict
        Dictionary of regex key, value pairs where the key
        is the pattern to match and the value is string to
        replace the matched pattern.
    Returns
    ----------
    dict
        Dictionary where the key is the file path and the value
        is a nested dictionary of the match patterns, substituted 
        value, and a count of matches.
        The dictionary can be used to track file modifications. 
        {file_path: {pattern : {substitution : x , count: y}}
    """    
    matched = []
    for k,v in sub_dict.items():
        # count the number of records/lines with a match
        l_cnt = 0
        
        with open(file, 'r+') as f:
            filedata = f.read()
            matches = re.findall(k, filedata, flags=re.M)   
            if matches:
                l_cnt = len(matches)
                
            # if match found in file, replace with substring
            if l_cnt > 1:
                matched.append({k : {'substitution':v, 'count' : l_cnt}})
                print(f'\t\t-Found {l_cnt} patterns matching {k} in {file}')

                #Only modify strings in files that match the replace pattern(s)
                print(f'\t\t\t-Replacing ({k}) with ({v}) in {file}\n')

                # Replace the target pattern
                text = re.sub(k, v, filedata, flags=re.M)

                f.seek(0)
                f.truncate()
                f.write(text)
                

    return {file : matched}
#---------------------------------------------------------------------------------- 
def regex_file_pattern_sub_mcp(files, sub_dict, n_cores=None):   
    """Function uses multi-core processing to call regex_file_pattern_sub.
    See regex_file_pattern_sub documentation.
    Parameters
    ----------
    files : list
        List of string file paths
    sub_dict : dict
        Dictionary of regex key, value pairs where the key
        is the pattern to match and the value is string to
        replace the matched pattern.
    n_cores : int
        Number of cores to use.  Will not exceed 85% of 
        available cores.
        Default, < 85% of the total cores available
    Returns
    ----------
    list of dictionaries
        list of dictionaries where the key is the file path and the value
        is a nested dictionary of the match patterns, substituted 
        value, and a count of matches.
        The dictionary can be used to track file modifications. 
        [{file_path: {pattern : {substitution : x , count: y}}]
    """     
    max_cores = math.floor(cpu_count() * 0.85)
    if not n_cores:
        n_cores = max_cores
    elif n_cores > max_cores:
        n_cores = max_cores

    print(f"Using {n_cores} cores's of {cpu_count()}")
    pool = ThreadPool(n_cores)
    
    task_params = [(f, sub_dict) for f in files]
    try:
        result = pool.starmap(regex_file_pattern_sub, task_params)
        pool.close()
        pool.join()
    except Exception as e:
        print(f'Error: {e}')
        pool.terminate()
    return result
#---------------------------------------------------------------------------------- 
def export_dataframe(in_df, out_dir, out_name, file_type='csv'):
    """
    Export a Pandas DataFrame to a single file in the specified 
    file format. This is an alternative to the spark folder and
    split multi-file structure.  The aim is to make the files
    easier to work with outside of databricks. 
    
    Parameters
    ----------
    in_df : Pandas DataFrame
        Pandas DataFrame to export.
    out_dir : str, optional
        Output directory path 
    out_name : str
        Output file name
    file_type : str
        Output file type name
        Allowed file types: 
                          'csv', 'json', 'html', 
                          'pickle', 'pkl', 'parquet'  
    Returns
    -------
    file_path : str
        Full file path of the exported file

    Raises
    ------
    ValueError
    If the file type is invalid.

    """

    if not isinstance(in_df, pd.DataFrame):
        # convert the spark dataframe to a pandas dataframe
        df = in_df.toPandas()
    else:
        df = in_df

    # get the api/os file path
    out_dir = dbfs_path.db_path_to_local(out_dir)

    # remove all extra file extensions 
    if out_name.endswith(file_type):
        out_name = ''.join([x for x in out_name.split(file_type) if bool(x)])

    # set the file path
    out_name = f'{out_name}.{file_type}'
    file_path = os.path.join(out_dir, out_name)

    if file_type == "csv":
        df.to_csv(file_path, index=False, chunksize=250000)
    elif file_type == "json":
        df.to_json(file_path)
    elif file_type == "html":
        df.to_html(file_path, index=False)
    elif file_type == "pickle" or file_type == "pkl" :
        df.to_pickle(file_path)
    elif file_type == "parquet":
        df.to_parquet(file_path, index=False)
    else:
        raise ValueError("Invalid file type: " + file_type)

    return file_path
#---------------------------------------------------------------------------------- 
def sql_query_to_file(spark, sql_str, out_dir, out_name, file_type='csv'):
    """Takes a Spark SQL query and exports the results 
    to a single file ('csv', 'json', 'html', 
                          'pickle', 'pkl', or 'parquet'). 
    Use this method when the outputs are expected
    to be relatively small summary tables.  Otherwise, use 
    the native spark methods. 
    Parameters
    ----------
    sql_str: str
        Spark SQL query string
    out_dir : str
        DataBricks folder/directory path 
    out_name : str
        output name of the file
    file_type : str
        Output file type name
        Allowed file types: 
                          'csv', 'json', 'html', 
                          'pickle', 'pkl', 'parquet'  
    Returns
    ----------
    out_file: str
        string representing the output file path
    """

    # create a spark dataframe of the results given an sql query
    s_df = spark.sql(sql_str)

    # export the results
    out_file = export_dataframe(in_df=s_df, 
                                out_dir=out_dir, 
                                out_name=out_name, 
                                file_type=file_type)
    return out_file
#---------------------------------------------------------------------------------- 
def upsert_csv(df, output_path, upsert_columns):
    """
    Upsert a Pandas DataFrame to a CSV file.

    Parameters
    ----------
    df : Pandas DataFrame
        The DataFrame to upsert.
    output_path : str
        The path to the CSV file to upsert to.
    upsert_columns : list of str
        The columns to use for upserting.

    Returns
    -------
    output_path : str
        The output file path
    """
    if not output_path.endswith('.csv'):
        output_path = output_path + '.csv'
    # Check if the CSV file exists
    if not os.path.exists(output_path):
        # Write the DataFrame to the CSV file
        df.to_csv(output_path, index=False)
    else:
        # Read the CSV file into a DataFrame
        existing_df = pd.read_csv(output_path)

        # Append the DataFrames
        existing_df = existing_df.append(df, ignore_index=True)

        # Drop duplicates based on the upsert columns
        existing_df.drop_duplicates(subset=upsert_columns)

        # Write the merged DataFrame to the CSV file
        existing_df.to_csv(output_path, index=False)
    return(output_path)
#---------------------------------------------------------------------------------- 
