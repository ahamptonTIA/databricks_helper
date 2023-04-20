import os, re, math

from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count

#----------------------------------------------------------------------------------
def convert_size(size_bytes):
    if size_bytes == 0:
        return '0 : B'
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f'{s} : {size_name[i]}'
#----------------------------------------------------------------------------------
def get_file_details(dir_path, id_col):
    """Function returns a pyspark sql dataframe that details
    the file name, size (bytes), total record counts, and 
    a count by unique values in a field. 

    Parameters
    ----------
    dir_path : str
        DataBricks file storage path

    id_col : str
        Column name for a column that holds an ID or
        set of values to count distinct values of. 

    Returns
    ----------
    df: pyspark.sql.dataframe.DataFrame
        Dataframe that details the file name, 
        size (bytes), total record counts, and 
        a count by unique values in a field.
    """
    
    
    schema = f"""
                file_name STRING, 
                file_size_bytes LONG,
                file_size_memory_unit STRING, 
                record_qty LONG, 
                {id_col}_qty LONG
              """     
    data = []

    files = dbutils.fs.ls(dir_path)
    file_cnt = len(files)
    
    for f in tqdm(files, desc="Evaluating Files..."):
        sdf = spark.read.csv(f.path,
                              header=True)

        data.append(
                      (
                        f.name,
                        f.size,
                        convert_size(int(f.size)),                      
                        sdf.count(),
                        sdf.select(id_col).distinct().count()
                      )
                    )
    df = spark.createDataFrame(data=data,schema=schema)
    
    return(df)
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
                print(f'\t\t-Found {l_cnt} with patterns matching {k} in {file}')

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
