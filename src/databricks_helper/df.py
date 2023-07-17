import pandas as pd
#----------------------------------------------------------------------------------  
def df_to_pandas_chunks(df, chunk_size=100000, keys=[], spark=None):
    """
    Generator that sorts and then chunks a PySpark 
    or pandas DataFrame into DataFrames of the given
    chunk size.

    Parameters
    ----------
    df : pd.DataFrame or pyspark.sql.DataFrame
        The dataframe to sort and chunk.
    chunk_size: int
        The max size of each chunk
    keys: str or list
        Column name or list of column names to sort 
        a dataframe on before chunking.
        Default, None - Sorting will not be applied
    spark : spark session object
        Default, if not supplied a new session will be built

    Returns
    -------
    generator : A generator that yields chunks of pandas DataFrames.
    """      
    # if a key was supplied, sort the dataframe
    if bool(keys):
        if not isinstance(keys, list):
            keys = [keys]
            
    # convert pandas dataframs to pyspark
    if not isinstance(df, pd.DataFrame):
        df = df.orderBy(keys)
        for i in range(0, df.count(), chunk_size):
            chunk = df.toPandas()[i:i + chunk_size]
            yield chunk
    else:
        df = df.sort_values(by=keys)
        for i in range(0, len(df), chunksize):
            chunk = df[i:i + chunksize]
            yield chunk
#---------------------------------------------------------------------------------- 
