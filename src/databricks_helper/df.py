import pandas as pd
import openpyxl

#----------------------------------------------------------------------------------  

def df_to_pandas_chunks(df, chunk_size=100000, keys=[]):
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
    Returns
    -------
    generator : A generator that yields chunks of pandas DataFrames.
    """      
    # if a key was supplied, sort the dataframe
    if bool(keys):
        if not isinstance(keys, list):
            keys = [keys]
            
    # sort and yield chunked pandas dataframes from pyspark
    if not isinstance(df, pd.DataFrame):
        df = df.orderBy(keys)
        for i in range(0, df.count(), chunk_size):
            chunk = df.toPandas()[i:i + chunk_size]
            yield chunk
    else:
        # sort and yield chunked pandas dataframes 
        df = df.sort_values(by=keys)
        for i in range(0, len(df), chunk_size):
            chunk = df[i:i + chunk_size]
            yield chunk
            

#---------------------------------------------------------------------------------- 

def remove_pd_df_newlines(df, replace_char=''):
    """Removes newline characters ('\n') from all string 
    columns in a pandas DataFrame with the given replace 
    character.

    Parameters:
    ----------
    df : pandas.DataFrame
        The DataFrame to process.
    replace_char  : str, optional
        String value to replace newline character with.
        Defaults to single space (' ') .
    Returns:
    -------
    df : pandas.DataFrame
        The DataFrame with newlines removed from string columns.
    """

    df = df.replace('\n',replace_char, regex=True)
    return df

#---------------------------------------------------------------------------------- 

def xlsx_tabs_to_pd_dataframes(path, header_idx=0, rm_newlines=True):
    """
    Read all sheets/tabs from an excel file into a list of 
    pandas DataFrames.

    Parameters:
    ----------
    path : str
        Path to the Excel file.
    header_idx : int, optional
        Row index to use as column names (0-indexed).
        Defaults to 0.
    rm_newlines : Boolean, optional
        Option to remove newline characters ('\n') from 
        all string columns.
        Defaults to True.
    Returns:
    -------
    list of pandas.DataFrame
        A list containing a DataFrame for each worksheet in 
        the Excel file.
    """

    dfs = {}
    xls = pd.ExcelFile(path)

    # Iterate through each worksheet and read its data into a DataFrame
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet_name, header=header_idx)
        if rm_newlines:
            df = remove_pd_df_newlines(df)
        dfs[sheet_name] = df

    return dfs
