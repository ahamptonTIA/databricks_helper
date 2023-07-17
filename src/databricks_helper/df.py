import os, re, uuid
import pandas as pd
from datetime import datetime, timezone
from pyspark.sql import SparkSession
# from databricks_helper import *
#----------------------------------------------------------------------------------  
def df_to_pandas_chunks(in_df, chunk_size=100000, keys=[], spark=spark):
    """
    Generator that sorts and then chunks a PySpark 
    or pandas DataFrame into DataFrames of the given
    chunk size.

    Parameters
    ----------
    in_df : pd.DataFrame or pyspark.sql.DataFrame
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
    # convert pandas dataframs to pyspark
    if isinstance(in_df, pd.DataFrame):
        if not spark:
            spark = file_ops.get_spark_session()
        df = spark.createDataFrame(in_df) 
    else:
        df = in_df
    # if a key was supplied, sort the dataframe
    if bool(key):
        df = df.orderBy([key])

    for i in range(0, df.count(), chunk_size):
        chunk = df.toPandas()[i:i + chunk_size]
        yield chunk
#----------------------------------------------------------------------------------  
