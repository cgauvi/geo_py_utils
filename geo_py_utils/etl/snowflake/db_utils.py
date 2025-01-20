import json
import logging 
import numpy as np
import pandas as pd
from snowflake.connector.connection import SnowflakeConnection


logger = logging.getLogger(__name__)


def table_exists(conn: SnowflakeConnection, tbl_name) -> bool:

    if conn.schema :
         schema_str =f" and table_schema = '{str(conn.schema).upper()}' " 
    else:
        schema_str =""
        logger.warning(f'Warning in `table_exists`! could not infer schema from connection;')

    query_tbl_exists = f"""
        select to_boolean(count(1)) as tbl_exists
        from information_schema.tables 
        where table_schema = '{str(conn.schema).upper()}' and 
        table_name = '{str(tbl_name).upper()}';
    """

    df_results = pd.read_sql(query_tbl_exists, conn)

    return df_results.TBL_EXISTS.values[0]



def sfkl_show_column(conn: SnowflakeConnection, tbl_name: str) -> pd.DataFrame:
    """
    Run the `SHOW COLUMNS IN TABLE . ` command and load results in pd.DataFrame
    Also parses the type json to extract the `type`

    Args:
        conn (SnowflakeConnection): _description_
        tbl_name (str): _description_

    Returns:
        pd.DataFrame: df with new json_col column containing a text for the type (rather than a json)
    """

    if not table_exists(conn, tbl_name):
        raise ValueError(f'Error! {tbl_name} does not exist')

    query_types = f"""
        SHOW COLUMNS in TABLE {tbl_name}
    """

    df_types = pd.read_sql(query_types, conn)

    #Parse the json
    df_types['json_col'] = df_types.apply(lambda row: json.loads(row['data_type'])['type'], axis=1)

    return  df_types


def sfkl_get_specific_col_types(conn: SnowflakeConnection, tbl_name: str, list_cols: list[str]) -> pd.DataFrame:
    """
    Similar to sfkl_show_column, but filters by column 

    Args:
        conn (SnowflakeConnection): _description_
        tbl_name (str): _description_
        list_cols (list[str]): _description_

    Raises:
        ValueError: error if queried cols do not exist

    Returns:
        pd.DataFrame: df with columns: ['column_name','data_type']
    """

    if len(list_cols) <= 0:
        raise ValueError('Error! list of columns is empty!') 
    
    df_types = sfkl_show_column(conn, tbl_name)

    if df_types.shape[0] <= 0:
        raise ValueError(f'Error! {tbl_name} does not have any rows') 

    matching_cols = df_types.column_name[df_types.column_name.isin(np.array(list_cols))]
    not_matching_cols = np.array(list_cols)[~np.isin(np.array(list_cols), (df_types.column_name))]
    not_matching_cols_str = ",".join(not_matching_cols)

    if len(matching_cols) < len(list_cols):
        str_sfkl = ",".join(df_types.column_name.values)
        str_query = ",".join(list_cols)
        raise ValueError(f'Error! list of columns {str_query} not all in sfkl which has {str_sfkl} -> missing {not_matching_cols_str}') 
    
    dict_results = {}
    for cols in matching_cols:
        dict_results[cols] = json.loads(df_types[df_types.column_name == cols].data_type.values[0])['type']
        
    return pd.DataFrame(dict_results.items(), columns=['column_name','data_type'])



def sfkl_get_table_srid(
        conn: SnowflakeConnection, 
        tbl_name: str,
        geometry_name :str
    ) -> int:
                
    """Get the SRID associated with a geo enabled table

    Args:
        conn (SnowflakeConnection): _description_
        tbl_name (str): _description_
        geometry_name (str): _description_
    

    Returns:
        int: unique srid of the table

    Raises:
        Exception: if there are more than 1 SRID used in the table 

    """

    query_srid = " SELECT srid, count(*) as num_geo_feat" \
                 " FROM  " \
                 f" (SELECT ST_SRID({geometry_name}) as srid from {tbl_name}) as sri_tbl " \
                 " GROUP BY srid "

    df_srid = pd.read_sql(query_srid, conn)

    assert df_srid.shape[0] == 1, \
         f"Fatal error! {tbl_name} has more than one SRID!"

    srid = df_srid.SRID.values[0]

    return srid

