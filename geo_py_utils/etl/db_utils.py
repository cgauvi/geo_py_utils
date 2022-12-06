
    
from os.path import exists
import logging
from typing import Union
import pandas as pd
import geopandas as gpd
import sqlite3
from pathlib import Path

logger = logging.getLogger(__file__)
    
def list_tables(db_name: Union[Path, str]) -> Union[None, list]:
    """ List tables  in a spatialite db

    Args:

    Returns:
        list of table names : list

    """

    list_tables = None
    if exists(db_name):
        logger.info(f"DB {db_name} exists!")

        with sqlite3.connect(db_name) as con:
            df_tbls = pd.read_sql("SELECT tbl_name FROM sqlite_master WHERE type = 'table';", con)

        list_tables = df_tbls.tbl_name.values
    else:
        logger.info(f"DB {db_name} does not exists")

        
    return list_tables



def sql_to_df(db_name: Union[Path, str], query: str) ->  pd.DataFrame :

    """Convenience wrapper to send a query to the db and fetch results as a dataframe

    Args:
        db_name: name
        query: query string

    Returns:
        df : (geo) dataframe

    """
    with sqlite3.connect(db_name) as conn:
        df = pd.read_sql(query, conn)

    return df


def get_table_rows(db_name: Union[Path, str], tbl_name :str) -> int: 
    
    """Count the number of rows.

    Transfers query to sql_to_df

    """
    
    df = sql_to_df(db_name, f"select count(*) as num_rows from {tbl_name}")

    return df.num_rows.values[0]



if __name__ == "__main__":

    import os
    from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
    from geo_py_utils.misc.constants import DATA_DIR

    existing_tables = list_tables(os.path.join(DATA_DIR, "test.db"))