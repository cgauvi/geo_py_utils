
    
from os.path import exists
import logging
from typing import Union
import pandas as pd
import geopandas as gpd
import sqlite3
from pathlib import Path

logger = logging.getLogger(__file__)
    
def drop_table(db_name: Union[Path, str], tbl_name: str):
    """Drop a table in a spatialite db if it exists.

    Args:
        db_name :str name of db
        tbl_name: str name of table to drop
    Returns:

    """

    existing_tables = list_tables(db_name)

    if (tbl_name not in existing_tables):
        logger.warning(f"Warning, cannot drop table {tbl_name} : not in db")
        return 

    with sqlite3.connect(db_name) as conn:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE {tbl_name}")



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


def get_table_crs(db_name: str, tbl_name: str, return_srid = False) -> Union[int, str] : 

    """ Extract the coordinate reference system from a spatialite table.

    Args:
        db_name : database
        tbl_name: name of table
        return_srid: returns the srid int if set to True. Otherwise returns the wkt2 representation.  (Defaults to False)
    Returns:
        crs:  WKT2 proj representation of crs (string) or srid (int)

    """

    # Check if table exists first 
    existing_tables = list_tables(db_name)

    if tbl_name not in existing_tables:
        raise ValueError(f"Fatal error! {tbl_name} does not exist")

    with sqlite3.connect(db_name) as con:

        # sqlite connection with extensions
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite")

        try:
            df_geometry = pd.read_sql(f"select * from geometry_columns where f_table_name = '{tbl_name}' ", con) 
            if df_geometry.shape[0] != 1:
                raise ValueError(f"Fatal error in get_table_crs: there are {df_geometry.shape[0]} rows in geometry_columns and there should be only 1 ")
                
            srid = df_geometry.srid.values[0]

            if return_srid:
                return srid

            else:
                df_crs = pd.read_sql(f'select * from spatial_ref_sys where srid = {srid}', con)
                crs = df_crs.srtext.values[0]# use the WKT2 proj representation: best practice 
                return crs

        except Exception as e:
            logger.error(f"Error retrieving the CRS from table ")


if __name__ == "__main__":

    import os
    from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
    from geo_py_utils.misc.constants import DATA_DIR

    existing_tables = list_tables(os.path.join(DATA_DIR, "test.db"))

    get_table_crs(os.path.join(DATA_DIR, "test.db"), 'qc_city_test_tbl', return_srid = True)
    get_table_crs(os.path.join(DATA_DIR, "test.db"), 'qc_city_test_tbl', return_srid = False)