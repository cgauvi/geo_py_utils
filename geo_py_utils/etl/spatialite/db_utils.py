
    
from os.path import exists
import logging
from typing import Union
import pandas as pd
import geopandas as gpd
import sqlite3
from pathlib import Path
import numpy as np 

logger = logging.getLogger(__file__)
  


def promote_multi(db_name: Union[Path, str],
                 tbl_name: str ,
                 geometry_name: str = 'GEOMETRY') -> None:

    """Promote geometry to multi version

    Args:
        db_name (Union[Path, str]): _description_
        tbl_name (str): _description_
        geometry_name(str): _description_

    Returns:
        None
    """
    logger.info("Forcing promotion to multipolygon...")

    # Safey get geom before
    geom_before = "MIXED/ERROR"
    try:
        geom_before = get_table_geometry_type(db_name, tbl_name)
    except Exception as e:
        logger.warning(f'Error in promote_multi getting geom type before - {e}')

    with sqlite3.connect(db_name) as con:
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite")
         
        # Now update the tabe and actually fill the geometry
        query_update = f"UPDATE {tbl_name} " \
                f" SET {geometry_name} = CastToMulti({tbl_name}.{geometry_name})  "  
            
        c = con.cursor()
        c.execute(query_update)
        con.commit()

    # Safey get geom after
    geom_after = "MIXED/ERROR"
    try:
        geom_after = get_table_geometry_type(db_name, tbl_name)
    except Exception as e :
        logger.warning(f'Error in promote_multi getting geom type before - {e}')

    # Print info on transformation
    if geom_after == geom_before:
        logger.warning(f"""
        Warning! didnt managed to promote {tbl_name}.{geometry_name} to multi version!
        Geometry is stil {geom_before}
        """)
    else:
        logger.info(f"""
        Successfully promoted {tbl_name}.{geometry_name} to multi! 
        Geometry was {geom_before} and is now {geom_after}
        """)



def list_tables(db_name: Union[Path, str]) ->  list:
    """ List tables  in a spatialite db

    Args:
        db_name 
    Returns:
        list of table names : list: will return an empty list if db does not exist

    """

    list_tables = []
    if exists(db_name):
        logger.info(f"DB {db_name} exists!")

        with sqlite3.connect(db_name) as con:
            df_tbls = pd.read_sql("SELECT tbl_name FROM sqlite_master WHERE type = 'table';", con)

        list_tables = df_tbls.tbl_name.values
    else:
        logger.info(f"DB {db_name} does not exists")

        
    return list_tables



def sql_to_df(db_name: Union[Path, str], query: str) -> pd.DataFrame:

    """Convenience wrapper to send a query to the db and fetch results as a dataframe

    Args:
        db_name: name
        query: query string

    Returns:
        df : dataframe

    """
    with sqlite3.connect(db_name) as conn:
        df = pd.read_sql(query, conn)

    return df


def get_table_geometry_type(db_name: Union[Path, str], tbl_name: str) -> str: 
    """Get the goemtry type

    See https://r-spatial.github.io/sf/articles/sf1.html
    1 - POINT
    2 - LINESTRING
    3 - POLYGON
    4 - MULTIPOINT	set of points; a MULTIPOINT is simple if no two Points in the MULTIPOINT are equal
    5 - MULTILINESTRING	set of linestrings
    6 - MULTIPOLYGON	set of polygons
    7 - GEOMETRYCOLLECTION

    Args:
        db_name (Union[Path, str]): _description_
        tbl_name (str): _description_

    Raises:
        ValueError: if the tbl does not have a geometry column

    Returns:
        str: geom name - e.g.' POINT' 
    """
    query = f"select * from geometry_columns where f_table_name = '{tbl_name}'"
    df_geom_type = sql_to_df(db_name, query)

    if df_geom_type.shape[0] == 0:
        raise ValueError(f"Fatal error table {tbl_name} does not have an associated geometry!")

    if df_geom_type['geometry_type'].values[0] == 1:
        return 'POINT'
    elif df_geom_type['geometry_type'].values[0] == 2:
        return 'LINESTRING'
    elif df_geom_type['geometry_type'].values[0] == 3:
        return 'POLYGON'
    elif df_geom_type['geometry_type'].values[0] == 4:
        return 'MULTIPOINT'
    elif df_geom_type['geometry_type'].values[0] == 5:
        return 'MULTILINESTRING'
    elif df_geom_type['geometry_type'].values[0] == 6:
        return 'MULTIPOLYGON'
    elif df_geom_type['geometry_type'].values[0] == 7:
        return 'GEOMETRYCOLLECTION'
    else:
        raise ValueError(f"""
            Fatal error! cannot determine geoemtry type, 
            got {df_geom_type['geometry_type'].values[0]} as code
            """
        )

def get_table_rows(db_name: Union[Path, str], tbl_name: str) -> int: 
    """Count the number of rows.

    Transfers query to sql_to_df

    Args:
        db_name (Union[Path, str]): _description_
        tbl_name (str): _description_

    Returns:
        int: _description_
    """
    
    df = sql_to_df(db_name, f"select count(*) as num_rows from {tbl_name}")

    return df.num_rows.values[0]


def is_spatial_index_enabled_valid(db_name: str, tbl_name: str, geometry_name:str) -> bool:

    return is_spatial_index_enabled(db_name, tbl_name) and \
            is_spatial_index_valid(db_name, tbl_name, geometry_name)


def is_spatial_index_enabled(db_name: str, tbl_name: str) -> bool:
    """Determine if the spatial index is enabled for a given table

    Args:
        db_name (str):  
        tbl_name (str):  

    Returns:
        bool: True if column `spatial_index_enabled` is 1
    """

    # Check if table exists first 
    existing_tables = list_tables(db_name)

    if tbl_name not in existing_tables:
        raise ValueError(f"Fatal error! {tbl_name} does not exist")

    with sqlite3.connect(db_name) as con:

        # sqlite connection with extensions
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite")

        df_geometry = pd.read_sql(f"SELECT spatial_index_enabled, f_table_name FROM geometry_columns WHERE f_table_name = '{tbl_name}' ", con) 

    is_enabled = df_geometry['spatial_index_enabled'].values[0] == 1

    return is_enabled



def is_spatial_index_valid(db_name: str, tbl_name: str, geometry_name: str) -> bool:
    """Check if a given spatial index is valid

    Args:
        db_name (str): _description_
        tbl_name (str): _description_
        geometry_name (str): _description_

    Raises:
        ValueError: if no spatial index exists for the table geometry

    Returns:
        bool: True if no error, False (0) otherwise
    """
    with sqlite3.connect(db_name) as con:
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite")

        query_idx = f" SELECT CheckSpatialIndex ('{tbl_name}', '{geometry_name}') as is_spatial_idx_correct "

        df_results = pd.read_sql(query_idx, con)
        
        if (df_results is None) or \
            (df_results.shape[0] == 0) or \
            (df_results.is_spatial_idx_correct.values[0] is None) :
            raise ValueError(f"Fatal error {tbl_name}.{geometry_name} does not have an index")
        
    return bool(df_results.is_spatial_idx_correct.values[0] == 1)




def drop_geo_table_all(db_name: Union[Path, str], tbl_name: str, geometry_name: str)-> None:
    """Safely delete a table + auxiliary data including records in geometry_column table + spatial index if it exists

    Args:
        db_name (Union[Path, str]): _description_
        tbl_name (str): _description_
        geometry_name (str): _description_
    """

    drop_table(db_name, tbl_name) 
    drop_geometry_columns(db_name, tbl_name, geometry_name)
    drop_spatial_index(db_name, tbl_name, geometry_name)
 



def drop_spatial_index(db_name: str,  tbl_name: str, geometry_name: str) -> None:

    """Remove all the virtual tables associated with the spatial index of a given table geometry + disable geometry

    Args:
        db_name (str): _description_
        tbl_name (str): _description_
        geometry_name (str): _description_
    """

    with sqlite3.connect(db_name) as con:
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite")

        try:
            cur = con.cursor()
            cur.execute(f" SELECT DisableSpatialIndex('{tbl_name}', '{geometry_name}'); ")
            con.commit()
        except Exception as e:
            logger.warning(f"Warning! Failed to drop spatial index on table {tbl_name} -> geometry {geometry_name}" )

        list_aux_tables_to_drop = ['','_rowid', '_node', '_parent']
        for suffix in list_aux_tables_to_drop:
            try:
                cur.execute(f"DROP TABLE IF EXISTS idx_{tbl_name}_{geometry_name}{suffix}")
                con.commit()
            except Exception as e:
                logger.warning(f"Warning! Failed to drop table idx_{tbl_name}_{geometry_name}{suffix}" )




  
def drop_table(db_name: Union[Path, str], tbl_name: str):
    """Drop a table in a spatialite db if it exists.

    Args:
        db_name :str name of db
        tbl_name: str name of table to drop
    Returns:

    """

    with sqlite3.connect(db_name) as conn:
        try:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE if exists {tbl_name}")
        except Exception as e:
            logger.warning(f"Warning! Failed to drop table tbl_name" )


def drop_geometry_columns(db_name: str, tbl_name: str, geometry_name:str) -> None:

    """Remove the `tbl_name` record in the auxiliary `geometry_columns`, `vector_layers`, etc tables.

    Useful to ensure we clean everything up when deleting a table with geometry

    Args:
        db_name (str): _description_
        tbl_name (str): _description_
    """
    with sqlite3.connect(db_name) as con:
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite")

        try:
            cur = con.cursor()
            cur.execute(f" SELECT DiscardGeometryColumn('{tbl_name}', '{geometry_name}'); ")
            con.commit()
        except Exception as e:
            logger.warning(f"Warning! Failed too discard geometryon table {tbl_name} -> geometry {geometry_name}" )


        for t in ['geom_cols_ref_sys', 'geometry_columns']:
            try:
                cur = con.cursor()
                cur.execute(f"DELETE FROM {t} WHERE f_table_name = '{tbl_name}'; ")
                con.commit()
            except Exception as e:
                logger.warning(f"Warning! Failed to drop record from {t} for table {tbl_name}" )


        for suff in ['', '_auth', '_fields_info', '_statistics']:
            try:
                cur = con.cursor()
                cur.execute(f"DELETE FROM vector_layers{suff} WHERE table_name = '{tbl_name}'; ")
                con.commit()
            except Exception as e:
                logger.warning(f"Warning! Failed to drop record from vector_layers{suff} for table {tbl_name}" )


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



def get_table_column_names(db_name: str, tbl_name: str) -> np.array:

    df = get_table_column_all_metadata(db_name, tbl_name)

    if df.shape[0] == 0:
        logger.warning(f"Warning! table {tbl_name} does not exist or does not have columns")

    return df.name.values

    
def get_table_column_all_metadata(db_name: str, tbl_name: str) -> pd.DataFrame:

    with sqlite3.connect(db_name) as conn:
        query = f"PRAGMA table_info({tbl_name});"
        df = pd.read_sql(query, conn)

    return df
        

if __name__ == "__main__":

    import os
    from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
    from geo_py_utils.misc.constants import DATA_DIR
    from geo_py_utils.etl.db_etl import  Url_to_spatialite 
    from geo_py_utils.etl.spatialite.db_utils import list_tables, drop_geo_table_all

    def upload_qc_neigh_db(db_name = os.path.join(DATA_DIR, "test.db"),
                            tbl_name = "qc_city_test_tbl",
                            download_url = QC_CITY_NEIGH_URL):
        
        with Url_to_spatialite(
            db_name = db_name, 
            table_name = tbl_name,
            download_url = download_url,
            download_destination = DATA_DIR,
            promote_to_multi=False) as uploader:

            uploader.upload_url_to_database()

    drop_geo_table_all(os.path.join(DATA_DIR, "test.db"),"qc_city_test_tbl",'GEOMETRY' )

    if (not os.path.exists(os.path.join(DATA_DIR, "test.db"))) or \
        (not "qc_city_test_tbl" in list_tables(os.path.join(DATA_DIR, "test.db"))) :
        upload_qc_neigh_db()
    
    promote_multi(os.path.join(DATA_DIR, "test.db"),"qc_city_test_tbl")