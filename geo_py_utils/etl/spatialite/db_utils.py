
    
from os.path import exists
import logging
from typing import Union, List
import pandas as pd
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
    if not exists(db_name):
        raise ValueError(f"Fatal error! Cannot run `promote_multi`. {db_name} does not exist")

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



def list_tables(db_name: Union[Path, str]) -> list:
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

    Raises:
        ValueError if DB does not exist

    """

    if not exists(db_name):
        raise ValueError(f"Fatal error! Cannot run `sql_to_df`. {db_name} does not exist")

    with sqlite3.connect(db_name) as conn:
        df = pd.read_sql(query, conn)

    return df


class GeomColumnAbsentError(Exception):
    def __init__(self, message):
        self.message = message

def get_table_geometries_name(db_name: Union[Path, str], tbl_name: str) -> List[str]: 
    """Get the geometry columns in a given table.

    Returns the list of ALL ORIGINAL columns in the table (if these were properly registered with AddGeometryColumn). 
    This does NOT return the columns registered in geometry_columns which are always lowercase.
    So this function is case sensitive

    Args:
        db_name (Union[Path, str]): _description_
        tbl_name (str): _description_

    Raises:
        GeomColumnAbsentError: error if no geometry present

    Returns:
        List[str]: list of geometry columns 
    """

    # Cols in geometry_columns all lower case, not necessarily the case in the original table..
    query = f"select f_geometry_column from geometry_columns where f_table_name = '{tbl_name}'"
    df_geom_type = sql_to_df(db_name, query)

    if df_geom_type.shape[0] == 0 :
        raise GeomColumnAbsentError(f'Fatal error! there are no geometry columns in db {db_name} - tbl {tbl_name}!')

    geom_names = df_geom_type['f_geometry_column'].values.tolist()

    if len(geom_names) != 1:
        logger.warn(f'Warning! There are {len(geom_names)} geometry columns in {tbl_name}')

    true_col_names = get_table_column_names(db_name, tbl_name)
    true_geo_col_names = [n for n in true_col_names if n.lower() in geom_names]

    return true_geo_col_names


class GeomTypeError(Exception):
    def __init__(self, message):
        self.message = message


def get_table_geometries_classes(db_name: Union[Path, str], tbl_name: str, geom_col:str = 'GEOMETRY') -> np.array:
    """Get the geometry clases where classes is in [POINT, MULTILINESTRING, ..] for the entire geometry coumn

    See https://www.gaia-gis.it/gaia-sins/spatialite-manual-2.3.1.html#t2.3. 

    Args:
        db_name (Union[Path, str]): _description_
        tbl_name (str): _description_
        geom_col (str, optional): _description_. Defaults to 'GEOMETRY'.

    Raises:
        ValueError: error if no class found for any geom in column

    Returns:
        np.array: unique classes - typically 1 or 2
    """

    with sqlite3.connect(db_name) as con:
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite") 

        df_type = pd.read_sql(f'select GeometryType({geom_col}) geomtype from {tbl_name}', con)

    if df_type[['geomtype']].dropna().shape[0] == 0:
        raise ValueError(f'Fatal error! no geometry type for {geom_col}')
    
    return df_type.geomtype.values


def get_table_geometry_group_classes(db_name: Union[Path, str], tbl_name: str, geom_col:str = 'GEOMETRY') -> str:
    """Using the unique geometry classes of the class, return a str representig the type.

    Safe: always returns the multi version if single and many.

    Args:
        db_name (Union[Path, str]): _description_
        tbl_name (str): _description_
        geom_col (str, optional): _description_. Defaults to 'GEOMETRY'.

    Returns:
        str: class e.g. `MULTIPOINT`
    """

    df_types_all = get_table_geometries_classes(db_name, tbl_name, geom_col)
    df_types_unique = np.unique(df_types_all)
    df_types_unique = sorted(df_types_unique) # order important: could convert to set also

    if len(df_types_unique) == 1:
        return df_types_unique[0] 
        
    logger.warning('More than one type of geometry')
 
    if np.array_equal(['LINESTRING', 'MULTILINESTRING'],  df_types_unique):
        return 'MULTILINESTRING' 
    elif np.array_equal(['MULTIPOINT', 'POINT'], df_types_unique) :
        return 'MULTIPOINT' 
    elif np.array_equal(['MULTIPOLYGON', 'POLYGON'], df_types_unique):
        return 'MULTIPOLYGON'
    else:
        logger.warning('Mixed geometries, cant find a fixe group, returning GEOMETRYCOLLECTION')
        return 'GEOMETRYCOLLECTION'



def get_table_geometry_type(db_name: Union[Path, str], tbl_name: str, return_int = False) -> str: 
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
        return 1 if return_int else 'POINT'
    elif df_geom_type['geometry_type'].values[0] == 2:
        return 2 if return_int else  'LINESTRING'
    elif df_geom_type['geometry_type'].values[0] == 3:
        return 3 if return_int else  'POLYGON'
    elif df_geom_type['geometry_type'].values[0] == 4:
        return 4 if return_int else  'MULTIPOINT'
    elif df_geom_type['geometry_type'].values[0] == 5:
        return 5 if return_int else  'MULTILINESTRING'
    elif df_geom_type['geometry_type'].values[0] == 6:
        return 6 if return_int else  'MULTIPOLYGON'
    elif df_geom_type['geometry_type'].values[0] == 7:
        return 7 if return_int else 'GEOMETRYCOLLECTION'
    else:
        raise GeomTypeError(f"""
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

    if exists(db_name):
        drop_table(db_name, tbl_name) 
        drop_geometry_columns(db_name, tbl_name, geometry_name)
        drop_spatial_index(db_name, tbl_name, geometry_name)
    else:
        logger.info(f'Not running drop_geo_table_all {db_name} does not exist')
 



def drop_spatial_index(db_name: str,  tbl_name: str, geometry_name: str) -> None:
    """Remove all the virtual tables associated with the spatial index of a given table geometry + disable geometry

    Args:
        db_name (str): _description_
        tbl_name (str): _description_
        geometry_name (str): _description_
    """

    if not exists(db_name):
        logger.info(f'Not running drop_spatial_index {db_name} does not exist')
        return 

    with sqlite3.connect(db_name) as con:
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite")

        try:
            cur = con.cursor()
            cur.execute(f" SELECT DisableSpatialIndex('{tbl_name}', '{geometry_name}'); ")
            con.commit()
        except Exception as e:
            logger.warning(f"Warning! Failed to drop spatial index on table {tbl_name} -> geometry {geometry_name} - {e}" )

        list_aux_tables_to_drop = ['','_rowid', '_node', '_parent']
        for suffix in list_aux_tables_to_drop:
            try:
                cur.execute(f"DROP TABLE IF EXISTS idx_{tbl_name}_{geometry_name}{suffix}")
                con.commit()
            except Exception as e:
                logger.warning(f"Warning! Failed to drop table idx_{tbl_name}_{geometry_name}{suffix} - {e}" )




  
def drop_table(db_name: Union[Path, str], tbl_name: str):
    """Drop a table in a spatialite db if it exists.

    Args:
        db_name :str name of db
        tbl_name: str name of table to drop
    Returns:

    """
    if not exists(db_name):
        logger.info(f'Not running drop_table {db_name} does not exist')
        return 

    with sqlite3.connect(db_name, isolation_level=None) as conn:
        try:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE if exists {tbl_name}")
            conn.commit()
        except Exception as e:
            logger.warning(f"Warning! Failed to drop table tbl_name - {e}" )


def drop_geometry_columns(db_name: str, tbl_name: str, geometry_name:str) -> None:
    """Remove the `tbl_name` record in the auxiliary `geometry_columns`, `vector_layers`, etc tables.

    Useful to ensure we clean everything up when deleting a table with geometry

    Args:
        db_name (str): _description_
        tbl_name (str): _description_
    """

    if not exists(db_name):
        logger.info(f'Not running drop_geometry_columns {db_name} does not exist')
        return

    with sqlite3.connect(db_name, isolation_level=None) as con:
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite")

        try:
            cur = con.cursor()
            cur.execute(f" SELECT DiscardGeometryColumn('{tbl_name}', '{geometry_name}'); ")
            con.commit()
        except Exception as e:
            logger.warning(f"Warning! Failed to discard geometry on table {tbl_name} -> geometry {geometry_name} - {e}" )


        for t in ['geom_cols_ref_sys', 'geometry_columns']:
            try:
                cur = con.cursor()
                cur.execute(f"DELETE FROM {t} WHERE f_table_name = '{tbl_name}'; ")
                con.commit()
            except Exception as e:
                logger.warning(f"Warning! Failed to drop record from {t} for table {tbl_name} - {e}" )


        for suff in ['', '_auth', '_fields_info', '_statistics']:
            try:
                cur = con.cursor()
                cur.execute(f"DELETE FROM vector_layers{suff} WHERE table_name = '{tbl_name}'; ")
                con.commit()
            except Exception as e:
                logger.warning(f"Warning! Failed to drop record from vector_layers{suff} for table {tbl_name} - {e}" )


def rename_geom_column(
    db_name: str, 
    tbl_name: str, 
    new_geom: str="GEOMETRY", 
    old_geom: Union[str,None] = None,
    create_index = True
    ) -> None:

    # Get the curent geometry name
    if old_geom is None:
        logger.info('Trying to guess the old geom name and taking the first if there are many..')
        geom_cols = get_table_geometries_name(db_name, tbl_name)
        old_geom = geom_cols[0]
    
    # watch out for case sensitiviy, safer to do nothing if same when dropping case
    if old_geom.lower() == new_geom.lower():
        logger.warn('Warning! same geometry, but different case! Not doing anything since unsafe!')
        return 

    # Get the srid
    srid = get_table_crs(db_name, tbl_name, return_srid=True)

    # Get the geometry class: e.g. MULTIPOINT
    try:
        geom_type = get_table_geometry_group_classes(db_name, tbl_name, old_geom)
    except GeomTypeError as e:
        logger.warn(f'Could not identify poygon type: might have some nulls: returning GEOMETRYCOLLECTION - {e}')
        geom_type= "GEOMETRYCOLLECTION"

    cast_to_multi=False
    if geom_type in ["GEOMETRYCOLLECTION", "MULTIPOLYGON", "MULTILINESTRING", "MULTIPOINT"  ]:
        cast_to_multi =True

    # 1. Add a blank geom col
    # 2. Populate by copying from old geom
    # 3. Create an index (optional)
    # 4. Remove the old geom from the tbl + aux geometry columns
    with sqlite3.connect(db_name, isolation_level=None) as con:
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite")
    
        cur = con.cursor()

        cur.execute(f"SELECT AddGeometryColumn('{tbl_name}', '{new_geom}', {srid}, '{geom_type}', 'XY', 0);")
        con.commit()
        
        if cast_to_multi:
            cur.execute(f'UPDATE {tbl_name} SET "{new_geom}" = CastToMulti("{old_geom}");')
        else:
            cur.execute(f'UPDATE {tbl_name} SET "{new_geom}" = "{old_geom}";')
        con.commit()
    
        if create_index:
            cur.execute(f"SELECT CreateSpatialIndex('{tbl_name}', '{new_geom}');")
            con.commit()
        
        cur.execute(f"SELECT DiscardGeometryColumn('{tbl_name}', '{old_geom}');")
        con.commit()

        cur.execute(f"ALTER TABLE '{tbl_name}' DROP COLUMN '{old_geom}';")
        con.commit()
    

    logger.info(f'Successfully renamed column {old_geom} to {new_geom} in table {tbl_name}')


def rename_columns(db_name: str, tbl_name: str, dict_rename: dict) -> None:
    """Rename columns safely (dont touch)

    Args:
        db_name : database
        tbl_name: name of table
        dict_rename: {old: new}
    Returns:
       None

    Raises:
        ValueError if we try to rename a geometry column. Should use the specialized rename_geom_column instead which requires add arguments

    """

    if len(get_table_column_names(db_name, tbl_name)) == 0:
        raise ValueError(f'Fatal error! {tbl_name} has no columns!')

    with sqlite3.connect(db_name, isolation_level=None) as con:
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite")

        try:
            geom_cols = get_table_geometries_name(db_name, tbl_name)
        except GeomColumnAbsentError:
            geom_cols = [] # no geometry

        for k, v in dict_rename.items():
            if v in geom_cols:
                raise ValueError(f'Fatal error! Cannot rename geometry column {v} wih rename_columns. Use `rename_geom_column`')
            
            query_rename=f"""
                ALTER TABLE {tbl_name}
                RENAME COLUMN {k} TO '{v}';
            """

            cur = con.cursor()
            cur.execute(query_rename)
            con.commit()
                
    # QA + print message
    unchanged_cols = np.setdiff1d(list(dict_rename.values()), get_table_column_names(db_name, tbl_name))
    unchanged_cols_str = ",".join(unchanged_cols)

    if len(unchanged_cols) != 0:
        logger.warn(f'Warning! {unchanged_cols_str} columns not changed!')
    else:
        renamed_cols_str = ",".join(dict_rename.keys())
        new_cols_str = ",".join(dict_rename.values())

        logger.info(
            f"""
            Successfully renamed columns {renamed_cols_str}
            to -> {new_cols_str}
            """
            )
    



def get_table_crs(db_name: str, tbl_name: str, return_srid = False) -> Union[int, str] : 

    """ Extract the coordinate reference system from a spatialite table.

    Args:
        db_name : database
        tbl_name: name of table
        return_srid: returns the srid int if set to True. Otherwise returns the wkt2 representation.  (Defaults to False)
    Returns:
        crs:  WKT2 proj representation of crs (string) or srid (int)

    Raises:
        ValueError if db or tbl does not exist

    """

    if not exists(db_name):
        raise ValueError(f"Fatal error! Cannot run `get_table_crs` db {db_name} does not exist")

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
            logger.error(f"Error retrieving the CRS from table - {e}")



def get_table_column_names(db_name: str, tbl_name: str) -> np.array:
    """Get table column names

    Calls `get_table_column_all_metadata` 

    Args:
        db_name (str): _description_
        tbl_name (str): _description_

    Raises:
        ValueError: _description_

    Returns:
        np.array: _description_
    """

    if not exists(db_name):
        raise ValueError(f"Fatal error! Cannot run `get_table_column_names` db {db_name} does not exist")

    df = get_table_column_all_metadata(db_name, tbl_name)

    if df.shape[0] == 0:
        logger.warning(f"Warning! table {tbl_name} does not exist or does not have columns")

    return df.name.values

    
def get_table_column_all_metadata(db_name: str, tbl_name: str) -> pd.DataFrame:
    """Get the table info - cloumn names, types, etc.

    Wrapper over PRAGMA table_info(tbl)

    Args:
        db_name (str): _description_
        tbl_name (str): _description_

    Raises:
        ValueError: _description_

    Returns:
        pd.DataFrame: _description_
    """

    if not exists(db_name):
        raise ValueError(f"Fatal error! Cannot run `get_table_column_all_metadata` db {db_name} does not exist")

    with sqlite3.connect(db_name) as conn:
        query = f"PRAGMA table_info({tbl_name});"
        df = pd.read_sql(query, conn)

    return df
        

if __name__ == "__main__":

    import os
    from geo_py_utils.census_open_data.open_data import DEFAULT_QC_CITY_NEIGH_URL
    from geo_py_utils.misc.constants import DATA_DIR
    from geo_py_utils.etl.db_etl import  Url_to_spatialite 
 
    DB_PATH = os.path.join(DATA_DIR, "test.db")

    def upload_qc_neigh_db(db_name = DB_PATH,
                            tbl_name = "qc_city_test_tbl",
                            download_url = DEFAULT_QC_CITY_NEIGH_URL):
        
        with Url_to_spatialite(
            db_name = db_name, 
            table_name = tbl_name,
            download_url = download_url,
            download_destination = DATA_DIR,
            promote_to_multi=False) as uploader:

            uploader.upload_url_to_database()

    drop_geo_table_all(DB_PATH, "qc_city_test_tbl",'GEOMETRY' )

    if (not os.path.exists(DB_PATH)) or \
        (not "qc_city_test_tbl" in list_tables(DB_PATH)) :
        upload_qc_neigh_db()
    
    promote_multi(DB_PATH, "qc_city_test_tbl")
