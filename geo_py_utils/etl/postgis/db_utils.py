
import sqlalchemy
import geopandas as gpd
import pandas as pd
from typing import Union
import re

logger = logging.getLogger(__file__)
  


def pg_db_exists(engine: sqlalchemy.engine.base.Engine , db_name: str) -> bool:
    """Check if a db exists

    Args:
        db_name (str): _description_

    Returns:
        bool: True if db exists
    """
 
    query_exists = f"""
                SELECT datname 
                FROM pg_catalog.pg_database 
                WHERE lower(datname) = lower('{db_name}');
                """
    with engine.connect() as conn:
        df_results = pd.read_sql(query_exists, conn)

    return df_results.shape[0] > 0


def pg_create_db(db_name, 
                user, 
                password , 
                host, 
                port):
    """Create a pg db + enable postgis extension 

    Sets user as owner

    Args:
        db_name (_type_): _description_
        user (_type_): _description_
        password (_type_): _description_
        host (_type_): _description_
        port (_type_): _description_
    """

    # Inspect all DBs
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{db_name}')

    # Db does not exist
    if pg_db_exists(engine):
         with engine.connect() as conn:

            conn.autocommit = True

            #Creating a cursor object using the cursor() method
            cursor = conn.cursor()

            #Preparing query to create a database - make sure it is a POSTGIS DB
            cursor.execute(f"""
                CREATE database {db_name} 
                WITH 
                    OWNER = {user}
                    ENCODING = 'UTF8'
                    CONNECTION LIMIT = -1;
                """)
            cursor.execute("CREATE EXTENSION postgis;")

            conn.close()

    logger.info(f"Successfully created postgis db {self.db_name}")


def pg_list_tables():
    pass



def pg_sql_to_df( engine: sqlalchemy.engine.base.Engine, 
                query: str, 
                is_spatial: bool=True, 
                **kwargs) -> Union[pd.DataFrame, gpd.GeoDataFrame] :
    """Convenience wrapper to send a query to the db and fetch results as a (geo) dataframe


    Args:
        engine (sqlalchemy.engine.base.Engine): _description_
        query (str): _description_
        is_spatial (bool, optional): _description_. Defaults to True.

    Returns:
        Union[ pd.DataFrame, gpd.GeoDataFrame]: _description_
    """

    with engine.connect() as conn:
        if is_spatial:
            df = pd.read_sql(query, conn)
        else:
            df= gpd.read_postgis(query, conn)
        

    return df



def pg_get_table_rows(engine: sqlalchemy.engine.base.Engine, tbl_name: str) -> int: 
    """Count the number of rows.

    Transfers query to pg_sql_to_df

    Args:
        db_name (sqlalchemy.engine.base.Engine): _description_
        tbl_name (str): _description_

    Returns:
        int: _description_
    """
    
    df = pg_sql_to_df( engine, 
                    f"select count(*) as num_rows from {tbl_name}",
                    is_spatial=False)

    return df.num_rows.values[0]



def pg_is_spatial_index_enabled(engine: sqlalchemy.engine.base.Engine,
                            tbl_name: str, 
                            schema = 'public',
                            geometry_name:str = 'geom') -> bool:
    """Determine if a spatial index has been set on the geom

    Looks for pg_indexes

    Args:
        engine (sqlalchemy.engine.base.Engine): _description_
        tbl_name (str): _description_
        schema (str, optional): _description_. Defaults to 'public'.
        geometry_name (str, optional): _description_. Defaults to 'geom'.

    Returns:
        bool: _description_
    """

    is_enabled = False

    try:
        query = f"""
        SELECT indexname, indexdef 
        FROM pg_indexes 
        WHERE tablename = '{tbl_name}' 
        AND schemaname = '{schema}'
        """

        df = pg_sql_to_df( engine, 
                        query,
                        is_spatial=False)

        # Few options are available for spatial index type
        # https://www.crunchydata.com/blog/the-many-spatial-indexes-of-postgis
        is_enabled = df.indexname.str.match('.*geom_idx') or \
            df.indexdef.str.contains('gist|btree|hash|brin|spgist') 

    except Exception as e:
        logger.warning(f"""
        Warning could not determine if spatial index is enabled or not for
        table {tbl_name} - {e}
        """


    return is_enabled
    
