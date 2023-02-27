
import sqlalchemy
import geopandas as gpd
import pandas as pd
from typing import Union, List
import re
import logging
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__file__)
  
def pg_create_engine(db_name:str,
                    user: str,
                    password: str,
                    host: str,
                    port: Union[int,str]) -> sqlalchemy.engine.base.Engine:
    """Super thin convenience wrapper around create_engine to abstract string details

    Args:
        db_name (str): _description_
        user (str): _description_
        password (str): _description_
        host (str): _description_
        port (Union[int,str]): _description_

    Returns:
        sqlalchemy.engine.base.Engine: _description_
    """
    return create_engine(f'postgresql://{user}:{password}@{host}:{str(port)}/{db_name}')

def pg_db_exists(engine: sqlalchemy.engine.base.Engine, db_name: str) -> bool:
    """Check if a db exists

    Args:
        engine (sqlalchemy.engine.base.Engine)
        db_name (str): _description_

    Returns:
        bool: True if db exists
    """
    db_exists = False

    query_exists = f"""
                SELECT datname 
                FROM pg_catalog.pg_database 
                WHERE lower(datname) = lower('{db_name}');
                """
    # Trying to connect to non-existent db raises OperationalError
    # https://stackoverflow.com/questions/15062208/how-to-search-for-the-existence-of-a-database-with-sqlalchemy
    try:
        with engine.connect() as conn:
            df_results = pd.read_sql(query_exists, conn)
        db_exists = df_results.shape[0] > 0
    except OperationalError:
        logger.info(f'{db_name} does not seem to exist')

    return db_exists


def pg_create_db(db_name: str, 
                user: str,
                password: str, 
                host: str,
                port: str):
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
    engine_db_specific = create_engine(f'postgresql://{user}:{password}@{host}:{str(port)}/{db_name}')

    # Db does not exist
    if not pg_db_exists(engine_db_specific, db_name):

        # Conenct to generic posrgres db
        engine_generic = create_engine(f'postgresql://{user}:{password}@{host}:{str(port)}/postgres')
        with engine_generic.connect() as conn:

            session = sessionmaker(bind=engine_generic)()
            session.connection().connection.set_isolation_level(0)

            #Preparing query to create a database - make sure it is a POSTGIS DB
            session.\
                execute(f"""
                CREATE database {db_name} 
                WITH 
                    OWNER = {user}
                    ENCODING = 'UTF8'
                    CONNECTION LIMIT = -1;
                """)


        # Connect on the correct newlys created db 
        with engine_db_specific.connect() as conn:

            conn.\
                    execute(f"""
                    CREATE EXTENSION IF NOT EXISTS postgis 
                    """
                )


        logger.info(f"Successfully created postgres db {db_name} & enabled postigs extension")
    else:
        logger.info(f"{db_name} already exists")


def pg_list_tables(engine: sqlalchemy.engine.base.Engine, db_name: str) -> List[str]:
    """ List tables in a postgres db

    Args:
        engine (sqlalchemy.engine.base.Engine)
        db_name (str)
    Returns:
        list of table names : list: will return an empty list if db does not exist

    """
    
    list_tables = []
    if not pg_db_exists(engine, db_name):
        logger.info(f"DB {db_name} does not exist!")
        return list_tables

    query = """
        SELECT tablename, schemaname
        FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog'
        AND   schemaname != 'information_schema';
        """

    with engine.connect() as conn:
        df_results = pd.read_sql(query, conn)

    return df_results.tablename.values




  
def pg_drop_table(engine: sqlalchemy.engine.base.Engine, db_name: str, tbl_name: str):
    """Drop a table in a postgres db if it exists.

    Args:
        engine (sqlalchemy.engine.base.Engine)
        db_name (str): name of db
        tbl_name(str):  name of table to drop
    Returns:

    """
    if not pg_db_exists(engine, db_name):
        logger.info(f'Not running pg_drop_table {db_name} does not exist')
        return 

    with engine.connect() as conn:
        try:
            conn.execute(f"DROP TABLE if exists {tbl_name}")
        except Exception as e:
            logger.warning(f"Warning! Failed to drop table {tbl_name}" )


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
        )


    return is_enabled
    
