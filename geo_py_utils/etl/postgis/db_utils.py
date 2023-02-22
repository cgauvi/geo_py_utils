
import sqlalchemy
import geopandas as gpd
import pandas as pd
from typing import Union
import re

logger = logging.getLogger(__file__)
  


def list_tables():



def sql_to_df(  engine: sqlalchemy.engine.base.Engine, 
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



def get_table_rows(engine: sqlalchemy.engine.base.Engine, tbl_name: str) -> int: 
    """Count the number of rows.

    Transfers query to sql_to_df

    Args:
        db_name (sqlalchemy.engine.base.Engine): _description_
        tbl_name (str): _description_

    Returns:
        int: _description_
    """
    
    df = sql_to_df( engine, 
                    f"select count(*) as num_rows from {tbl_name}",
                    is_spatial=False)

    return df.num_rows.values[0]



def is_spatial_index_enabled(engine: sqlalchemy.engine.base.Engine,
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

        df = sql_to_df( engine, 
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
    
