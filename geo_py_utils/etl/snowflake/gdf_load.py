
import geopandas as gpd
import pandas as pd
import re
from shapely import wkt
from snowflake.connector.connection import SnowflakeConnection

from geo_py_utils.etl.snowflake.db_utils import (
    sfkl_get_table_srid,
    sfkl_show_column,

) 
 

def sfkl_to_gpd(query:str, 
                conn: SnowflakeConnection, 
                tbl_name: str,
                geometry_name: str ='GEOMETRY',
                chunksize :int = 100 ) -> gpd.GeoDataFrame :

    """Run a query against snowflake on a table with geometry column and return results as geodataframe

    Args:
        query (str): sql query
        conn (SnowflakeConnection): Snowflake connection
        geometry_name (str, optional): name of geometry column. Defaults to 'GEOMETRY'.
        chunksize (int): number of records to fetch each time - might help avoid time outs for large datasets
    Returns:
        gpd.GeoDataFrame: Result of query - with geometry column
    """

    # Important to set the output format as WKT before we query - otherwise defaults to json and everything will crash
    conn.cursor().execute("ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT  = 'WKT'") 

    
    # Remove the first part and add in the ST_ASWKT() section 
    assert re.search('.*select.*',query, flags=re.IGNORECASE) is not None , 'Fatal error, invalid query without a SELECT clause'
    query_no_select = re.sub('select', '', query, count=1,flags=re.IGNORECASE)

    # Make sure geometry is read in as wkt (and not wkb or geojson)
    query_wrapped = f"""
        SELECT ST_ASWKT({geometry_name}) as GEOMETRY_WKT, {query_no_select}
    """

    # Read in the results
    data_gen = pd.read_sql(query_wrapped, conn, chunksize=chunksize)
    df_snowflake_results = pd.concat(data_gen, ignore_index=True) 

    # Remove geometry column if it exists
    if geometry_name in df_snowflake_results.columns :
        df_snowflake_results.drop(columns=[geometry_name],inplace=True)

    # Rename
    df_snowflake_results.rename(columns={'GEOMETRY_WKT':geometry_name},inplace=True)

    # Reload geometry
    geom =  [wkt.loads(g) for g in df_snowflake_results[geometry_name]]
    
    # Convert back to geodataframe 
    srid = sfkl_get_table_srid(conn, tbl_name, geometry_name)
    shp_snowflake_results = gpd.GeoDataFrame(
        df_snowflake_results.drop(columns=[geometry_name]),
        geometry = gpd.GeoSeries(geom),
        crs = srid
        )

    return shp_snowflake_results
 


if __name__ == '__main__':
    import dotenv
    from os.path import exists

    from geo_py_utils.etl.snowflake.snowflake_ext_browser_connection import SnowflakeExternalBrowserConnection
    from geo_py_utils.constants import ProjectPaths

    from geo_py_utils.etl.snowflake.db_utils    import (
        sfkl_show_column,
        sfkl_get_specific_col_types
    ) 
 

    TBL_NAME = "TEST_SHP_WRITE_SFKL"
    PATH_CREDENTIAL_FILES_SFKL = ProjectPaths.PATH_PROJET_ROOT / '..' / 'config' / 'snowflake' / '.env'
    assert exists(PATH_CREDENTIAL_FILES_SFKL)
    
    with SnowflakeExternalBrowserConnection(PATH_CREDENTIAL_FILES_SFKL).get_connection() as conn:
        df_results = sfkl_show_column(conn, TBL_NAME)

        df_results_subset = sfkl_get_specific_col_types(conn, TBL_NAME, ['ID', 'GEO_TEST'])

        try:
            sfkl_get_specific_col_types(conn, TBL_NAME, ['ID', 'GEOGRAPHY'])
        except: 
            print('missing cols')

        