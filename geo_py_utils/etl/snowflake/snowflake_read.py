import pandas as pd
import geopandas as gpd
import re

from shapely import wkt
from snowflake.connector.connection import SnowflakeConnection

 
def sfkl_get_table_srid(conn: SnowflakeConnection, 
                        tbl_name: str,
                        geometry_name :str) -> int:
                
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
 


 