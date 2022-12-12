
import sqlite3
import geopandas as gpd
import pandas as pd
from typing import Union
from pathlib import Path
import logging


logger = logging.getLogger(__file__)

def spatialite_db_to_gdf(db_name : Union[str, Path],
                       tbl_name: str,
                       additional_where_limit_causes: str = None) -> gpd.GeoDataFrame:

    """Read a tbl from a spatialite db into memory as a GeoDataFrame.

    Args:
        db_name (Union[str, Path]): db name 
        tbl_name (str): table name
        additional_where_limit_causes (str) : additional clauses to add - e.g. LIMIT 10 or WHERE ID < 4 LIMIT 10
    Returns:
       gpd.GeoDataFrame  : table result

    """

    with sqlite3.connect(db_name) as con:

        # sqlite connection with extensions
        con.enable_load_extension(True)
        con.load_extension("mod_spatialite")
            
        # select everything, but convert the geometry to binary
        query = "SELECT *, Hex(AsBinary(GEOMETRY)) as geometry " \
                f"FROM {tbl_name}"

        if additional_where_limit_causes is not None:
            query += f" {additional_where_limit_causes}" # add a space just in case

        logger.info(f"Reading {db_name}\n Using query {query}")

        # read with geo
        df = gpd.read_postgis(sql = query,
                                con = con, 
                                geom_col = 'geometry' )

        # Drop the useless hex geometry
        df = df.drop(columns=['GEOMETRY'])

        # Try to assign the correct CRS by reading back the `geometry_columns`
        # Fuck it, if this fails we can set it manually later
        try: 
            df_geometry = pd.read_sql(f"select * from geometry_columns where f_table_name = '{tbl_name}' ", con) 
            if df_geometry.shape[0] == 1:
                srid = df_geometry.srid.values[0]
                df_crs = pd.read_sql(f'select * from spatial_ref_sys where srid = {srid}', con)
                crs = df_crs.srtext.values[0]# use the WKT2 proj representation: best practice 
                df = df.set_crs(crs)
                logger.info("Successfully managed to set the crs when loading back to geopandas")
            else:
                logger.warning("Warning couldnt set the crs when loading back to geopandas -> perhaps `geometry_columns` not written yet")
        except Exception as e:
            logger.error(f"Error setting the CRS when loading back the data from spatialite")

    return df




if __name__ == "__main__":

    import os
    from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
    from geo_py_utils.misc.constants import DATA_DIR

    
    df = spatialite_db_to_gdf(db_name = os.path.join(DATA_DIR, "test.db"),
                            tbl_name = "qc_city_test_tbl",
                            additional_where_limit_causes = "LIMIT 10"
    ) 