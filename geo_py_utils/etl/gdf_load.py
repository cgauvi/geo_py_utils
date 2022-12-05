
import sqlite3
import geopandas as gpd
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

    return df


if __name__ == "__main__":

    import os
    from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
    from geo_py_utils.misc.constants import DATA_DIR

    
    df = spatialite_db_to_gdf(db_name = os.path.join(DATA_DIR, "test.db"),
                            tbl_name = "qc_city_test_tbl",
                            additional_where_limit_causes = "LIMIT 10"
    ) 