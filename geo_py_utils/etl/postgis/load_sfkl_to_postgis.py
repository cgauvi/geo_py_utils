
from os.path import join, exists
from os import remove, makedirs, environ 
import logging
import geopandas as gpd
import sys

from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.etl.db_etl import Url_to_postgis
from geo_py_utils.etl.snowflake.snowflake_connect import connnect_snowflake_ext_browser
from geo_py_utils.etl.snowflake.snowflake_read import sfkl_to_gpd
from geo_py_utils.etl.spatialite.gdf_load import spatialite_db_to_gdf
 

# Logger
logger = logging.getLogger(__file__)


class LoadSfklPostgis:


    """Transfer a (geo) table from snowflake to postgis.

    Downloads the snowflake table localy + uses ogr2ogr to export to postgis

    Warning: only works with a table with a geometry/geography column on snowflake

    Args:
            sfkl_tbl_name (str): _description_
            postgis_tbl_name (str): _description_
            host (str): _description_
            user (str): _description_
            password (str): _description_
            port (_type_, optional): _description_. Defaults to "5052":str.
            postgis_db_name (_type_, optional): _description_. Defaults to 'gis':str.
            schema (_type_, optional): _description_. Defaults to 'public':str.
            sfkl_geometry_name (_type_, optional): _description_. Defaults to 'GEOGRAPHY':str.
            overwrite (_type_, optional): _description_. Defaults to True:bool.
            promote_to_multi (_type_, optional): _description_. Defaults to False:bool.
    """

    def __init__(self,
                sfkl_tbl_name: str,
                postgis_tbl_name: str,
                host: str,
                user: str,
                password: str,
                port: str="5052",
                postgis_db_name: str='gis',
                schema: str='public',
                sfkl_geometry_name: str='GEOGRAPHY',
                overwrite: bool=True,
                promote_to_multi: bool=False):
 


        self.sfkl_tbl_name = sfkl_tbl_name
        self.sfkl_geometry_name = sfkl_geometry_name

        self.postgis_tbl_name = postgis_tbl_name
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.postgis_db_name = postgis_db_name
        self.schema = schema
 
        self.overwrite = overwrite
        self.promote_to_multi = promote_to_multi

    def _extract_from_sfkl(self) -> gpd.GeoDataFrame:
        """Read in the snowflake table + read in memory as gpd.GeoDf

        Returns:
            gpd.GeoDataFrame: _description_
        """

        # Connect
        con = connnect_snowflake_ext_browser()

        # Fetch the table
        shp_sfkl = sfkl_to_gpd(
            query=f"SELECT * from {self.sfkl_tbl_name}",
            conn=con,  
            tbl_name=self.sfkl_tbl_name,
            geometry_name=self.sfkl_geometry_name
            )

        logger.info(f"""
                    Successfully downloaded table {self.sfkl_tbl_name} 
                    with {shp_sfkl.shape[0]} records from snowflake
                    """)

        return shp_sfkl


    
    def upload_url_to_database(self):
        """Extract geo data stored on snowflake and load into a postgis DB
        """

        # Create the directory to write the object
        dir_dict = join(DATA_DIR, self.sfkl_tbl_name)
        if not exists(dir_dict): 
            makedirs(dir_dict)
        path_shp_file = join(dir_dict, f"{self.sfkl_tbl_name}.gpkg")

        if not exists(path_shp_file):
            # Get the shp file
            shp_role_cleaned = self._extract_from_sfkl()

            # Write to disk
            shp_role_cleaned.to_file(path_shp_file)

        # Call ogr2ogr
        postgis_etl =  Url_to_postgis(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            schema=self.schema,
            db_name=self.postgis_db_name,
            table_name=self.postgis_tbl_name ,
            download_url=path_shp_file,
            overwrite = self.overwrite, 
            promote_to_multi=self.promote_to_multi,
            download_destination=dir_dict)

        postgis_etl.path_src_to_upload = path_shp_file
        postgis_etl._ogr2gr()

        logger.info(f"Successfully uploaded table {self.sfkl_tbl_name} from snowflake to {self.postgis_tbl_name} on {self.host}:{self.port}/{self.postgis_db_name}")

 



 