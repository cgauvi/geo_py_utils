
from os.path import exists
import logging

from geo_py_utils.etl.db_etl import Url_to_postgis
from geo_py_utils.etl.spatialite.db_utils import list_tables

# Logger
logger = logging.getLogger(__file__)


class LoadSqlitePostgis:

    def __init__(self,
                sqlite_tbl_name,
                postgis_tbl_name,
                sqlite_db_name,
                postgis_db_name,
                host,
                port,
                user,
                password,
                schema='public',
                overwrite=True,
                promote_to_multi=False):

        self.sqlite_tbl_name = sqlite_tbl_name
        self.sqlite_db_name = sqlite_db_name

        self.postgis_tbl_name = postgis_tbl_name
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.postgis_db_name = postgis_db_name
        self.schema = schema
 
        self.overwrite = overwrite
        self.promote_to_multi = promote_to_multi
 
    
    def upload_url_to_database(self):
        """Extract spatialite table to postgis
        """

        # QA
        assert exists(self.sqlite_db_name)
        assert self.sqlite_tbl_name in list_tables(self.sqlite_db_name)
   
        # Call ogr2ogr
        # No `With` to avoid deleting the db .. -> there is an automatic cleanup at object deletion 
        postgis_etl = Url_to_postgis(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            schema=self.schema,
            db_name=self.postgis_db_name,
            table_name=self.postgis_tbl_name,
            download_url=None,
            src_spatialite_tbl_name=self.sqlite_tbl_name,
            overwrite=self.overwrite,
            promote_to_multi=self.promote_to_multi,
            download_destination=None) 

        #https://gis.stackexchange.com/questions/110818/moving-spatialite-table-into-postgresql-using-ogr2ogr
        # Feed in the sqlite DB + table name as ogr2ogr `source`
        postgis_etl.path_src_to_upload = self.sqlite_db_name
        postgis_etl._ogr2gr()
 

        logger.info(f"Successfully uploaded table {self.sqlite_tbl_name} from sqlite to {self.postgis_tbl_name} on {self.host}:{self.port}/{self.postgis_db_name}")


 