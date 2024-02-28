
import dotenv
import geopandas as gpd
import logging
import numpy as np
from os.path import isfile
from os import remove
from pathlib import Path
from snowflake.connector.pandas_tools import write_pandas 

from geo_py_utils.etl.snowflake.snowflake_connect import connnect_snowflake_ext_browser
from geo_py_utils.etl.snowflake.db_utils import table_exists


logger = logging.getLogger(__file__)

DEFAULT_LIST_NA_STRINGS = "'\\N', 'NULL', 'NUL', '', 'None' , 'NA', 'N/A'"
DEFAULT_SFKL_GEO_COL_NAME = "GEOGRAPHY"

class SnowflakeGeoWriter:

    def __init__(
            self, 
            shp: gpd.GeoDataFrame,
            sfkl_name: str,
            path_env_file: Path = Path(".env"),
            local_tmp_file_dir = Path("") ,
            keep_tmp_local_files = False,
            na_strings = DEFAULT_LIST_NA_STRINGS,
            overwrite_tbl = True,
            sfkl_geo_col_name = DEFAULT_SFKL_GEO_COL_NAME,
            **sfkl_conn_params
            ):

        self.shp = shp
        self.geom_col_name = self.shp.geometry.name # will need to drop and recreate ..
        self.sfkl_geo_col_name = str(sfkl_geo_col_name).upper()

        self.sfkl_name = str(sfkl_name).upper() # safer to convert all to caps

        self.local_tmp_file_dir = local_tmp_file_dir # could be a tmpfile() also
        self.local_tmp_file_path = Path(self.local_tmp_file_dir) / self.sfkl_name
        self.keep_tmp_local_files = keep_tmp_local_files
        self.na_strings = na_strings
        self.overwrite_tbl = overwrite_tbl

        self.sfkl_conn_params = sfkl_conn_params
        
        if not isfile(path_env_file):
            raise FileNotFoundError(f'Fatal error! Provide a valid credential .env file to connect to snowflake using the external browser and sso@')
        
        dotenv.load_dotenv(path_env_file , override=True)

        self._init()

    def _init(self):
        self._qa_input_gdf()
        self._clean_staging()
        self._remove_tmp_files()
        self._create_staging()

        if self.overwrite_tbl:
            self._clean_table()

    def _qa_input_gdf(self):
        if not isinstance(self.shp, gpd.GeoDataFrame):
            raise TypeError('Fatal error not a geodf!')

        if self.shp.shape[1] <= 1:
            raise ValueError('Fatal error gpf contains only a geometry column!')


    def _clean_table(self):
        with connnect_snowflake_ext_browser(**self.sfkl_conn_params) as conn:
            tbl_exists = table_exists(conn, self.sfkl_name)

            if tbl_exists:
                logger.info(f"Table {self.sfkl_name} already exists -> deleting")
                conn.cursor().execute(f"DROP TABLE {self.sfkl_name}")
            else:
                logger.info(f"Table {self.sfkl_name} does not exist")

    def _clean_staging(self):
        with connnect_snowflake_ext_browser(**self.sfkl_conn_params) as conn:
            conn.cursor().execute(f"DROP STAGE IF EXISTS {self.sfkl_name}")
    
    def _create_staging(self):
        with connnect_snowflake_ext_browser(**self.sfkl_conn_params) as conn:
            conn.cursor().execute(f"CREATE OR REPLACE stage {self.sfkl_name}")
    
    def _remove_tmp_files(self):

        local_tmp_file_path_no_ext = Path(self.local_tmp_file_path).stem # remove extension
        ext=['csv','csvt','prj']
        files_with_ext = [f'{local_tmp_file_path_no_ext}.{e}' for e in ext]
        for f in files_with_ext:
            if isfile(f):
                remove(f)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._clean_staging()
        self._remove_tmp_files()

    def _create_cols_str_not_geo(self):

        return ",".join(
            [
                f"REPLACE(t.${col_n}, '\"',  '') "\
                for col_n in np.arange(2, self.shp.shape[1]+1) # 1 based indexing 
            ]
        )

    def _create_file_format_str(self):
            
        return  (
            "CREATE OR REPLACE file format " 
            f"myformat type =  'csv' "
            f"field_delimiter = '\t' "
            f"skip_header = 1 NULL_IF = ({self.na_strings}) "
            "FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' "
            "error_on_column_count_mismatch=false; "
        )

    def _put_local_file(self):

        self._remove_tmp_files()

        self.shp.to_file(
            f"{self.local_tmp_file_path}.csv",
            GEOMETRY="AS_WKT",
            SEPARATOR="TAB",
            GEOMETRY_NAME="GEOMETRY",
            CREATE_CSVT="YES",
            STRING_QUOTING="ALWAYS"
        )

        with connnect_snowflake_ext_browser(**self.sfkl_conn_params) as conn:
            conn.cursor().execute(f"put file://{self.local_tmp_file_path}.csv @{self.sfkl_name};")

        if not self.keep_tmp_local_files:
            self._remove_tmp_files()


    def _create_empty_table(self):

        with connnect_snowflake_ext_browser(**self.sfkl_conn_params) as conn:
            write_pandas(
                conn=conn,  
                df=self.shp.iloc[:0].drop(columns=self.geom_col_name), # only the header + NO GEOM (type not correct)
                table_name=self.sfkl_name, 
                schema=conn.schema, 
                database=conn.database, 
                overwrite=True
            )
            conn.cursor().execute(f"ALTER TABLE {self.sfkl_name} ADD COLUMN {self.sfkl_geo_col_name} GEOGRAPHY") 


    def create_table(self):
        """
        Main function used to transfer a gpd.GeoDataFrame to snowflake by creatng a tmp local csv and uploading it to a tmp staging area on snowflake
        """

        # Check if table already exists
        with connnect_snowflake_ext_browser(**self.sfkl_conn_params) as conn:
            tbl_exists = table_exists(conn, self.sfkl_name)

        if tbl_exists and not self.overwrite_tbl:
            logger.info(f'Table {self.sfkl_name} already exists! Not doing anything')
            return 
        
        # Create the empty table structure
        self._create_empty_table()

        # Write & upload the local file
        self._put_local_file()

        # Set the file format used for the local file + file zipped uploaded to snowflake
        # Essential to set separator, quoting and other details to ensure data is read in correctly
        str_format_query = self._create_file_format_str()

        with connnect_snowflake_ext_browser(**self.sfkl_conn_params) as conn:
            conn.cursor().execute(str_format_query) 
            conn.cursor().execute("ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT  = 'WKT'") 

        # Create the inert into select .. statement based on the custom myformat defined above
        select_cols_str = self._create_cols_str_not_geo()

        # Geometry always in FIRST position using ogr2ogr driver 
        # Quotes are hacky, but ensure the WKT is read in in its entirety
        query_insert_into =  (
            f"INSERT INTO {self.sfkl_name} "
            f"SELECT {select_cols_str}, TO_GEOGRAPHY(REPLACE(t.$1, '\"', '')) "
            f"FROM @{self.sfkl_name} (file_format => 'myformat') as t; "
            )
        
        with connnect_snowflake_ext_browser(**self.sfkl_conn_params) as conn:
            conn.cursor().execute(query_insert_into) 


        logger.info(f'Successfully wrote table {self.sfkl_name} to snowflake')




if __name__ == '__main__':

    import dotenv
    from pathlib import Path
    from os.path import abspath, dirname, exists

    import geopandas as gpd
    from shapely.geometry import Polygon

    from geo_py_utils.etl.snowflake.snowflake_connect import connnect_snowflake_ext_browser
    #from geo_py_utils.misc.utils_test import SAMPLE_GDF
    
    HERE = Path(abspath(dirname(__file__)))  / '..' / '..' / '..'
    SFKL_TBL_NAME = "TEST_SHP_WRITE_SFKL"

    path_config = HERE / "config" / "snowflake" / ".env"
    if not exists(path_config):
        raise FileNotFoundError()
    

    
    SAMPLE_GDF =   gpd.GeoDataFrame(
                    {'id': [0, 1]},
                    geometry=[
                        Polygon(((-71.5, 46.5), (-71.6, 46.4),  (-71.5, 46.7),   (-71.5, 46.5))) ,
                        Polygon(((-71.5, 46.5), (-72, 46.4),  (-71.5, 46.7),   (-71.5, 46.5)))
                    ],
                    crs=4326
                )
    dotenv.load_dotenv(path_config, override= True)

    with SnowflakeGeoWriter(
        shp=SAMPLE_GDF,
        sfkl_name=SFKL_TBL_NAME,
        path_env_file = path_config
    ) as sfkl_writer:
        
        sfkl_writer.create_table()
        
    with connnect_snowflake_ext_browser() as conn:
        tbl_exists = table_exists(conn, SFKL_TBL_NAME)
      
    print(tbl_exists)

    with connnect_snowflake_ext_browser() as conn:
        conn.cursor().execute(f'DROP TABLE {SFKL_TBL_NAME}')