
import dotenv
import geopandas as gpd
import logging
import numpy as np
from os.path import isfile
from os import remove
from pathlib import Path

from geo_py_utils.etl.snowflake.snowflake_connect import connnect_snowflake_ext_browser
from geo_py_utils.etl.snowflake.db_utils import table_exists


logger = logging.getLogger(__file__)

DEFAULT_LIST_NA_STRINGS = "'\\N', 'NULL', 'NUL', '', 'None' , 'NA', 'N/A'"

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
            **sfkl_conn_params
            ):

        self.shp = shp

        self.sfkl_name = sfkl_name

        self.local_tmp_file_dir = local_tmp_file_dir # could be a tmpfile() also
        self.local_tmp_file_path = Path(self.local_tmp_file_dir) / self.sfkl_name
        self.keep_tmp_local_files = keep_tmp_local_files
        self.na_strings = na_strings
        self.overwrite_tbl = overwrite_tbl

        self.sfkl_conn_params = sfkl_conn_params
        
        if not isfile(path_env_file):
            raise FileNotFoundError(f'Fatal error! Provide a valid credential .env file to connect to snowflake using the external browser and sso@')
        
        dotenv.load_dotenv(path_env_file , override=True)

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
        self._clean_staging()
        if self.overwrite_tbl:
            self._clean_table()
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        if self.keep_tmp_local_files:
            self._safe_delete_all_files()
        else:
            logger.warning("Warning! not deleting temprary download files! make sure to not clog your system")

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


        logger.info(f'Successfully wrote table {str(self.sfkl_name).upper()} to snowflake')