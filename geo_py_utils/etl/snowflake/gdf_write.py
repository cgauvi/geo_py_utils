import geopandas as gpd
import logging
import numpy as np
from os.path import isfile
from os import remove
from pathlib import Path
from snowflake.connector.pandas_tools import write_pandas 
from unidecode import unidecode

from geo_py_utils.etl.snowflake.db_utils import table_exists
from geo_py_utils.etl.snowflake.snowflake_abstract_connection import SnowflakeAbstractConnection

logger = logging.getLogger(__file__)

DEFAULT_LIST_NA_STRINGS = "'\\N', 'NULL', 'NUL', '', 'None' , 'NA', 'N/A'"
DEFAULT_SFKL_GEO_COL_NAME = "GEOGRAPHY"
DEFAULT_MAX_ROWS_FOR_TYPE_INF = 100

class SnowflakeGeoWriter:
    """SnowflakeGeoWriter: class to upload a GeoDataFrame to Snowflake by creating a temp csv file which is then uploaded to a temp staging area.

    Uses SSO connection so cannot be used or tested automatically.
    Final sfkl table has a GEOGRAPHY column in 4326 
    Writes to M_DATA_SCIENCE.SANDBOX by default, but can be changed with `sfkl_connector` params
    Context manager useful to clean up temp files and staging 

    Attributes:
        shp (gpd.GeoDataFrame): gdf to upload
        sfkl_name (str): table name to create in Snowflake
        sfkl_connector (SnowflakeAbstractConnection):  connector object that implements get_connection() which returns a snowflake.connector.connection object 
        local_tmp_file_dir (_type_, optional): Path where the temp local files are written. Defaults to Path("").
        keep_tmp_local_files (bool, optional): Keep the local files?. Defaults to False.
        na_strings (_type_, optional): For format creation. Tweak only if NAs are custom coded in a weird way. Defaults to DEFAULT_LIST_NA_STRINGS.
        overwrite_tbl (bool, optional): Overwrite the Snowflake table. Defaults to True.
        sfkl_geo_col_name (_type_, optional): Name of geography column in new Snowflake table. Defaults to DEFAULT_SFKL_GEO_COL_NAME.


    Raises:
        TypeError: if not a geodf
        ValueError: if geodf has 1 column or less

    Usage:

        with SnowflakeGeoWriter(
            shp = SAMPLE_GDF,
            sfkl_name = SFKL_TBL_NAME,
            path_env_file = path_config
        ) as sfkl_writer:
            
            sfkl_writer.create_table()
    """
    def __init__(
            self, 
            shp: gpd.GeoDataFrame,
            sfkl_name: str,
            sfkl_connector: SnowflakeAbstractConnection,
            local_tmp_file_dir = Path("") ,
            keep_tmp_local_files = False,
            na_strings = DEFAULT_LIST_NA_STRINGS,
            overwrite_tbl = True,
            sfkl_geo_col_name = DEFAULT_SFKL_GEO_COL_NAME,
            max_rows_for_auto_type_inference = DEFAULT_MAX_ROWS_FOR_TYPE_INF
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

        self.max_rows_for_auto_type_inference = max_rows_for_auto_type_inference

        self.sfkl_connector = sfkl_connector

        self._init()

    def _init(self):
        
        self._qa_input_gdf()
        self._clean_up_gdf()
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

    def _clean_up_gdf(self):
        # CRS
        logger.info('Converting crs to 4326 to simplify snowflake upload!')
        self.shp = self.shp.to_crs(4326) # important to convert to geographic since TO_GEOGRAPHY is hardcoded below

        # Watch out for accents 
        str_cols = self.shp.columns[ self.shp.dtypes == 'object' ].values
        str_cols_join = ",".join(str_cols)
        logger.info(f'Converting strings to characters for: {str_cols_join}')
        for col in str_cols:
            self.shp[col] = self.shp.apply(lambda row : unidecode(str(row[col])), axis=1)

        # Upper case (except the geometry column whose name is set by the ogr2ogr driver in _put_local_file )
        logger.info('Converting column names to upper case')
        self.shp.columns = [x.upper() if x != self.geom_col_name else x for x in self.shp.columns.values ]
 

    def _clean_table(self):
        with self.sfkl_connector as connector:
            conn = connector.get_connection()
            tbl_exists = table_exists(conn, self.sfkl_name)

            if tbl_exists:
                logger.info(f"Table {self.sfkl_name} already exists -> deleting")
                conn.cursor().execute(f"DROP TABLE {self.sfkl_name}")
            else:
                logger.info(f"Table {self.sfkl_name} does not exist")

    def _clean_staging(self):
        with self.sfkl_connector as connector:
            conn = connector.get_connection()
            conn.cursor().execute(f"DROP STAGE IF EXISTS {self.sfkl_name}")
    
    def _create_staging(self):
        with self.sfkl_connector as connector:
            conn = connector.get_connection()
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

        with self.sfkl_connector as connector:
            conn = connector.get_connection()
            conn.cursor().execute(f"put file://{self.local_tmp_file_path}.csv @{self.sfkl_name};")

        if not self.keep_tmp_local_files:
            self._remove_tmp_files()


    def _create_empty_table(self):

        # only a subset (faster) + NO GEOM (type never automatically infered correctly)
        num_rows_safe = min(self.max_rows_for_auto_type_inference, self.shp.shape[0])
        data_subset_for_auto_type_inference = self.shp.\
            iloc[:num_rows_safe].\
            drop(columns=self.geom_col_name)

        with self.sfkl_connector as connector:
            conn = connector.get_connection()
            write_pandas(
                conn=conn,  
                df=data_subset_for_auto_type_inference, 
                table_name=self.sfkl_name, 
                schema=conn.schema, 
                database=conn.database, 
                overwrite=True
            )
            conn.cursor().execute(f"TRUNCATE table {self.sfkl_name} ") # flush the table 
            conn.cursor().execute(f"ALTER TABLE {self.sfkl_name} ADD COLUMN {self.sfkl_geo_col_name} GEOGRAPHY")  # add the GEOGRAPHY


    def create_table(self):
        """
        Main function used to transfer a gpd.GeoDataFrame to snowflake by creatng a tmp local csv and uploading it to a tmp staging area on snowflake
        """

        # Check if table already exists
        with self.sfkl_connector as connector:
            conn = connector.get_connection()
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

        with self.sfkl_connector as connector:
            conn = connector.get_connection()
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
        
        with self.sfkl_connector as connector:
            conn = connector.get_connection()
            conn.cursor().execute(query_insert_into) 


        logger.info(f'Successfully wrote table {self.sfkl_name} to snowflake')

 


