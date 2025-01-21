import logging
from pathlib import Path
import pandas as pd
from pyarrow import csv as pa_csv
import pyarrow.dataset as pa_ds

from geo_py_utils.etl.snowflake.db_utils import table_exists, stage_exists
from geo_py_utils.etl.snowflake.snowflake_abstract_connection import SnowflakeAbstractConnection
from geo_py_utils.etl.snowflake.type_conversion import DF_MAP_PA_SFKL_TYPES

logger = logging.getLogger(__file__)

DEFAULT_LIST_NA_STRINGS = "'\\N', 'NULL', 'NUL', '', 'None' , 'NA', 'N/A'"
DEFAULT_FIELD_ENCLOSED = "'\"'"
DEFAULT_SKIP_HEADER = 1

# https://docs.snowflake.com/en/sql-reference/sql/create-file-format
DEFAULT_FORMAT_TYPE_OPTIONS = {
    'SKIP_HEADER': DEFAULT_SKIP_HEADER, 
    'NULL_IF': f'({DEFAULT_LIST_NA_STRINGS})',
    'FIELD_OPTIONALLY_ENCLOSED_BY' : DEFAULT_FIELD_ENCLOSED,
    'ERROR_ON_COLUMN_COUNT_MISMATCH' : 'TRUE'
}

class SnowflakeCsvWriter:
    """Write a csv directly to snowflake by using pyarrow (without pandas)

    Based on `SnowflakeGeoWriter` in https://github.com/LaCapitale/geo_py_utils/blob/master/geo_py_utils/etl/snowflake/gdf_write.py#L229

    Args:
        csv_path (Path): path to csv file
        sfkl_name (str): name of snowflake table
        sfkl_stage(str, optional): name of stage. If none, will try to create a temp stage names sfkl_name. Otherwise, will try to read an existing stage
        sfkl_connector (SnowflakeAbstractConnection): snowflake connection wrapper.
        df_type_map (_type_, optional): dict of pyarrow types to snowflake types for conversion. Defaults to DF_MAP_PA_SFKL_TYPES.
        na_strings (_type_, optional): list of na strings in the csv. Defaults to DEFAULT_LIST_NA_STRINGS.
        overwrite_tbl (bool, optional): overwrite the existing table in that db.schema (given by sfkl_connector). Defaults to True.
    """
    def __init__(
                self, 
                csv_path: Path,
                sfkl_name: str,
                sfkl_connector: SnowflakeAbstractConnection,
                sfkl_stage: str = None, 
                df_type_map = DF_MAP_PA_SFKL_TYPES,
                overwrite_tbl = True,
                format_type_options = DEFAULT_FORMAT_TYPE_OPTIONS
                ):

        self.csv_path = csv_path
        self.sfkl_name = sfkl_name
        self.sfkl_stage_none = sfkl_stage is None
        self.sfkl_stage = sfkl_name if self.sfkl_stage_none else sfkl_stage
        self.sfkl_connector = sfkl_connector
        self.df_type_map= df_type_map
        self.overwrite_tbl = overwrite_tbl
        self.format_type_options = format_type_options

        self._init()

    def _init(self):
        
        # New temp stage to create
        if self.sfkl_stage_none :
            self._clean_staging()
            self._create_staging()
        else:
            connector = self.sfkl_connector
            conn = connector.get_connection()
            if not stage_exists(conn, self.sfkl_stage):
                raise ValueError(f'Fatal error! stage {self.sfkl_stage} does not exist')

        if self.overwrite_tbl:
            self._clean_table()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.sfkl_stage_none is None:
            self._clean_staging()
        self.sfkl_connector.close_connection()

    def _create_empty_table_sql(self):

        # read_csv + get the schema
        df_pa_csv = pa_csv.read_csv(self.csv_path)
        col_types={}
        for field in df_pa_csv.schema:
            col_types[field.name] = field.type

        # create a df of pyarrow types X variable
        df_schema = pd.DataFrame.from_dict(
            col_types, 
            orient='index',
            columns=['pa_type']
            ).\
            reset_index().\
            rename(columns={'index':'var_name'})

        # get the corresponding sfkl types
        df_schema = df_schema.merge(
            self.df_type_map,
            how='left',
            on='pa_type'
            )

        # check for errors
        df_error = df_schema [ df_schema['sfkl_type'].isna() ]

        if df_error.shape[0] > 0:
            var_missing = ','.join(df_error.var_name.values)
            raise RuntimeError(f'Fatal error for {var_missing} - missing types - check dates and other potentially problematic types')
        
        # check for missing column nmaes (e.g. if index is written with pandas without a col index)
        df_error_2 = df_schema.var_name[df_schema.var_name == '']
        
        if df_error_2.shape[0] > 0:
            raise RuntimeError(
                f'''
                Fatal error there are {df_error_2.shape[0]} columns with missing names.
                Perhaps a fuck with pandas IO - error with index
                '''
                )
        
        # convert back to dict
        dict_type_sfkl_var = df_schema.set_index('var_name')['sfkl_type'].to_dict()

        # create the ddl
        data_type_table_creation = ",".join([f"{k} {v}" for k,v in dict_type_sfkl_var.items()])
        query_tbl_creation = f"CREATE OR REPLACE TABLE {self.sfkl_name} ({data_type_table_creation});"

        return query_tbl_creation


    def _create_file_format_str_sql(self):
                
        # Set the required params
        format_keys = list(map(lambda x: x.lower(), self.format_type_options.keys() ))
        if 'type' not in format_keys:
            self.format_type_options.update({'type': 'csv'})
       
        # concat together
        params_str = " ".join(
                [fr'{k} = {v}' for k,v in self.format_type_options.items() ]
        )

        query_creation =f"""
                CREATE OR REPLACE TEMPORARY file format myformat 
                {params_str}
                """
        
        return "".join(str(query_creation).splitlines())


    def _create_insert_into_sql(self):

        # get the number count + extract columns by index
        csv_ds = pa_ds.dataset(self.csv_path, format='csv')

        # 1 based indexing 
        select_cols_str = ",".join( [f't.${ind+1}' for ind in range(len(csv_ds.schema.names))])

        query_insert_into =  (
            f"INSERT INTO {self.sfkl_name} "
            f"SELECT {select_cols_str} "
            f"FROM @{self.sfkl_stage} (file_format => 'myformat') as t; "
        )

        return query_insert_into

    def _create_staging(self):
        connector = self.sfkl_connector
        conn = connector.get_connection()
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE OR REPLACE TEMPORARY stage {self.sfkl_stage}")


    def _clean_table(self):

        conn = self.sfkl_connector.get_connection()
        
        tbl_exists = table_exists(conn, self.sfkl_name)

        if tbl_exists:
            logger.info(f"Table {self.sfkl_name} already exists -> deleting")
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE {self.sfkl_name}")
        else:
            logger.info(f"Table {self.sfkl_name} does not exist")

    def _clean_staging(self):
        connector = self.sfkl_connector
        conn = connector.get_connection()
        with conn.cursor() as cursor:
            cursor.execute(f"DROP STAGE IF EXISTS {self.sfkl_stage}")

    def _upload_local_to_staging(self):

        query_put_local_file = f"put 'file://{self.csv_path.as_posix()}' @{self.sfkl_stage};"

        conn = self.sfkl_connector.get_connection()
        with conn.cursor() as cur:
            cur.execute(query_put_local_file)
        
        logger.info(f'Sucessfully wrote local {self.csv_path} to stage {self.sfkl_stage}')
    

    def _upload_staging_to_table(self):

        query_csv_format = self._create_file_format_str_sql()
        query_insert_into = self._create_insert_into_sql() 

        conn = self.sfkl_connector.get_connection()
        with conn.cursor() as cur:

            cur.execute(query_csv_format)
            logger.info('Sucessfully created csv format')

            cur.execute(query_insert_into)
            logger.info(f'Sucessfully copied staging {self.sfkl_stage} into table {self.sfkl_name}')

 

    def _create_empty_table(self):
        query_empty_table = self._create_empty_table_sql()
        conn = self.sfkl_connector.get_connection()

        with conn.cursor() as cur:
            cur.execute(query_empty_table)

        logger.info(f'Sucessfully created empty table {self.sfkl_name}')


    def create_table(self):
        """Main interface to call to upload the csv
        """

        # Check if table already exists
        conn = self.sfkl_connector.get_connection()
        tbl_exists = table_exists(conn, self.sfkl_name)

        if tbl_exists and not self.overwrite_tbl:
            logger.info(f'Table {self.sfkl_name} already exists! appending .. ')

            df_num=pd.read_sql(  f'select count(*) NUM from {self.sfkl_name}', conn )
            n_before = df_num.NUM.values[0]
        else: 
            logger.info(f'Table {self.sfkl_name} does not exist: creating new table .. ')
            n_before = 0
            self._create_empty_table()

        self._upload_local_to_staging()
        self._upload_staging_to_table()

        if self.sfkl_stage_none:
            self._clean_staging()

        df_num=pd.read_sql(  f'select count(*) NUM from {self.sfkl_name}', conn )
        n_after = df_num.NUM.values[0]

        logger.info(f'Successfully wrote {n_after-n_before} row to table {self.sfkl_name} on snowflake {self.sfkl_connector}')