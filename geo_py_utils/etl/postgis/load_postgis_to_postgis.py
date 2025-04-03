import logging
from sqlalchemy import create_engine

from geo_py_utils.etl.port import is_port_open
from geo_py_utils.etl.postgis.db_utils import pg_list_tables, pg_db_exists
from geo_py_utils.misc.cli_windows import ogr2ogr_windows_wrapper

# Logger
logger = logging.getLogger(__file__)


class LoadPostgis2Postgis:


    """Transfer a (geo) table from postgis to postgis.

        Uses ogr2ogr to export to postgis

        Args:
            src_host (str): _description_
            src_user (str): _description_
            src_password (str): _description_
            src_port (str): _description_
            src_database (str): _description_
            src_defaut_schema (str): _description_
            dest_postgis_tbl_name (str): _description_
            dest_host (str): _description_
            dest_user (str): _description_
            dest_password (str): _description_
            dest_port (str): _description_
            dest_database (str): _description_
            dest_defaut_schema (str): _description_
            tables_to_transfer (_type_, optional): _description_. Defaults to None.
            new_geom_name (str, optional): _description_. Defaults to None.
            overwrite (bool, optional): _description_. Defaults to False.
            append (bool, optional): _description_. Defaults to False.
            promote_to_multi (bool, optional): _description_. Defaults to False.
    """
    
    def __init__(self,
                src_host: str,
                src_user: str,
                src_password: str,
                src_port: str,
                src_database: str,
                dest_host: str,
                dest_user: str,
                dest_password: str,
                dest_port: str,
                dest_database: str,
                dest_default_schema: str = None,
                src_default_schema: str = None,
                tables_to_transfer = None,
                new_geom_name:str = None,
                overwrite: bool=False,
                append: bool = False,
                promote_to_multi: bool=False
                ):
    

 

        self.src_host = src_host
        self.src_port = src_port
        self.src_user = src_user
        self.src_password = src_password
        self.src_database = src_database
        self.src_default_schema = src_default_schema

        self.dest_host = dest_host
        self.dest_port = dest_port
        self.dest_user = dest_user
        self.dest_password = dest_password
        self.dest_database = dest_database
        self.dest_default_schema = dest_default_schema
 
        self.tables_to_transfer = tables_to_transfer
        self.new_geom_name = new_geom_name
        self.overwrite = overwrite
        self.append = append
        self.promote_to_multi = promote_to_multi



        self._init()


    def _init(self):

        if self.overwrite and self.append:
            raise ValueError('Fatal error choose at most 1 of overwrite or append')
        
        # Make sure the ports are open
        for h,p in [(self.dest_host, self.dest_port), (self.src_host, self.src_port)]:
            if not is_port_open(h, p):
                raise RuntimeError(f'Fatal error! Port {p} is not open on {h}')

        # Create the sqlalchemy engines
        self.src_engine = create_engine(f'postgresql://{self.src_user}:{self.src_password}@{self.src_host}:{self.src_port}/{self.src_database}')
        self.dest_engine = create_engine(f'postgresql://{self.dest_user}:{self.dest_password}@{self.dest_host}:{self.dest_port}/{self.dest_database}')

        # Make sure DBs exist
        for engine,db in [ (self.src_engine, self.src_database),  (self.dest_engine, self.dest_database)]:
            if not pg_db_exists(engine, db):
                raise RuntimeError(f'Fatal error! DB {db} does not exist for {engine}')
        
        # Warning message for empty schema
        if not self.src_default_schema:
            logger.warning('Warning! No src schema specified: will consider transfering ALL tables from ALL schemas in the DB')

        # Check which tables we want to transfer
        if self.tables_to_transfer :
                
            if not isinstance(self.tables_to_transfer, list):
                self.tables_to_transfer = [self.tables_to_transfer]

            orig_tables = self.tables_to_transfer
            desired_num_tables_to_transfer = len(self.tables_to_transfer)

            existing_src_tables = pg_list_tables(self.src_engine, self.src_database, self.src_default_schema)

            self.tables_to_transfer = set(self.tables_to_transfer).intersection(set(existing_src_tables)) 
            if  desired_num_tables_to_transfer != len(self.tables_to_transfer):
                logger.warning(f'Warning! requested transfer of {orig_tables} - only {existing_src_tables} exist')
        else:
            self.tables_to_transfer = pg_list_tables(self.src_engine, self.src_database, self.src_default_schema)

        existing_dest_tables = pg_list_tables(self.dest_engine, self.dest_database, self.dest_default_schema)

        # Deal with table conflicts
        potential_tbl_conflict = set(self.tables_to_transfer).intersection(set(existing_dest_tables))
        if len(potential_tbl_conflict):
            logger.warning (f'Warning! Tables {potential_tbl_conflict} are already present in the destination DB')

            if not self.overwrite and not self.append:
                self.tables_to_transfer = set(self.tables_to_transfer).difference(set(potential_tbl_conflict))
                logger.warning (f'Warning! not overwriting or appending: only the new {self.tables_to_transfer} will beconsidered ')
        
        # Cast to set and make sure we don't have system tables 
        self.tables_to_transfer = set(self.tables_to_transfer).difference(set(['spatial_ref_sys']))


    def _ogr2ogr(self):

 
        cmd = "ogr2ogr  " \
            ' -progress ' \
            ' --config PG_USE_COPY YES ' 

        # Dest/Target DB
        cmd += fr"  -f 'PostgreSQL' PG:'host={self.dest_host} port={self.dest_port} dbname={self.dest_database} user={self.dest_user} password={self.dest_password}'" 
            
        # Source DB
        cmd += fr"  PG:'host={self.src_host} port={self.src_port} dbname={self.src_database} user={self.src_user} password={self.src_password}'" 
        
        # List of tables to transfer
        if self.tables_to_transfer and len(self.tables_to_transfer):
            table_str = " ".join(self.tables_to_transfer)
            cmd += f' {table_str} '

        # Schema  
        if self.src_default_schema:
            cmd += f' -oo SCHEMA={self.src_default_schema} '
        if self.dest_default_schema:
            cmd += f' -lco SCHEMA={self.dest_default_schema} '

        # Encoding 
        cmd += " -lco ENCODING=UTF-8 " 

        if self.new_geom_name is not None:
            cmd += f" -lco GEOMETRY_NAME='{self.new_geom_name}'"

        if self.promote_to_multi:
             cmd +=  " -nlt PROMOTE_TO_MULTI "

        # Overwrite or append
        if self.overwrite:
            cmd += " -overwrite"

        if self.append:
            cmd += " -append"


        # Actual ogr2ogr call
        ogr2ogr_windows_wrapper(cmd)

    
    def upload_url_to_database(self):
        """Extract geo data stored on snowflake and load into a postgis DB
        """
 
        self._ogr2ogr()

        logger.info(
            f"""
            Successfully uploaded tables {self.tables_to_transfer} 
            from PG {self.src_engine}
            to PG {self.dest_engine}
            """
        )
 
 