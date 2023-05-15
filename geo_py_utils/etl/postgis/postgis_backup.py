

import dotenv
import os
import logging
import sys
from pathlib import Path
import pandas as pd
import subprocess
from abc import ABC, abstractmethod

from geo_py_utils.etl.postgis.postgis_tables import PostGISDBTablesIdentifier, PostGISDBPublicTablesIdentifier
from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
from geo_py_utils.etl.postgis.db_utils import pg_list_tables, pg_create_ogr2ogr_str, pg_create_engine


# Logger
logger = logging.getLogger(__file__)


class PostGISDBBackup(ABC):

    def __init__(self, pg_tables_identifier: PostGISDBTablesIdentifier):

        self.pg_tables_identifier = pg_tables_identifier
        conn = self.pg_tables_identifier.get_pg_connection()
        self.ogr2ogr_src_connection_str = pg_create_ogr2ogr_str(**conn.get_credentials())


    @abstractmethod
    def postgis_to_gpkg(self):
        pass 

    @abstractmethod
    def gpkg_to_postgis(self):
        pass 



class PostGISDBBackupGPK(PostGISDBBackup):
    """Backup a postgis DB to a local gpkg.

        Wrapper over ogr2ogr. 

        Args:
            dest_gpkg (Path): _description_
            pg_tables_identifier (PostGISDBTablesIdentifier): _description_
            overwrite (bool, optional): delete any pre existent db. Defaults to True.
            promote_to_multi (bool, optional): to limit error by casting e.g polygon to multipolygon. Defaults to False.
            keep_col_name_case (bool, true): if True, will keep the original variable case - e.g. GEOMETRY. Otherwise, all get converted to lower case. Defaults to True
    """

    def __init__(
        self,
        dest_gpkg: Path, 
        pg_tables_identifier: PostGISDBTablesIdentifier,
        overwrite: bool = True,
        promote_to_multi: bool = False,
        keep_col_name_case=True
        ):


        super().__init__(pg_tables_identifier)

        self.dest_gpkg = dest_gpkg
        self.overwrite = overwrite
        self.promote_to_multi = promote_to_multi
        self.keep_col_name_case = keep_col_name_case

        if self.overwrite and os.path.exists(dest_gpkg):
            logger.info(f'Removing {dest_gpkg}')
            os.unlink(dest_gpkg)


    def gpkg_to_postgis(self, pg_conn: PostGISDBConnection = None, overwrite_pg_tbl = False):
        """(Re) load a local gpkg to a postgis DB.

        Args:
            pg_conn (PostGISDBConnection, optional): Connection object representing the target. Defaults to None. In which case the original DB is used - self.ogr2ogr_src_connection_str
            overwrite_pg_tbl (bool, optional): _description_. Defaults to False.
        """

        # Point to the correct DB
        if pg_conn is not None:
            new_creds = pg_conn.get_credentials()
            logger.info(f"Reloading gpkg to different DB: {new_creds['host']} - {new_creds['database']}..")
            self.ogr2ogr_dest_connection_str = pg_create_ogr2ogr_str(**new_creds)
        else:
            logger.info(f"Reloading gpkg to same original DB..")
            self.ogr2ogr_dest_connection_str = self.ogr2ogr_src_connection_str

        # Actual system call
        cmd_pg_gpkg = self._get_common_cmd_gpkg_to_pg(overwrite_pg_tbl = overwrite_pg_tbl)
        subprocess.check_call(cmd_pg_gpkg, shell=True)  

        logger.info(f"Successfully completed reload of gpkg -> postgis!")


    def _get_common_cmd_gpkg_to_pg(self, overwrite_pg_tbl = False) -> str:

        """Build the ogr2ogr string for gpkg -> postgis
        
        Args:
            overwrite_pg_tbl (bool, optional): _description_. Defaults to False.
        """

        # Basic commands
        cmd = "ogr2ogr  " \
            ' -progress ' \
            ' --config PG_USE_COPY YES ' 

        # ogr2ogr format: Dest Source
        # See https://gdal.org/programs/ogr2ogr.html
        cmd += fr" {self.ogr2ogr_dest_connection_str} {self.dest_gpkg}  " 

        # Additional commands 
        cmd += " -lco ENCODING=UTF-8 " 

        # Should fail if overwrite_pg_tbl False and the table exists
        if overwrite_pg_tbl:
            cmd += ' -overwrite '

        if self.keep_col_name_case:
            cmd += " -lco LAUNDER=NO "

        return cmd
    

    def postgis_to_gpkg(self):
        """Dump a postgis DB to a local gpkg 

        Two options:
        
        1. Either backs up the entire db at once using a single ogr2ogr command 
        2. Sequentially (cant parallelize this) runs ogr2ogr commands with `append` for each table to add to the db

        """

        if (self.pg_tables_identifier.selected_tables is None):
            logger.info("No tables selected -> extracting ALL tables")
            self._backup_all()
        else :
            logger.info("Extracting only select tables")
            self._backup_select_tables()

        
        logger.info(f'Successfully backed up all tables!')



    def _get_common_cmd_pg_to_gpkg(self) -> str:
        """Build the ogr2ogr string for pg -> gpkg

        Returns:
            str:  ogr2ogr string to call with e.g. `subprocess.check_call`
        """

        # Basic commands
        cmd = "ogr2ogr  " \
            ' -progress ' \
            ' --config PG_USE_COPY YES ' 

        # ogr2ogr format: Dest Source
        # See https://gdal.org/programs/ogr2ogr.html
        cmd += fr" {self.dest_gpkg} {self.ogr2ogr_src_connection_str} " 

        # Additional commands 
        cmd += " -lco ENCODING=UTF-8 " 

        if self.promote_to_multi:
            cmd += " -nlt PROMOTE_TO_MULTI "
        
        return cmd


    def _backup_select_tables(self):
        """Builds the ogr2ogr str + calls it for each table
        """

        cmd = self._get_common_cmd_pg_to_gpkg()

        # Make sure we append new tables to the existing file/db
        cmd += " -append"

        df_tables = self.pg_tables_identifier.get_df_tables()
        for idx, row in df_tables.iterrows():

            cmd_tbl = cmd + f" {row['tablename']} "

            # Actual system call
            logger.info(f"""
                        Backuping table {row['tablename']}... 
                        Running {cmd_tbl}
                        """)
            subprocess.check_call(cmd_tbl, shell=True)  


    def _backup_all(self):
        """Builds the ogr2ogr str + calls it ONCE
        """

        cmd = self._get_common_cmd_pg_to_gpkg()

        # Actual system call
        logger.info(cmd)
        subprocess.check_call(cmd, shell=True)  



if __name__ == '__main__':

    gpkg_to_postgis