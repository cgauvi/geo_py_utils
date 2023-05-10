

import dotenv
import os
import logging
import sys
from pathlib import Path
import pandas as pd
import subprocess
from abc import ABC, abstractmethod

from gic.constants import ProjectPaths, GeoConstants
from gic.logger import set_up_logger
from geo_py_utils.etl.postgis.postgis_tables import PostGISDBTablesIdentifier, PostGISDBPublicTablesIdentifier
from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
 

sys.path.append(os.path.join(ProjectPaths.PATH_TO_GIC, 'geo_py_utils')) # add this line to be able to import ths submodule directly  `git submodule add https://github.com/cgauvi/ben_py_utils.git`  (from within GIC_VEXCEL/gic)

from geo_py_utils.etl.postgis.db_utils import pg_list_tables, pg_create_ogr2ogr_str, pg_create_engine


# Logger
logger = logging.getLogger(__file__)




class PostGISDBBackup(ABC):

    def __init__(self, pg_tables_identifier: PostGISDBTablesIdentifier):

        self.pg_tables_identifier = pg_tables_identifier
        conn = self.pg_tables_identifier.get_pg_connection()
        self.ogr2ogr_src_connection_str = pg_create_ogr2ogr_str(**conn.get_credentials())


    @abstractmethod
    def backup_to_destination(self):
        pass 



class PostGISDBBackupGPK(PostGISDBBackup):
    """Backup a postgis DB to a local gpkg.

        Wrapper over ogr2ogr. 

        Args:
            dest_gpkg (Path): _description_
            pg_tables_identifier (PostGISDBTablesIdentifier): _description_
            overwrite (bool, optional): delete any pre existent db. Defaults to True.
            promote_to_multi (bool, optional): to limit error by casting e.g polygon to multipolygon. Defaults to False.
    """

    def __init__(
        self,
        dest_gpkg: Path, 
        pg_tables_identifier: PostGISDBTablesIdentifier,
        overwrite: bool = True,
        promote_to_multi: bool = False
        ):


        super().__init__(pg_tables_identifier)

        self.dest_gpkg = dest_gpkg
        self.overwrite = overwrite
        self.promote_to_multi = promote_to_multi

        if self.overwrite and os.path.exists(dest_gpkg):
            logger.info(f'Removing {dest_gpkg}')
            os.unlink(dest_gpkg)


    def backup_to_destination(self):
        """Main interface. 

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



    def _get_common_cmd(self) -> str:
        """Build the ogr2ogr string 

        Returns:
            str:  ogr2ogr string to call with e.g. `subprocess.check_call`
        """

        # Basic commands
        cmd = "ogr2ogr  " \
            ' -progress ' \
            ' --config PG_USE_COPY YES ' 

        # ogr2ogr format: Dest Source
        # See https://gdal.org/programs/ogr2ogr.html
        cmd += fr" '{self.dest_gpkg}' {self.ogr2ogr_src_connection_str} " 

        # Additional commands 
        cmd += " -lco ENCODING=UTF-8 " 

        if self.promote_to_multi:
            cmd += " -nlt PROMOTE_TO_MULTI "
        
        return cmd


    def _backup_select_tables(self):
        """Builds the ogr2ogr str + calls it for each table
        """

        cmd = self._get_common_cmd()

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

        cmd = self._get_common_cmd()

        # Actual system call
        logger.info(cmd)
        subprocess.check_call(cmd, shell=True)  



