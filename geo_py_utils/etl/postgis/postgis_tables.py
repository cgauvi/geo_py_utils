

import dotenv
import os
import logging
import sys
from pathlib import Path
import pandas as pd
from typing import List
from abc import ABC, abstractmethod

from geo_py_utils.constants import ProjectPaths
from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
from geo_py_utils.etl.postgis.db_utils import pg_list_tables, pg_create_ogr2ogr_str, pg_create_engine

# Logger
logger = logging.getLogger(__file__)


class PostGISDBTablesIdentifier:
    """Get table names from a postgres db (potentiall fltering by schema)

    Args:
        pg_connect (PostGISDBConnection): _description_
        selected_schemas (List[str], optional): _description_. Defaults to None.
        extract_all ( List[str]), optional): Extract all tables. Defaults to None.
    """
        
    def __init__(
                self, 
                pg_connect: PostGISDBConnection,  
                selected_schemas: List[str] = None,
                selected_tables: List[str] = None
                ):

        self.pg_connect = pg_connect
        self.selected_schemas = selected_schemas
        self.selected_tables = selected_tables


    def get_pg_connection(self):
        return self.pg_connect

    def get_df_tables(self) -> pd.DataFrame:
        """Get a df with details on tables in db
        Args:
            selected_schemas (List[str]): List of schemas to consider - e.g. ['public', 'postgistfw']. Defaults to None - in which case considers all schemas 
        Returns:
            pd.DataFrame: dataframe with tablename, schemaname columns
        """

        # Base query 
        query = """
            SELECT tablename, schemaname
            FROM pg_catalog.pg_tables
            """

        # Schema filter
        if self.selected_schemas is None:
            query += """
                WHERE schemaname != 'pg_catalog'
                AND   schemaname != 'information_schema'
            """
        else:
            schema_str = ", ".join([f"'{str(ss)}'" for ss in self.selected_schemas])
            query += f"""
                WHERE schemaname in ({schema_str})
            """

        # Table filter
        if self.selected_tables is not None:
            table_str = ", ".join([f"'{str(st)}'" for st in self.selected_tables])
            query += f"""
                AND tablename in ({table_str});
            """
         

        # Read in results
        engine = self.pg_connect.get_sql_alchemy_engine()
        with engine.connect() as conn:
            df_results = pd.read_sql(query, conn)

        return df_results



class PostGISDBPublicTablesIdentifier(PostGISDBTablesIdentifier):

    def __init__(
                self, 
                pg_connect: PostGISDBConnection, 
                list_tables: List[str] = None
                ):

        super().__init__(pg_connect, selected_schemas=['public'], selected_tables=list_tables)

    def get_df_tables(self):
        return super().get_df_tables()

        


if __name__ == '__main__':

    db_connector = PostGISDBConnection()

    # all public.*
    public_schema_extractor_all = PostGISDBPublicTablesIdentifier(db_connector)
    df_tables = public_schema_extractor_all.get_df_tables()
    print(df_tables.head())

    # filtered
    public_schema_extractor_filtered = PostGISDBPublicTablesIdentifier(db_connector, list_tables=['gic_geo_pc_dups'])
    df_tables = public_schema_extractor_filtered.get_df_tables()
    print(df_tables.head())