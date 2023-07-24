

import logging
import pandas as pd
from typing import List
from sqlalchemy import text

from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection

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

    def get_df_geometry_columns(self, exclude_views=True) -> pd.DataFrame:
        """Get a df with details on geometry columns in db
        Returns:
            pd.DataFrame: dataframe with tablename, schemaname columns
        """

        query = """ 
            SELECT 
                f_table_catalog,
                f_table_name tablename,  
                f_table_schema schemaname, 
                f_geometry_column, 
                srid, 
                type
            FROM geometry_columns
            """

        # Schema filter
        if self.selected_schemas is not None:
            schema_str = ", ".join([f"'{str(ss)}'" for ss in self.selected_schemas])
            query += f"""
                WHERE f_table_schema in ({schema_str})
            """

        # Table filter
        if self.selected_tables is not None:
            table_str = ", ".join([f"'{str(st)}'" for st in self.selected_tables])
            query += f"""
                AND f_table_name in ({table_str});
            """
         
        # Read in tables
        engine = self.pg_connect.get_sql_alchemy_engine()
        with engine.connect() as conn:
            df_tables = pd.read_sql(text(query), conn)

        # Anti join on views
        if exclude_views:
            df_views = self.get_df_views(exclude_system_views=False)
            df_merge = df_tables.merge(df_views, how='outer', on = ['tablename','schemaname'], indicator=True)
            df_results= df_merge[(df_merge._merge == 'left_only')].drop('_merge', axis=1)
        else:
            df_results = df_tables

        if not exclude_views:
            logger.info('geom cols can be from views! views not excluded')
        else:
            logger.info('Excluding ALL views from geom cols')

        return df_results


    def get_df_views(self, exclude_system_views=True) -> pd.DataFrame:
        """Get a df with details on views in db
        Returns:
            pd.DataFrame: dataframe with tablename, schemaname columns
        """
        query = '''select table_name tablename , table_schema schemaname
			 from INFORMATION_SCHEMA.views
             '''

        if exclude_system_views or self.selected_schemas is not None:
            query +=  'WHERE '

        string_to_add=''
        if exclude_system_views:
             query += 'table_schema = ANY (current_schemas(false))'
             string_to_add=' AND '

        # Schema filter
        if self.selected_schemas is not None:
            schema_str = ", ".join([f"'{str(ss)}'" for ss in self.selected_schemas])
            query += f"""
                {string_to_add} table_schema in ({schema_str})
            """

        # Read in results
        engine = self.pg_connect.get_sql_alchemy_engine()
        with engine.connect() as conn:
            df_results = pd.read_sql(text(query), conn)

        return df_results


    def get_df_tables(self) -> pd.DataFrame:
        """Get a df with details on tables in db
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
            df_results = pd.read_sql(text(query), conn)

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

    def get_df_geometry_columns(self):
        return super().get_df_geometry_columns()

        


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