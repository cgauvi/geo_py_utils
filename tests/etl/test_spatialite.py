from os.path import join
import geopandas as gpd
import os
import tempfile
import numpy as np
from pathlib import Path
import pytest
import sys

from geo_py_utils.etl.db_etl import  Url_to_spatialite
from geo_py_utils.etl.spatialite.gdf_load import spatialite_db_to_gdf
from geo_py_utils.etl.spatialite.db_utils import (
    list_tables, 
    sql_to_df, 
    get_table_rows,
    get_table_crs,
    drop_table,
    drop_geo_table_all,
    is_spatial_index_enabled,
    is_spatial_index_enabled_valid,
    get_table_geometry_type,
    promote_multi,
    rename_columns,
    rename_geom_column,
    get_table_geometries_name,
    get_table_column_names,
    execute_spatialite_cmd,
    remove_accents_column

)
from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.census_open_data.open_data import DEFAULT_QC_CITY_NEIGH_URL
from geo_py_utils.census_open_data.census import FSA_2016_URL
from geo_py_utils.utilities_testing.spatialite_io import upload_qc_neigh_test_db, create_local_spatialite
from geo_py_utils.utilities_testing.data import QcCityTestData, LocalGDFData

 

def setup_qc_neigh():
    
    if os.path.exists(QcCityTestData.SPATIAL_LITE_DB_PATH):
        os.remove(QcCityTestData.SPATIAL_LITE_DB_PATH)
        
    upload_qc_neigh_test_db(promote_to_multi=False)

    assert os.path.exists(QcCityTestData.SPATIAL_LITE_DB_PATH)
 

def setup_local():
    create_local_spatialite(overwrite=True)
 
 

def test_sqlite_no_apostrophes():

    try:
        with open('test_str.db', 'w') as _:
            pass

        # reate the table
        execute_spatialite_cmd(
            'test_str.db',
            '''
            CREATE TABLE test (
            sometext text not null
            );
            '''
        )

        # Insert 4 types of accent variations 
        list_statements = [
            """insert into test (sometext)
            values ('d''');""",
            """insert into test (sometext)
            values ('d''''');""",
            """insert into test (sometext)
            values ('d’');""",
            """insert into test (sometext)
            values ('d‘'); """
        ]

        for s in list_statements:
            execute_spatialite_cmd('test_str.db',  s  )


        # Remove the accents in place using the built-in `replace` function
        remove_accents_column(  db_name = 'test_str.db', tbl_name= 'test', column_name='sometext' )

        df_results = sql_to_df('test_str.db', 'select * from test')

        assert (df_results.sometext.values == 'd').all()

        if os.path.exists('test_str.db'):
            os.remove('test_str.db')
    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of test_str.db on windows')


def test_execute_spatialite_cmd():

    try:

        # Clean slate
        setup_local()

        # Drop the new geo col if it exists 
        geo_cols = get_table_geometries_name(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)
        drop_geo_table_all(
            LocalGDFData.SPATIAL_LITE_DB_PATH, 
            f'{LocalGDFData.SPATIAL_LITE_TBL_QC}_new', 
            geo_cols[0]
        )

        execute_spatialite_cmd(
            LocalGDFData.SPATIAL_LITE_DB_PATH, 
            f'''
            create table if not exists {LocalGDFData.SPATIAL_LITE_TBL_QC}_new as
            select * from {LocalGDFData.SPATIAL_LITE_TBL_QC} limit 1
            '''
            )

        assert get_table_rows( LocalGDFData.SPATIAL_LITE_DB_PATH, f'{LocalGDFData.SPATIAL_LITE_TBL_QC}_new') == 1

    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {LocalGDFData.SPATIAL_LITE_DB_PATH} on windows')

def test_rename_geo():

    try:
        # Make sure we start from scratch, and dont defacto promot to multipolygon
        setup_local()

        geo_cols = get_table_geometries_name(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)
        rename_geom_column(
            LocalGDFData.SPATIAL_LITE_DB_PATH, 
            LocalGDFData.SPATIAL_LITE_TBL_QC,
            new_geom=geo_cols[0] + 'new'
            )

        assert geo_cols[0] + 'new' in get_table_column_names(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)
        assert geo_cols[0] + 'new' in get_table_geometries_name(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)

        assert geo_cols[0] not in get_table_column_names(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)
        assert geo_cols[0] not in get_table_geometries_name(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)
    
    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {LocalGDFData.SPATIAL_LITE_DB_PATH} on windows')

def test_rename():

    try:
         # Make sure we start from scratch, and dont defacto promot to multipolygon
        setup_local()

        geo_cols = get_table_geometries_name(QcCityTestData.SPATIAL_LITE_DB_PATH, QcCityTestData.SPATIAL_LITE_TBL_QC)
        try:
            rename_columns(QcCityTestData.SPATIAL_LITE_DB_PATH, QcCityTestData.SPATIAL_LITE_TBL_QC, {geo_cols[0]: 'ssGEOMsss'})
        except :
            print("Got error as expected")

        rename_columns(QcCityTestData.SPATIAL_LITE_DB_PATH, QcCityTestData.SPATIAL_LITE_TBL_QC, {'nom': 'new_nom'})
        assert 'new_nom' in get_table_column_names(QcCityTestData.SPATIAL_LITE_DB_PATH, QcCityTestData.SPATIAL_LITE_TBL_QC)
    
    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {QcCityTestData.SPATIAL_LITE_DB_PATH} on windows')

def test_promot_to_multi():

    try:
        # Make sure we start from scratch, and dont defacto promot to multipolygon
        setup_local()

        shp_before = spatialite_db_to_gdf(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)
        
        promote_multi(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)

        shp_after = spatialite_db_to_gdf(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)

        # Column renaming creates a new geom column and so needs to infer type
        geo_cols = get_table_geometries_name(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)
        rename_geom_column(
            LocalGDFData.SPATIAL_LITE_DB_PATH, 
            LocalGDFData.SPATIAL_LITE_TBL_QC,
            new_geom=geo_cols[0] + 'new'
            )
        
        assert geo_cols[0] + 'new' in get_table_column_names(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)

        # The geometry is not properly set in the aux geometry_columns table, but the geometry might still be successfully changed
        assert len(shp_before.geometry.type.unique()) == 2

        assert len(shp_after.geometry.type.unique()) == 1
        assert shp_after.geometry.type.unique() == 'MultiPolygon' 
    
    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {QcCityTestData.SPATIAL_LITE_DB_PATH} on windows')


def test_geometry_type():

    try:

        # Make sure we start from scratch, and dont defacto promot to multipolygon
        setup_qc_neigh()

        with pytest.raises(Exception):
            get_table_geometry_type( 
                QcCityTestData.SPATIAL_LITE_DB_PATH,
                QcCityTestData.SPATIAL_LITE_TBL_QC
            ) == 'POLYGON' # We expect a polygon, but the geometry type is mixed so we get an error
    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {QcCityTestData.SPATIAL_LITE_DB_PATH} on windows')

def test_spatial_index_enabled():

    try:

        setup_local()

        assert is_spatial_index_enabled(
            LocalGDFData.SPATIAL_LITE_DB_PATH,
            LocalGDFData.SPATIAL_LITE_TBL_QC
            )
        
    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {QcCityTestData.SPATIAL_LITE_DB_PATH} on windows')
    

def test_spatial_index_valid():

    try:
        setup_local()

        assert is_spatial_index_enabled_valid(
            LocalGDFData.SPATIAL_LITE_DB_PATH,
            LocalGDFData.SPATIAL_LITE_TBL_QC,
            LocalGDFData.SPATIAL_LITE_TBL_GEOMETRY_COL_NAME
            )
            
    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {QcCityTestData.SPATIAL_LITE_DB_PATH} on windows')

 

def test_read_gdf_spatialite_qc ():

    try:
        setup_local()

        df = spatialite_db_to_gdf(db_name = LocalGDFData.SPATIAL_LITE_DB_PATH,
                                tbl_name = LocalGDFData.SPATIAL_LITE_TBL_QC,
                                additional_where_limit_causes = "LIMIT 1"
        ) 

        assert isinstance(df, gpd.GeoDataFrame)
        assert df.shape[0] == 1
    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {LocalGDFData.SPATIAL_LITE_DB_PATH} on windows')


def test_read_gdf_error_geom ():

    try:
        setup_local()

        with pytest.raises(Exception):
            _ = spatialite_db_to_gdf(db_name = LocalGDFData.SPATIAL_LITE_DB_PATH,
                                    tbl_name = LocalGDFData.SPATIAL_LITE_TBL_QC,
                                    current_geo_col_name='somecolumnthatclearlydoesnotexist',
                                    additional_where_limit_causes = "LIMIT 10"
            ) 
    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {LocalGDFData.SPATIAL_LITE_DB_PATH} on windows')
 


def test_spatialite_rename_cols():

    try:
        setup_local()

        rename_columns(
            LocalGDFData.SPATIAL_LITE_DB_PATH,
            LocalGDFData.SPATIAL_LITE_TBL_QC,
            {'GEOMETRY': 'mishka'}
            )

        df_results = sql_to_df(
            LocalGDFData.SPATIAL_LITE_DB_PATH, 
            f'SELECT * from {LocalGDFData.SPATIAL_LITE_TBL_QC} limit 1'
            )

        assert 'mishka' in df_results.columns
        assert 'GEOMETRY' not in df_results.columns

    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {LocalGDFData.SPATIAL_LITE_DB_PATH} on windows')
    

def test_spatialite_list_tables():

    try:
        setup_local()

        existing_tables = list_tables(LocalGDFData.SPATIAL_LITE_DB_PATH)

        assert LocalGDFData.SPATIAL_LITE_TBL_QC in existing_tables
    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {LocalGDFData.SPATIAL_LITE_DB_PATH} on windows')

def test_spatialite_send_query():
    try:
        setup_local()

        df_results = sql_to_df(LocalGDFData.SPATIAL_LITE_DB_PATH, 
                                f"select count(*) as num_rows from {LocalGDFData.SPATIAL_LITE_TBL_QC}")

        assert get_table_rows(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC) == df_results.num_rows.values[0]
    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {LocalGDFData.SPATIAL_LITE_DB_PATH} on windows')

@pytest.mark.requires_or2ogr
def test_spatialite_zip_with_proj():

    try:

        if os.path.exists(join(DATA_DIR, "test_fsa.db")):
            os.remove(join(DATA_DIR, "test_fsa.db"))

        with Url_to_spatialite(
            db_name = join(DATA_DIR, "test_fsa.db"), 
            table_name = 'geo_fsa_tbl',
            download_url = FSA_2016_URL,
            download_destination = DATA_DIR,
            target_projection= 32198,
            overwrite=True,
            force_download = True) as uploader:

            uploader.upload_url_to_database()

        #Load back into memory using geopandas
        shp_test = spatialite_db_to_gdf(join(DATA_DIR, "test_fsa.db"),
        'geo_fsa_tbl',
        'limit 10'
        )

        assert shp_test.crs == 32198

        # Get the crs using spatialite only
        crs_spatial_lite = get_table_crs(join(DATA_DIR, "test_fsa.db"), 'geo_fsa_tbl', return_srid = True)
        assert crs_spatial_lite == 32198

    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {QcCityTestData.SPATIAL_LITE_DB_PATH} on windows')
 
def test_drop_table():

    try:
        # Make sure table exists initially
        setup_local()

        # Drop table 
        drop_table(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC)

        # Table not present
        assert LocalGDFData.SPATIAL_LITE_TBL_QC not in list_tables(LocalGDFData.SPATIAL_LITE_DB_PATH)

        # Drop ALL
        drop_geo_table_all(LocalGDFData.SPATIAL_LITE_DB_PATH, LocalGDFData.SPATIAL_LITE_TBL_QC, LocalGDFData.SPATIAL_LITE_TBL_GEOMETRY_COL_NAME)
        list_aux_tables_to_drop_suff = ['', '_rowid', '_node', '_parent']
        list_aux_tables_to_drop = [ f"{LocalGDFData.SPATIAL_LITE_TBL_QC}_{LocalGDFData.SPATIAL_LITE_TBL_GEOMETRY_COL_NAME}{suffix};"
                                    for suffix in list_aux_tables_to_drop_suff]
        assert not np.any(np.isin(np.array(list_aux_tables_to_drop), list_tables(LocalGDFData.SPATIAL_LITE_DB_PATH)))

    except PermissionError  as e:
        if 'win' not in sys.platform:
            raise e
        else:
            print(f'Failed: {e} - probably concurrent usage of {LocalGDFData.SPATIAL_LITE_DB_PATH} on windows')

   



if __name__ == '__main__':
    test_execute_spatialite_cmd()