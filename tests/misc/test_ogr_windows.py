import geopandas as gpd
import pytest
from pathlib import Path, WindowsPath

from geo_py_utils.utilities_testing.data import SAMPLE_GDF
from geo_py_utils.misc.cli_windows import ogr2ogr_windows_wrapper
from geo_py_utils.etl.spatialite.gdf_load import spatialite_db_to_gdf

PATH_READ = Path('samplegdf_in.sqlite')
PATH_WRITE = Path('samplegdf_out.sqlite')
LAYER_NAME='mytest'

def setup():
    if not PATH_READ.exists():
        SAMPLE_GDF.to_file(PATH_READ, driver="SQLite", spatialite=True, layer=LAYER_NAME)

    try:
        PATH_WRITE.unlink(missing_ok=True)    
    except PermissionError as e:
        print('Windows file permission error')


def teardown():
    try:
        PATH_READ.unlink(missing_ok=True)
        PATH_WRITE.unlink(missing_ok=True)    
    except PermissionError as e:
        print('Windows file permission error')

@pytest.mark.windows_specific
def test_windows_ogr():

    if 'win' in sys.platform:
        setup()

        source =  WindowsPath(PATH_READ) 
        dest = WindowsPath(PATH_WRITE)

        cmd = "ogr2ogr " \
                " -progress " \
                " -f 'SQLite' " \
                " -dsco SPATIALITE=YES "
        
        cmd += fr" {dest} {source} " 

        ogr2ogr_windows_wrapper(cmd)

        assert PATH_WRITE.exists()

            
        # Would be nice to check but always getting permisson errors because of the 
        try:
            shp_write= spatialite_db_to_gdf(
                db_name = PATH_WRITE,
                tbl_name = LAYER_NAME, # table name corresponds to first soure spatialite table
                current_geo_col_name='geometry'
            ) 

            assert isinstance(shp_write, gpd.GeoDataFrame)
            assert shp_write.shape[0] == SAMPLE_GDF.shape[0]
        except PermissionError as e:
            print('Windows file permission error')

        teardown()

if __name__ == '__main__':
    test_windows_ogr()
