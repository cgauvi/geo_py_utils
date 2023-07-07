import geopandas as gpd
from os.path import isdir, join
from os import makedirs
from shutil import rmtree

from geo_py_utils.misc.utils_test import create_mock_cache_dir
from geo_py_utils.census_open_data import census
 

DEFAULT_PARAMS={
    'use_cartographic': census.USE_CARTOGRAPHIC,
    'crs_str': "null"
}


class MockCache:
    """Class to manually implement unittest.patch which never actually works

    Create a tmp mock cache dir at init and remove it at teardown
    """

    def __init__(self):
        self.mocked_dir = create_mock_cache_dir()
        
    def __enter__(self):
        if not isdir(self.mocked_dir): makedirs(self.mocked_dir)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if isdir(self.mocked_dir): rmtree(self.mocked_dir)
    
    def download_compare(self, **kwargs):
        df = census.download_fsas(path_cache_root = self.mocked_dir, **kwargs)
        assert df.shape[0] > 0
        assert isinstance(df, gpd.GeoDataFrame)
        
        # Make sure the caching system works 
        dict_cache_params = kwargs.copy()
        for k, v in DEFAULT_PARAMS.items():
            if not k in dict_cache_params.keys():
                dict_cache_params.update({k: v})

        name_cache = "fsa_{year}_{pr_code}_carto_{use_cartographic}_crs_{crs_str}.parquet".format(**dict_cache_params)
        path_cache = join(self.mocked_dir, name_cache)
        df_parquet = gpd.read_parquet(path_cache)

        assert df_parquet.shape[0] == df.shape[0]

    def download_qc_city_fsa_2016(self, **kwargs):
        return census.download_qc_city_fsa_2016(path_cache_root = self.mocked_dir, **kwargs)

    def download_ca_cmas(self, **kwargs):
        return census.download_ca_cmas(path_cache_root = self.mocked_dir,**kwargs)

    def download_das(self, **kwargs):
        return census.download_das(path_cache_root = self.mocked_dir,**kwargs)


def test_crs_string():

    assert census.get_crs_str(4326) != 'null'
    assert census.get_crs_str(32198) != 'null'

def test_2021_fsa_qc():
    # Quebec - 2021
    with MockCache() as mocked_cache:
        mocked_cache.download_compare(year = 2021, pr_code = "24")  

def test_2016_fsa_qc():
    # Quebec - 2016
    with MockCache() as mocked_cache:
        mocked_cache.download_compare(year = 2016, pr_code = "24")  

def test_2016_fsa_alberta():
    # Alberta - 2016
    with MockCache() as mocked_cache:
        mocked_cache.download_compare(year = 2016, pr_code = "48")  


def test_2016_fsa_qc_city():

    with MockCache() as mocked_cache:
        df = mocked_cache.download_qc_city_fsa_2016() 
    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] > 0

 
def test_2016_cma_ca_qc():
    
    with MockCache() as mocked_cache:
        df = mocked_cache.download_ca_cmas(pr_code=24) 
    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] > 0

def test_2016_cma_ca_qc_force_spatial_join():
    with MockCache() as mocked_cache:
        df_spatial = mocked_cache.download_ca_cmas(pr_code=24, force_spatial_join=True) 
        df_str_filter = mocked_cache.download_ca_cmas(pr_code=24, force_spatial_join=False)

    assert df_spatial.shape[0] < df_str_filter.shape[0]

def test_2016_cma_ca_qc_crs():
    with MockCache() as mocked_cache:
        df = mocked_cache.download_ca_cmas(pr_code=24, new_crs = 4269) 

    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] > 0
    assert df.crs.to_epsg() == 4269

    # see https://gis.stackexchange.com/questions/154740/lat-and-long-extents-of-canadian-provinces
    assert df.centroid.x.min()  >  -80 and df.centroid.x.max() < -57
    assert df.centroid.y.min()  > 44 and df.centroid.x.max() < 63


def test_2021_nb_da ():
    with MockCache() as mocked_cache:
        df = mocked_cache.download_das(pr_code=13)
    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] > 0

if __name__ == "__main__":
    test_2021_fsa_qc()
    #test_2021_fsa_qc()
    #test_2016_cma_ca_qc()
    #test_crs_string()