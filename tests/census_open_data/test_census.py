import geopandas as gpd
from os.path import isdir, join
from os import makedirs
from shutil import rmtree
import inspect



from geo_py_utils.misc.utils_test import MockTmpCache
from geo_py_utils.census_open_data import census
 



class MockCacheCensus(MockTmpCache):
    """Class to manually implement unittest.patch which never actually works
    Based on MockCache which implements the callable  
    """

    def __init__(self):
         super().__init__()

    @staticmethod
    def get_default_args(func):
        """Extract function default arguments.

        Shamelessly copy pasted from SO: https://stackoverflow.com/questions/12627118/get-a-function-arguments-default-value

        Args:
            func (_type_): func to inspect
        Returns:
            dict: dict with optional params name:values
        """
        signature = inspect.signature(func)
        return {
            k: v.default
            for k, v in signature.parameters.items()
            if v.default is not inspect.Parameter.empty
        }

    def download_compare_cache(self, fct, pattern, **kwargs):

        """ Test underlying function + caching mechanism AND return the cached data.
        
            Run the function we want to test with `fct(path_cache_root = self.mocked_dir, **kwargs)`
            Then inspect the function and check the cache name 
        """

        # Make sure the actual code works
        # `path_cache_root = self.mocked_dir` is the most important bit of code here
        df = fct(path_cache_root = self.mocked_dir, **kwargs)

        # Tests 
        assert df.shape[0] > 0
        assert isinstance(df, gpd.GeoDataFrame)
        
        # Make sure the caching system also works 
        ## Requires reconstructing the arguments used for the cache name  
        dict_cache_params = kwargs.copy()
        fct_default_params = MockCacheCensus.get_default_args(fct)
        for k, v in fct_default_params.items():
            if not k in dict_cache_params.keys() \
                and k != 'path_cache_root' \
                and k != 'data_download_path':
                dict_cache_params.update({k: v})

        # Replace None by null
        if 'new_crs' in dict_cache_params.keys() and dict_cache_params['new_crs'] is None: 
            dict_cache_params['new_crs'] = 'null'

        name_cache = pattern.format(**dict_cache_params)
        path_cache = join(self.mocked_dir, name_cache)
        df_parquet = gpd.read_parquet(path_cache)

        assert df_parquet.shape[0] == df.shape[0]

        return df_parquet


    def download_compare_cache_fsa(self, **kwargs):
        pattern = "fsa_{year}_{pr_code}_carto_{use_cartographic}_crs_{new_crs}.parquet"
        return self.download_compare_cache(census.download_fsas, pattern, **kwargs)

    def download_qc_city_fsa_2016(self, **kwargs):
        return census.download_qc_city_fsa_2016(path_cache_root = self.mocked_dir, **kwargs)

    def download_compare_cache_ca_cmas(self, **kwargs):
        pattern = "ca_cmas_{year}_{pr_code}_carto_{use_cartographic}_crs_{new_crs}_join_{force_spatial_join}.parquet"
        return self.download_compare_cache(census.download_ca_cmas, pattern, **kwargs)

    def download_compare_cache_das(self, **kwargs):
        pattern = "das_{year}_{pr_code}_carto_{use_cartographic}_crs_{new_crs}_join_{force_spatial_join}.parquet"
        return self.download_compare_cache(census.download_das, pattern, **kwargs)


def test_crs_string():

    assert census.get_crs_str(4326) != 'null'
    assert census.get_crs_str(32198) != 'null'

def test_2021_fsa_qc():
    # Quebec - 2021
    with MockCacheCensus() as mocked_cache:
        mocked_cache.download_compare_cache_fsa(year = 2021, pr_code = "24")  

def test_2016_fsa_qc():
    # Quebec - 2016
    with MockCacheCensus() as mocked_cache:
        mocked_cache.download_compare_cache_fsa(year = 2016, pr_code = "24")  

def test_2016_fsa_alberta():
    # Alberta - 2016
    with MockCacheCensus() as mocked_cache:
        mocked_cache.download_compare_cache_fsa(year = 2016, pr_code = "48")  


def test_2016_fsa_qc_city():

    with MockCacheCensus() as mocked_cache:
        df = mocked_cache.download_qc_city_fsa_2016() 
    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] > 0

 
def test_2016_cma_ca_qc():
    
    with MockCacheCensus() as mocked_cache:
        df = mocked_cache.download_compare_cache_ca_cmas(pr_code=24) 


def test_2016_cma_ca_qc_force_spatial_join():
    with MockCacheCensus() as mocked_cache:
        df_spatial = mocked_cache.download_compare_cache_ca_cmas(pr_code=24, force_spatial_join=True) 
        df_str_filter = mocked_cache.download_compare_cache_ca_cmas(pr_code=24, force_spatial_join=False)

    assert df_spatial.shape[0] < df_str_filter.shape[0]

def test_2016_cma_ca_qc_crs():
    with MockCacheCensus() as mocked_cache:
        df = mocked_cache.download_compare_cache_ca_cmas(pr_code=24, new_crs = 4269) 

    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] > 0
    assert df.crs.to_epsg() == 4269

    # see https://gis.stackexchange.com/questions/154740/lat-and-long-extents-of-canadian-provinces
    assert df.centroid.x.min()  >  -80 and df.centroid.x.max() < -57
    assert df.centroid.y.min()  > 44 and df.centroid.x.max() < 63


def test_2021_nb_da ():
    with MockCacheCensus() as mocked_cache:
        df = mocked_cache.download_compare_cache_das(pr_code=13)
 

if __name__ == "__main__":
    test_2021_fsa_qc()
    #test_2021_fsa_qc()
    #test_2016_cma_ca_qc()
    #test_crs_string()