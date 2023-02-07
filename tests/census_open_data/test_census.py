import geopandas as gpd
import pandas as pd
from os.path import join

from geo_py_utils.census_open_data.census import (
    download_fsas, 
    download_qc_city_fsa_2016, 
    download_ca_cmas, 
    download_das,
    get_crs_str)
    
from geo_py_utils.misc.constants import DATA_DIR


def test_crs_string():

    assert get_crs_str(4326) != 'null'
    assert get_crs_str(32198) != 'null'

def download_compare(year, pr_code, use_cartographic=False):

    df = download_fsas(year,  pr_code, use_cartographic=use_cartographic)  
    assert df.shape[0] > 0
    assert  isinstance(df, gpd.GeoDataFrame)
    
    crs_str = "null"
    df_parquet = gpd.read_parquet(join(DATA_DIR, "cache",f"fsa_{year}_{pr_code}_carto_{use_cartographic}_crs_{crs_str}.parquet"))
    assert df_parquet.shape[0] == df.shape[0]


def test_2016_fsa_qc():
    # Quebec
    download_compare(year = 2016, pr_code = "24")  

def test_2016_fsa_alberta():
    # Alberta
    download_compare(year = 2016, pr_code = "48")  

def test_2016_fsa_qc_city():
    df = download_qc_city_fsa_2016() 
    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] > 0

def test_2016_cma_ca_qc():
    df = download_ca_cmas(pr_code=24) 
    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] > 0

def test_2016_cma_ca_qc_force_spatial_join():
    df_spatial = download_ca_cmas(pr_code=24, force_spatial_join=True) 
    df_str_filter = download_ca_cmas(pr_code=24, force_spatial_join=False) 

    assert df_spatial.shape[0] < df_str_filter.shape[0]

def test_2016_cma_ca_qc_crs():
    df = download_ca_cmas(pr_code=24, new_crs = 4269) 
    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] > 0
    assert df.crs.to_epsg() == 4269

    # see https://gis.stackexchange.com/questions/154740/lat-and-long-extents-of-canadian-provinces
    assert df.centroid.x.min()  >  -80 and df.centroid.x.max() < -57
    assert df.centroid.y.min()  > 44 and df.centroid.x.max() < 63


def test_2021_nb_da ():
    df = download_das(pr_code=13)
    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] > 0

if __name__ == "__main__":
    #test_2016_fsa_qc_city()
    #test_2016_cma_ca_qc()
    test_crs_string()