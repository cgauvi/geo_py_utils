import geopandas as gpd
import pandas as pd
from os.path import join

from geo_py_utils.census_open_data.census import download_fsas, download_qc_city_fsa_2016
from geo_py_utils.misc.constants import DATA_DIR


def download_compare(year, pr_code):

    df = download_fsas(year,  pr_code)  
    assert df.shape[0] > 0
    assert  isinstance(df, gpd.GeoDataFrame)
    
    df_parquet = gpd.read_parquet(join(DATA_DIR, "cache",f"qc_das_{year}_{pr_code}.parquet"))
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


if __name__ == "__main__":
    test_2016_fsa_qc_city()