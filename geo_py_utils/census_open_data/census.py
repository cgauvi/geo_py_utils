

""" Functions to download and cache polygon boundaries for select census/canada post regions """

import pandas as pd
import geopandas as gpd
from os.path import join
import logging
import numpy as np

from ben_py_utils.misc.cache import Cache_wrapper

from geo_py_utils.etl.download_zip import download_zip_shp
from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.census_open_data.open_data import download_qc_city_neighborhoods


logger = logging.getLogger(__file__)

# Cartographic is very precise (hence very heavy) whereas digital is coarser and faster to retrieve and process
USE_CARTOGRAPHIC = False

@Cache_wrapper(path_cache = join(DATA_DIR, "cache", "canada_water.parquet"))
def download_water(year = 2011, new_crs = None):
    
    # Download lakes and riers
    if year == 2011:
        url_water_lakes_rivers="https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/ghy_000c11a_e.zip"
        shp_rivers = download_zip_shp(url_water_lakes_rivers) 
    else:
        raise ValueError(f"Fatal error - inputed {year}, but only 2011 implemented")

    # Transform
    if new_crs is not None:
        shp_rivers = shp_rivers.to_crs(new_crs)

    return shp_rivers
    

def download_qc_das(year : int = 2016,
                    pr_code:str = '24',             
                    path_cache_root: str = join(DATA_DIR, "cache"),
                    use_cartographic:bool = USE_CARTOGRAPHIC,
                    new_crs = None)  -> gpd.GeoDataFrame:
                        
    @Cache_wrapper(path_cache = join(path_cache_root, f"qc_das_{year}_{pr_code}.parquet"))
    def download_qc_das_wrappee(year,
                                pr_code,
                                use_cartographic,
                                data_download_path ,
                                new_crs)  :
        """
        download_qc_das Read in the 2021 dissemination areas (DAs) for Province of Quebec

        Args:
            use_cartographic (bool): _description_

        Returns:
            gpd.GeoDataFrame: _description_
        """

        logger.info(f"Saving data to {data_download_path}")

        if year == 2021:
            zip_download_url = "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lda_000b21a_e.zip" \
            if use_cartographic \
            else "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lda_000a21a_e.zip"
        else:
            raise ValueError(f"Fatal error - inputed {year}, but only 2021 implemented")


        shp_das_all = download_zip_shp(zip_download_url,data_download_path)
        shp_qc = download_qc_boundary(use_cartographic,data_download_path)
        
        num_qc_das= np.sum(shp_das_all.DAUID.str[:2] == pr_code)
        shp_das_qc = gpd.sjoin(shp_das_all, shp_qc, how="inner", op="within" )

        assert num_qc_das == shp_das_qc.shape[0], f"Error, there are {shp_das_qc.shape[0]} DAs found by spatial join but {num_qc_das} based on DAUID filtering"
        print(f"There are {shp_das_qc.shape[0]} features/das in {pr_code} for the {year} census")

        # Transform
        if new_crs is not None:
            shp_das_qc = shp_das_qc.to_crs(new_crs)

        return shp_das_qc

    return download_qc_das_wrappee(year,
                                    path_cache_root,
                                    use_cartographic,
                                    new_crs)


def download_fsas(year : int = 2016,
                    pr_code : str = "24",
                    path_cache_root: str = join(DATA_DIR, "cache"),
                    use_cartographic:bool = USE_CARTOGRAPHIC,
                    data_download_path = DATA_DIR,
                    new_crs=None)  -> gpd.GeoDataFrame:
                        
    @Cache_wrapper(path_cache = join(path_cache_root,f"qc_das_{year}_{pr_code}.parquet"))
    def download_fsas_wrappee(year, 
                                pr_code,
                                 use_cartographic,
                                 data_download_path,
                                 new_crs  ) -> gpd.GeoDataFrame:
        """
        download_qc_boundary Read the 2016 FSAs for the province of Quebec

        Args:
            use_cartographic (bool): _description_

        Returns:
            _type_: _description_
        """
        if year == 2016:
            zip_download_url = "https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000a16a_e.zip" \
            if use_cartographic \
            else "https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000b16a_e.zip"
        else:
            raise ValueError(f"Fatal error - inputed {year}, but only 2016 implemented")

        shp_fsa_all = download_zip_shp(zip_download_url, data_download_path)
        shp_fsa_all_filtered = shp_fsa_all.loc[shp_fsa_all.PRUID.astype('str') == pr_code, ]
    
        print(f"There are {shp_fsa_all_filtered.shape[0]} FSAs in {pr_code} for the 2016 census")

        # Transform
        if new_crs is not None:
            shp_fsa_all_filtered = shp_fsa_all_filtered.to_crs(new_crs)

        return shp_fsa_all_filtered

    return download_fsas_wrappee(year, 
                                pr_code,
                                 use_cartographic,
                                 data_download_path,
                                 new_crs  ) 


@Cache_wrapper(path_cache = join(DATA_DIR, "cache", "qc_city_fsa_2016.parquet"))
def download_qc_city_fsa_2016(buffer_degrees=0.1, **fsa_fun_kwargs) -> gpd.GeoDataFrame:


    # Select only the fsa within the province 
    shp_2016_fsas_qc_province = download_fsas(year  = 2016, pr_code = "24", **fsa_fun_kwargs) 

    # Get the Quebec city neighborhood polygons
    shp_qc_city = download_qc_city_neighborhoods()

    # Same crs
    shp_qc_city = shp_qc_city.to_crs(shp_2016_fsas_qc_province.crs)

    # Get intersecting polygons
    idx_intersects = shp_2016_fsas_qc_province.geometry.intersects(shp_qc_city.unary_union.buffer(buffer_degrees) )
    shp_2016_fsas_qc_city = shp_2016_fsas_qc_province[idx_intersects]

    return shp_2016_fsas_qc_city


"""
@Cache_wrapper(path_cache=join(DATA_DIR,"qc_city_fsaldu_2016_df.parquet"))
def download_qc_city_fsaldu_2016_df(**kwargs)-> pd.DataFrame:

    # Select only the fsaldu within the fsa within the city
    shp_2016_fsas_qc_city = download_qc_city_fsa_2016(**kwargs)

    # Get the pccf file
    df_pccf = get_df_pccf()

    # Extract the FSA
    df_pccf['FSA'] = df_pccf.POSTCODE.str[:3]

    # Join to filter by province (PCCF has all Canadian PCs)
    df_pccf_qc_city = pd.merge(df_pccf,
            shp_2016_fsas_qc_city[['CFSAUID']]   ,
            left_on = 'FSA' ,
            right_on =  'CFSAUID')

    return df_pccf_qc_city
"""


@Cache_wrapper(path_cache=join(DATA_DIR,"qc_province.parquet"))
def download_qc_boundary(use_cartographic:bool = USE_CARTOGRAPHIC,
                        data_download_path  :str = DATA_DIR ) -> gpd.GeoDataFrame:
    """
    download_qc_boundary Read the 2021 Province of Quebec boundary files 

    Args:
        use_cartographic (bool): _description_

    Returns:
        gpd.GeoDataFrame: _description_
    """

    zip_download_url = "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lpr_000b21a_e.zip" \
    if use_cartographic \
    else "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lpr_000a21a_e.zip"

    shp_prov = download_zip_shp(zip_download_url, data_download_path)

    ## Get Quebec
    shp_qc = shp_prov[ shp_prov.PRUID.astype('str') == "24"]
    shp_qc = shp_qc.to_crs(4326)
    assert shp_qc.shape[0] ==1

    return shp_qc


if __name__ == "__main__":
     df = download_qc_city_fsa_2016() 