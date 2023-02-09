

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
from geo_py_utils.geo_general.crs import get_crs_str

logger = logging.getLogger(__file__)

# Cartographic is very precise (hence very heavy) whereas digital is coarser and faster to retrieve and process
USE_CARTOGRAPHIC = False
FSA_2016_URL = "https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000b16a_e.zip"



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
    


def download_ca_cmas(year : int = 2016,
                    pr_code:str = '24',             
                    path_cache_root: str = join(DATA_DIR, "cache"),
                    use_cartographic:bool = USE_CARTOGRAPHIC,
                    data_download_path  :str = DATA_DIR,
                    new_crs = None,
                    force_spatial_join: bool = False)  -> gpd.GeoDataFrame:


    @Cache_wrapper(path_cache = join(path_cache_root, f"ca_cmas_{year}_{pr_code}_carto_{use_cartographic}_crs_{get_crs_str(new_crs)}_join_{force_spatial_join}.parquet"))
    def download_ca_cmas_wrappee(year,
                                pr_code,
                                use_cartographic,
                                data_download_path ,
                                new_crs,
                                force_spatial_join)  :
        """ Read in the 2016 CAs + CMAs (DAs) for Province of Quebec (only available for 2016 as of this witing)

        Args:
            use_cartographic (bool): _description_

        Returns:
            gpd.GeoDataFrame: _description_
        """

        logger.info(f"Saving data to {data_download_path}")

        if year == 2016:
            zip_download_url = "https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lcma000b16a_e.zip"\
            if use_cartographic \
            else "https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lcma000a16a_e.zip"  
        else:
            raise ValueError(f"Fatal error - inputed {year}, but only 2016 implemented")


        shp_ca_cmas_all = download_zip_shp(zip_download_url, data_download_path)

        # Transform (do this before the spatial join)
        if new_crs is not None:
            shp_ca_cmas_all = shp_ca_cmas_all.to_crs(new_crs)

        # Get the province (with the correct crs) - use 2021 - shouldnt change
        shp_prov = download_prov_boundary(year= 2021,
                                        pr_code = pr_code,
                                        use_cartographic = use_cartographic, 
                                        data_download_path = data_download_path,
                                        new_crs = shp_ca_cmas_all.crs)
 
        
        num_qc_ca_cmas = np.sum(shp_ca_cmas_all.CMAPUID.str[:2] == str(pr_code))
        shp_cas_cmas_prov = gpd.sjoin(shp_ca_cmas_all, shp_prov[['geometry']], how="inner", op="within" )

        # Some CAs+CMAs straddle multiple provinces
        if num_qc_ca_cmas != shp_cas_cmas_prov.shape[0] :
            logger.warning(f"Warning, there are {shp_cas_cmas_prov.shape[0]} CA+CMAS found by spatial join but {num_qc_ca_cmas} based on CMAPUID filtering")

        # Conservative: take largest
        if num_qc_ca_cmas > shp_cas_cmas_prov.shape[0] and not force_spatial_join:
            shp_cas_cmas_prov = shp_ca_cmas_all[shp_ca_cmas_all.CMAPUID.str[:2] == str(pr_code)]

        logger.info(f"There are {shp_cas_cmas_prov.shape[0]} features/cas+cmas in {pr_code} for the {year} census")

        return shp_cas_cmas_prov

    return download_ca_cmas_wrappee(year,
                                pr_code,
                                use_cartographic,
                                data_download_path ,
                                new_crs,
                                force_spatial_join)

def download_das(year : int = 2021,
                    pr_code:str = '24',             
                    path_cache_root: str = join(DATA_DIR, "cache"),
                    use_cartographic:bool = USE_CARTOGRAPHIC,
                    data_download_path  :str = DATA_DIR,
                    new_crs = None,
                    force_spatial_join: bool = False)  -> gpd.GeoDataFrame:
    
 

    @Cache_wrapper(path_cache = join(path_cache_root, f"das_{year}_{pr_code}_carto_{use_cartographic}_crs_{get_crs_str(new_crs)}_join_{force_spatial_join}.parquet"))
    def download_das_wrappee(year,
                                pr_code,
                                use_cartographic,
                                data_download_path ,
                                new_crs,
                                force_spatial_join)  :
        """
        download_das Read in the 2021 dissemination areas (DAs) for a iven province

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

        # Get all DAs
        shp_das_all = download_zip_shp(zip_download_url, data_download_path)
        
        # Transform (do this before the spatial join)
        if new_crs is not None:
            shp_das_all = shp_das_all.to_crs(new_crs)

        # Get the province (with the correct crs) - use 2021 - doesnt chage
        shp_prov = download_prov_boundary(year= 2021,
                                        pr_code = pr_code,
                                        use_cartographic = use_cartographic, 
                                        data_download_path = data_download_path,
                                        new_crs = shp_das_all.crs)

        num_das= np.sum(shp_das_all.DAUID.str[:2] == str(pr_code))
        shp_das_prov = gpd.sjoin(shp_das_all, shp_prov[['geometry']], how="inner", op="within" )

        # Some DAs in sask + alberta might overlap 2 provinces? not sure
        if num_das != shp_das_prov.shape[0]:
            logger.warning( f"Warning, there are {shp_das_prov.shape[0]} DAs found by spatial join but {num_das} based on DAUID filtering")
        
        # Conservative: take largest
        if num_das > shp_das_all.shape[0] and not force_spatial_join:
            shp_das_prov = shp_das_all[shp_das_all.DAUID.str[:2] == str(pr_code)]

        logger.info(f"There are {shp_das_prov.shape[0]} features/das in {pr_code} for the {year} census")


        return shp_das_prov

    return download_das_wrappee(year,
                                pr_code,
                                use_cartographic,
                                data_download_path ,
                                new_crs,
                                force_spatial_join)


def download_fsas(year : int = 2016,
                    pr_code : str = "24",
                    path_cache_root: str = join(DATA_DIR, "cache"),
                    use_cartographic:bool = USE_CARTOGRAPHIC,
                    data_download_path = DATA_DIR,
                    new_crs=None)  -> gpd.GeoDataFrame:
 

    @Cache_wrapper(path_cache = join(path_cache_root,f"fsa_{year}_{pr_code}_carto_{use_cartographic}_crs_{get_crs_str(new_crs)}.parquet"))
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
            zip_download_url = "https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000b16a_e.zip"  \
            if use_cartographic \
            else "https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000a16a_e.zip"
        else:
            raise ValueError(f"Fatal error - inputed {year}, but only 2016 implemented")

        shp_fsa_all = download_zip_shp(zip_download_url, data_download_path)
        shp_fsa_all_filtered = shp_fsa_all.loc[shp_fsa_all.PRUID.astype('str') == str(pr_code), ]
    
        logger.info(f"There are {shp_fsa_all_filtered.shape[0]} FSAs in {pr_code} for the 2016 census")

        # Transform
        if new_crs is not None:
            shp_fsa_all_filtered = shp_fsa_all_filtered.to_crs(new_crs)

        return shp_fsa_all_filtered

    return download_fsas_wrappee(year, 
                                pr_code,
                                 use_cartographic,
                                 data_download_path,
                                 new_crs  ) 


 
def download_qc_city_fsa_2016(buffer_degrees=0.1, **fsa_fun_kwargs) -> gpd.GeoDataFrame:
    """Convenience function to get fsas for Quebec City only (using intersection)

    Args:
        buffer_degrees (float, optional): _description_. Defaults to 0.1.
        fsa_fun_kwargs: keywords to pass to download_fsas

    Returns:
        gpd.GeoDataFrame: _description_
    """    

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


 
def download_prov_boundary(year: int = 2021,
                        pr_code: int = 24,
                        path_cache_root: str = join(DATA_DIR, "cache"),
                        use_cartographic:bool = USE_CARTOGRAPHIC,
                        data_download_path  :str = DATA_DIR,
                        new_crs = None) -> gpd.GeoDataFrame:
    """
    download_prov_boundary Read the 2021 Province boundary files 

    Args:
        pr_code (int) : province code
        use_cartographic (bool): cartographic (high resolution) or digital 
        data_download_path (str): path where to download
        new_crs : crs to use (e.g. to reproject)

    Returns:
        gpd.GeoDataFrame: _description_
    """

 
    @Cache_wrapper(path_cache = join(path_cache_root,f"province_{year}_{pr_code}_carto_{use_cartographic}_crs_{get_crs_str(new_crs)}.parquet"))
    def download_prov_boundary_wrappee(year, 
                                pr_code,
                                 use_cartographic,
                                 data_download_path,
                                 new_crs  ) -> gpd.GeoDataFrame:

        if year != 2021:
            raise ValueError(f"Fatal error - inputed {year}, but only 2016 implemented")
                                    
        zip_download_url = "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lpr_000b21a_e.zip" \
        if use_cartographic \
        else "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lpr_000a21a_e.zip"

        shp_prov = download_zip_shp(zip_download_url, data_download_path)

        ## Get select province
        shp_prov_select = shp_prov[ shp_prov.PRUID.astype('str') == str(pr_code)]
        
        # Transform
        if new_crs is not None:
            shp_prov_select = shp_prov_select.to_crs(new_crs)

        assert shp_prov_select.shape[0] == 1

        return shp_prov_select

    return download_prov_boundary_wrappee(year, 
                                pr_code,
                                 use_cartographic,
                                 data_download_path,
                                 new_crs  ) 


if __name__ == "__main__":
     #df = download_qc_city_fsa_2016() 
     df_ca = download_ca_cmas(pr_code=24, new_crs = 4269) 
 