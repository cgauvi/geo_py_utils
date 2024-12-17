 
#Created on September 15 2021
#@author: charles gauvin
#Callable class used to cache long query results that can be used with the @wrapper syntax

 

import pandas as pd
import geopandas as gpd
import logging
from os.path import isdir,dirname, isfile, splitext
from os import makedirs
from typing import Union 

 

# Set logger
logger = logging.getLogger(__file__)


class Cache_wrapper:

    """Semi fancy wrapper implemented as callable class that caches results to a parquet file.

    Can be used as such:

    ```
    @Cache_wrapper(path_cache='bla/blo.parquet')
    def foo():
        pass
    ```
    
    Tries the following file formats in order and moves to next only in case of failure:

    1) .parquet
    2) extension considered in name (e.g. csv or geojson if path_cache = 'bla.csv')
    3) if Geodf, alternative geoformat (either shp or geojson )


    Attributes:
        path_cache (str), Default[None]
            Path of destination file with parquet extention 
        pd_save_index (boolean), Default[False]
            Save pandas index?  
        force_overwrite (boolean), Default[False]
            Run the function even the results have been cached 

    """

    def __init__(self, 
                path_cache,
                pd_save_index=False, 
                force_overwrite=False):

        self.path_cache = path_cache
        self.pd_save_index = pd_save_index
        self.force_overwrite = force_overwrite

        # Make sure we save as parquet 
        # Not the same interface with and without parquet - using a more general data format is good, but adds to mnay flows to the code 
        path_pre, path_ext = splitext(self.path_cache)
        if path_ext != ".parquet":
            raise ValueError(f"Fatal error, extension is {path_ext} - should be .parquet ")


    def _read_existing_file(self) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Try to read back an existing file from cache

        Returns:
            Union[pd.DataFrame, gpd.GeoDataFrame]: _description_
        """
        logger.info(f'Reading back {self.path_cache} ...')
 

        try:
            df_result = gpd.read_parquet(self.path_cache)
        except Exception as e:
            logger.warning(f"Fatal error trying to load back geo data from {self.path_cache} - {str(e)} - trying with pandas")
            try:
                df_result = pd.read_parquet(self.path_cache)
            except Exception as e:
                raise e(f"Fatal error trying to load back NON geo data from {self.path_cache} - {str(e)}")
                
        # Remove useless index if present and if we want to disregard indixes
        if 'Unnamed: 0' in df_result.columns and not self.pd_save_index:  
            df_result = df_result.drop(columns={'Unnamed: 0'})
            
        return df_result
 

    
    def _create_new_file(self, fun, *kws, **kwargs) -> Union[pd.DataFrame, gpd.GeoDataFrame] :
        """
        Run fun(*kws, **kwargs) and cache the results

        Args:
            fun (_type_): function to run 

        Returns:
             Union[pd.DataFrame, gpd.GeoDataFrame] : df created by fun + cached 
        """

        logger.info(f'Creating new {self.path_cache} ...')

        df_result = fun(*kws, **kwargs)

        try:
            # Raw 
            df_result.to_parquet(self.path_cache, index=False)
        except Exception:
            # Try converting to string first
            try:
                df_result.columns = df_result.columns.astype(str)
                df_result.to_parquet(self.path_cache, engine='pyarrow', index=False)
            # Fail: try different paths depending on gpd or pd df
            except Exception as err:
                logger.error(f'Parquet file creation failed \n{err}')

        return df_result



    def __call__(self, fun):

        def inner_wrapper(*kws, **kwargs):
            if not isdir(dirname(self.path_cache)):
                makedirs(dirname(self.path_cache))

            if isfile(self.path_cache) and not self.force_overwrite:
                df_result = self._read_existing_file()
            else:
                df_result = self._create_new_file(fun, *kws, **kwargs)

            return df_result

        return inner_wrapper