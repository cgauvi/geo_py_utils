from functools import wraps
import geopandas as gpd

import logging

logger = logging.getLogger(__name__)

def crs_transform(fun=None, new_crs=None):

    """Decorator that transforms the coordinate reference system after a function is done executing

    Shamelessly adapted from https://stackoverflow.com/questions/3888158/making-decorators-with-optional-arguments

    Should work with or without named keyword args

    Args:
        fun (_type_, optional): function to decorate. Defaults to None.
        new_crs (_type_, optional): new crs to use. Defaults to None.
    """
    def _decorate(function):

        @wraps(function)
        def wrapped_function(*args, **kwargs):
            df = function(*args, **kwargs)

            if not isinstance(df, gpd.GeoDataFrame):
                raise ValueError(f"Fatal error with crs_transform! Should return a GeoaDataframe; not {str(type(df))}")

            if new_crs:
                crs_init = df.crs
                logger.info(f"Transforming crs from {crs_init} to {new_crs} in crs_transform")
                df = df.to_crs(new_crs)
            else:
                logger.info(f"Doing nothing in crs_transform: no valid crs inputed")

            return df

        return wrapped_function

    if fun:
        return _decorate(fun)

    return _decorate
 

def temp_crs_transform(fun=None, temp_crs=None):
    """Decorator that transforms the projection after

    Shamelessly adapted from https://stackoverflow.com/questions/3888158/making-decorators-with-optional-arguments

    Args:
        fun (_type_, optional): _description_. Defaults to None.
        optional_argument1 (_type_, optional): _description_. Defaults to None.
        optional_argument2 (_type_, optional): _description_. Defaults to None.
    """

    def _decorate(function):

        @wraps(fun)
        def wrapped_function(*args, **kwargs):

            df = kwargs.get('df', None)

            if df is None:
                raise ValueError("Fatal error with temp_crs_transform! wrapped function needs to have a 'df' kwarg")

            if not isinstance(df, gpd.GeoDataFrame):
                raise ValueError(f"Fatal error with crs_transform! Should return a GeoaDataframe; not {str(type(df))}")

            if temp_crs:
                crs_init = df.crs
                df_new = df.to_crs(temp_crs).copy()
                kwargs['df'] = df_new

                df_result = function(*args, **kwargs)
                df_result = df_result.to_crs(crs_init)
                return df_result

            else:
                logger.info(f"Doing nothing in temp_crs_transform: no valid crs inputed")
                return df
            
        return wrapped_function

    if fun:
        return _decorate(fun)

    return _decorate
 







if __name__ == '__main__' :

    from geo_py_utils.census_open_data.open_data import download_qc_city_neighborhoods


    # --- 

    new_crs = 3857

    @crs_transform (new_crs = new_crs)
    def crs_transform_qc_city():
        shp_qc_city = download_qc_city_neighborhoods()
        return shp_qc_city

    assert crs_transform_qc_city().crs == new_crs


    # --- 



    @temp_crs_transform(temp_crs=32198)
    def crs_temp_area_qc_city(df):
        df['area_projected'] = df.area
        return df

    def compute_area():
        shp_qc_city = download_qc_city_neighborhoods()
        return crs_temp_area_qc_city(df = shp_qc_city)

    
    df_with_area = compute_area()
    assert df_with_area.crs != 32198

    assert min(abs(df_with_area.area - df_with_area['area_projected'])) > 0