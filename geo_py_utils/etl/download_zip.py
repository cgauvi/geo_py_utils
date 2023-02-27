
import geopandas as gpd
from os.path import join, isfile, dirname
from os import makedirs, remove
from pathlib import Path
from typing import Union

import requests
import zipfile
import logging

from geo_py_utils.misc.constants import DATA_DIR


logger = logging.getLogger(__file__)

DEFAULT_DATA_DOWNLOAD_PATH = DATA_DIR


def download_zip_shp(url: str,
                     data_download_path: str = DEFAULT_DATA_DOWNLOAD_PATH,
                     return_shp_file: bool = True) -> Union[gpd.GeoDataFrame, None]:
    """ Download a zipped shp file from a url + save results.

    Only meant to work with zipped shp files, for geojson just read in using .read_file()

    Args:
        url (str): url 
        data_download_path (str, optional): path to save the zipped file. Defaults to DEFAULT_DATA_DOWNLOAD_PATH.
        return_shp_file (bool, optional): if false, only downloads the zip file to disk
    Returns:
        gpd.GeoDataFrame: geopandas df
    """

    # e.g. extract lfsa000b16a_e from the following url
    # 'https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000b16a_e.zip'
    file_download = Path(url).stem

    ## Set the download path 
    path_data_dir_unzipped = join(data_download_path, file_download)  # path after unzipping 
    path_data_dir_zip = path_data_dir_unzipped + ".zip" # path of zipped file e.g. ../data/lpr_000a21a_e.zip
    path_data_dir_unzipped_shp = join(path_data_dir_unzipped, Path(file_download).stem + ".shp") # path of actual shp file e.g. ../data/lpr_000a21a_e/lpr_000a21a_e.shp


    ## Download and unzip as required 
    shp = None
    
    if not isfile(path_data_dir_unzipped_shp):
        response= requests.get(url)
        makedirs(dirname(path_data_dir_unzipped),exist_ok=True)
        with open(path_data_dir_zip, "wb") as f:
            f.write(response.content)

        with zipfile.ZipFile(path_data_dir_zip, 'r') as zip_ref:
            zip_ref.extractall(path_data_dir_unzipped)

        remove(path_data_dir_zip)

        if return_shp_file:
            shp = gpd.read_file(path_data_dir_unzipped_shp)
    elif return_shp_file:
        shp = gpd.read_file(path_data_dir_unzipped_shp)
    else:
        logger.warning(f"Only unzipping file to {path_data_dir_unzipped_shp}")
        

    return shp


