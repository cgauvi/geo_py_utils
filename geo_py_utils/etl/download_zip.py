
import geopandas as gpd
import logging
from os.path import join, isdir, isfile, dirname
from os import makedirs, remove
from pathlib import Path
import requests
from shutil import rmtree
from typing import Union
import zipfile

from geo_py_utils.misc.constants import DATA_DIR

logger = logging.getLogger(__file__)

DEFAULT_DATA_DOWNLOAD_PATH = DATA_DIR


def download_zip_shp(url: str,
                     data_download_path: str = DEFAULT_DATA_DOWNLOAD_PATH,
                     return_shp_file: bool = True,
                     extension: str = ".shp") -> Union[gpd.GeoDataFrame, None]:
    """ Download a zipped shp file from a url + save results.

    Only meant to work with zipped shp files, for geojson just read in using .read_file()

    Args:
        url (str): url 
        data_download_path (str, optional): path to save the zipped file. Defaults to DEFAULT_DATA_DOWNLOAD_PATH.
        return_shp_file (bool, optional): if false, only downloads the zip file to disk
        extension (str, optional): extension of geo file to look for. Should be readable by `geopandas.read_file()`  
    Returns:
        gpd.GeoDataFrame: geopandas df
    """

    # e.g. extract lfsa000b16a_e from the following url
    # 'https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000b16a_e.zip'
    file_download = Path(url).stem

    # QA 
    if not isdir(data_download_path): raise ValueError('Error with download dir!') 
    if extension is None or len(extension) < 2 or '.' not in extension: raise ValueError('Error with extension!')
    if len(file_download) == 0 : raise ValueError('Error with url!')

    # Download and unzip as required 
    ## Set the download path 
    path_data_dir_unzipped = join(data_download_path, file_download)  # path after unzipping 
    path_data_dir_zip = path_data_dir_unzipped + ".zip" # path of zipped file e.g. ../data/lpr_000a21a_e.zip

    if not isdir(path_data_dir_unzipped):
        response= requests.get(url)
        makedirs(dirname(path_data_dir_unzipped), exist_ok=True)

        with open(path_data_dir_zip, "wb") as f:
            f.write(response.content)

        with zipfile.ZipFile(path_data_dir_zip, 'r') as zip_ref:
            zip_ref.extractall(path_data_dir_unzipped)

    # Only unzip
    if not return_shp_file:
        logger.warning(f"Only unzipping file to {path_data_dir_unzipped_shp}")
        return None

    # Look for geo file 
    try:

        # Automatically look for geo files with the desired extension in the unzipped dir
        list_potential_files = list(Path(path_data_dir_unzipped).glob(f'**/*{extension}'))
        if len(list_potential_files) == 0 : raise RuntimeError('No files in unzipped dir!')
        if len(list_potential_files) > 1 : raise RuntimeError(f'More than one {extension} in dir: cannot determine which to read!')

        # Read the unique geo file
        shp = gpd.read_file( list_potential_files[0])

    finally:
        if isfile(path_data_dir_zip): remove(path_data_dir_zip)
        if isdir(path_data_dir_unzipped): rmtree(path_data_dir_unzipped)

    return shp


