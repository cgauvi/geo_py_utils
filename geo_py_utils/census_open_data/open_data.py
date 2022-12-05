
import geopandas as gpd
from os.path import join

from ben_py_utils.misc.cache import Cache_wrapper
 
from geo_py_utils.misc.constants import DATA_DIR

QC_CITY_NEIGH_URL = "https://www.donneesquebec.ca/recherche/dataset/5b1ae6f2-6719-46df-bd2f-e57a7034c917/resource/436c85aa-88d9-4e57-9095-b72b776a71a0/download/vdq-quartier.geojson"

@Cache_wrapper(path_cache=join(DATA_DIR, "cache", "qc_neighborhoods.parquet"))
def download_qc_city_neighborhoods():
    url_qc_city = QC_CITY_NEIGH_URL
    shp_qc_city = gpd.read_file(url_qc_city).to_crs(4326)        

    return shp_qc_city
