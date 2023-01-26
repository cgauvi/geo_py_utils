
import geopandas as gpd
from os.path import join

from ben_py_utils.misc.cache import Cache_wrapper
 
from geo_py_utils.misc.constants import DATA_DIR

QC_CITY_NEIGH_URL = "https://www.donneesquebec.ca/recherche/dataset/5b1ae6f2-6719-46df-bd2f-e57a7034c917/resource/436c85aa-88d9-4e57-9095-b72b776a71a0/download/vdq-quartier.geojson"

@Cache_wrapper(path_cache=join(DATA_DIR, "cache", "qc_neighborhoods.parquet"))
def download_qc_city_neighborhoods():
    """ Download the neighborhood polygons for Qc City (city proper only - corresponds to census sub division) 
    """
    url_qc_city = QC_CITY_NEIGH_URL
    shp_qc_city = gpd.read_file(url_qc_city).to_crs(4326)        

    return shp_qc_city



def get_qc_city_bbox()-> dict : 

    """ Build a spatial clause: take only policies within Quebec City bounding box. 

    Returns:
        bounding box: dict representing qc city bounding box
    """

    # Get the qc city neighborhoods
    shp_qc_city = download_qc_city_neighborhoods()

    # Take only policies within Quebec City bounding box
    min_lng, min_lat, max_lng, max_lat = shp_qc_city.total_bounds


    return {"min_lng": min_lng, 
            "min_lat": min_lat, 
            "max_lng": max_lng, 
            "max_lat": max_lat}