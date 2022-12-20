from os.path import join, isfile
import geopandas as gpd


from geo_py_utils.census_open_data.open_data import download_qc_city_neighborhoods, get_qc_city_bbox
 
def test_qc_city_download():
    download_qc_city_neighborhoods()

def test_qc_city_bbox():
    dict_bbox = get_qc_city_bbox()

    assert dict_bbox['min_lat'] < dict_bbox['max_lat']
    assert dict_bbox['min_lng'] < dict_bbox['max_lng']


if __name__== "__main__":
    test_qc_city_bbox()