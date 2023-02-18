from os.path import join, isfile
import geopandas as gpd


from geo_py_utils.census_open_data.open_data import (
    download_qc_city_neighborhoods, 
    get_qc_city_bbox,
    DownloadQcAdmBoundaries
)
 
def test_qc_city_download():
    download_qc_city_neighborhoods()

def test_qc_city_bbox():
    dict_bbox = get_qc_city_bbox()

    assert dict_bbox['min_lat'] < dict_bbox['max_lat']
    assert dict_bbox['min_lng'] < dict_bbox['max_lng']


def test_mrcs_dssolved():
    qc_boundaries_extracter = DownloadQcAdmBoundaries()
    shp_mrc = qc_boundaries_extracter.get_dissolved_mrc() 

    assert shp_mrc.shape[0] == 17


def test_qa_adm_boundaries():

    dict_check_num_records = {
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_METRO: 2,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MRC: 148,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MUNI: 2457
    }
    
    dict_results_num_records = { 
        k: DownloadQcAdmBoundaries(geo_level=k).\
            get_qc_administrative_boundaries().\
            shape[0] \
        for k,v in dict_check_num_records.items()
    }

    diffs = [ abs(dict_results_num_records[k] - dict_check_num_records[k]) \
            for k in dict_check_num_records.keys()]

    assert max(diffs) == 0



if __name__== "__main__":
    test_qc_city_bbox()