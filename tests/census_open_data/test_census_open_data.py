import numpy as np
from os import remove
from os.path import exists

from geo_py_utils.census_open_data.open_data import (
    download_qc_city_neighborhoods, 
    get_qc_city_bbox,
    DownloadQcAdmBoundaries,
    DownloadQcDissolvedMrc
)

 
def test_qc_city_download():
    download_qc_city_neighborhoods()

def test_qc_city_bbox():
    dict_bbox = get_qc_city_bbox()

    assert dict_bbox['min_lat'] < dict_bbox['max_lat']
    assert dict_bbox['min_lng'] < dict_bbox['max_lng']


def test_mrcs_dissolved():

    # Make sure clean slate 
    if exists(DownloadQcDissolvedMrc.PATH_CACHE):
        remove(DownloadQcDissolvedMrc.PATH_CACHE)

    qc_mrc_dissolved_extracter = DownloadQcDissolvedMrc()
    shp_mrc = qc_mrc_dissolved_extracter.get_qc_administrative_boundaries()

    assert shp_mrc.shape[0] == 17

    assert np.all(np.isin([DownloadQcDissolvedMrc.MRC_DISSOLVE_ID_COL, 'DOMAINE_OPUS'], shp_mrc.columns))


def test_qa_adm_boundaries():

    # Make sure clean slate 
    if exists(DownloadQcDissolvedMrc.PATH_CACHE):
        remove(DownloadQcDissolvedMrc.PATH_CACHE)

    # For some reason the number of features is different bepending on the precision - 1/20 or 1/100
    dict_check_num_records = {
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_METRO: 2,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MRC: 106,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MUNI: 1347
    }
    
    dict_results_num_records = { 
        k: DownloadQcAdmBoundaries(geo_level=k).\
            get_qc_administrative_boundaries().\
            shape[0] \
        for k,v in dict_check_num_records.items()
    }

    diffs = [ abs(dict_results_num_records[k] - dict_check_num_records[k]) \
            for k in dict_check_num_records.keys()]

    assert max(diffs) == 0, f"Fatal error, max difference is { max(diffs)}"



if __name__== "__main__":
    test_mrcs_dissolved()
    test_qa_adm_boundaries()