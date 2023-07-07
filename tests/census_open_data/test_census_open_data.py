import numpy as np
from os import remove
from os.path import exists

from geo_py_utils.census_open_data.open_data import (
    download_qc_city_neighborhoods, 
    get_qc_city_bbox,
    DownloadQcAdmBoundaries,
    DownloadQcDissolvedAdmReg,
    DownloadQcDissolvedMunicipalities
)

 
def test_qc_city_download():
    download_qc_city_neighborhoods()

def test_qc_city_bbox():
    dict_bbox = get_qc_city_bbox()

    assert dict_bbox['min_lat'] < dict_bbox['max_lat']
    assert dict_bbox['min_lng'] < dict_bbox['max_lng']


def test_admRegs_dissolved(clean_slate=False):

    # Make sure clean slate 
    if exists(DownloadQcDissolvedAdmReg.PATH_CACHE) and clean_slate:
        remove(DownloadQcDissolvedAdmReg.PATH_CACHE)

    qc_admReg_dissolved_extracter = DownloadQcDissolvedAdmReg()
    shp_admReg = qc_admReg_dissolved_extracter.get_qc_administrative_boundaries()

    assert shp_admReg.shape[0] == 15

    assert np.all(np.isin([DownloadQcDissolvedAdmReg.ADM_REG_DISSOLVE_ID_COL, 'DOMAINE_OPUS'], shp_admReg.columns))


def test_muni_dissolved(clean_slate=False):


    qc_admReg_dissolved_extracter = DownloadQcDissolvedMunicipalities()

    # Make sure clean slate 
    if exists(qc_admReg_dissolved_extracter.path_cache) and clean_slate:
        remove(qc_admReg_dissolved_extracter.path_cache)

    shp_admReg = qc_admReg_dissolved_extracter.get_qc_administrative_boundaries()

    assert shp_admReg.shape[0] == 1338

def test_muni_dissolved_raw(clean_slate=False):


    qc_admReg_dissolved_extracter = DownloadQcDissolvedMunicipalities(filter_out_unknown_muni=False)

    # Make sure clean slate 
    if exists(qc_admReg_dissolved_extracter.path_cache) and clean_slate:
        remove(qc_admReg_dissolved_extracter.path_cache)

    shp_admReg = qc_admReg_dissolved_extracter.get_qc_administrative_boundaries()

    assert shp_admReg.shape[0] == 1345
    assert qc_admReg_dissolved_extracter.get_raw_data().shape[0] == shp_admReg.shape[0] + 2
 



def test_qa_adm_boundaries():

    # Make sure clean slate 
    if exists(DownloadQcDissolvedAdmReg.PATH_CACHE):
        remove(DownloadQcDissolvedAdmReg.PATH_CACHE)

    # For some reason the number of features is different bepending on the precision - 1/20 or 1/100
    dict_check_num_records = {
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_METRO: 2,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MRC: 106,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MUNI: 1347,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_ARROND: 41
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
    test_muni_dissolved_raw()
 