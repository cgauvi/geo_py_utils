import numpy as np
from os import remove, makedirs
from os.path import exists, join

from geo_py_utils.misc.utils_test import MockTmpCache

from geo_py_utils.census_open_data.open_data import (
    download_qc_city_neighborhoods, 
    get_qc_city_bbox,
    DownloadQcAdmBoundaries,
    DownloadQcBoroughs,
    DownloadQcDissolvedAdmReg,
    DownloadQcDissolvedMunicipalities
)

class MockCacheOpenData(MockTmpCache):
    """Class to manually implement unittest.patch which never actually works
    Based on MockCache which implements the callable  
    """

    def __init__(self):
         super().__init__() 
         self.additional_mocked = join(self.mocked_dir, 'subdirmocked' )
         makedirs(self.additional_mocked, exist_ok=True) # will get deleted at cleanup if subdir

    def download_adm_bound (self, **kwargs):
        adm_bound_downloader = DownloadQcAdmBoundaries(data_download_path = self.mocked_dir, **kwargs)
        return adm_bound_downloader.get_qc_administrative_boundaries()
    
    def download_adm_bound_dissolved(self, **kwargs):
        adm_bound_downloader = DownloadQcDissolvedAdmReg(data_download_path = self.additional_mocked, path_cache_root= self.mocked_dir, **kwargs)
        return adm_bound_downloader.get_qc_administrative_boundaries()

    def download_boroughs (self, **kwargs):
        borough_downloader = DownloadQcBoroughs(data_download_path = self.mocked_dir, **kwargs)
        return borough_downloader.get_qc_administrative_boundaries()
    
    def download_dissolved_muni_and_raw(self, **kwargs):
        qc_admReg_dissolved_downloader = DownloadQcDissolvedMunicipalities(data_download_path = self.additional_mocked, path_cache_root = self.mocked_dir, **kwargs)
        return qc_admReg_dissolved_downloader.get_qc_administrative_boundaries(), qc_admReg_dissolved_downloader.get_raw_data()
    
    def download_dissolved_muni(self, **kwargs):
        qc_admReg_dissolved_downloader = DownloadQcDissolvedMunicipalities(data_download_path = self.additional_mocked, path_cache_root = self.mocked_dir, **kwargs)
        return qc_admReg_dissolved_downloader.get_qc_administrative_boundaries()

def test_qc_city_download():
    download_qc_city_neighborhoods()

def test_qc_city_bbox():
    dict_bbox = get_qc_city_bbox()

    assert dict_bbox['min_lat'] < dict_bbox['max_lat']
    assert dict_bbox['min_lng'] < dict_bbox['max_lng']


def test_admRegs_dissolved():

    with MockCacheOpenData() as mocked:
        shp_admReg = mocked.download_adm_bound_dissolved()

    assert shp_admReg.shape[0] == 15

    assert np.all(np.isin([DownloadQcDissolvedAdmReg.ADM_REG_DISSOLVE_ID_COL, 'DOMAINE_OPUS'], shp_admReg.columns))


def test_boroughs():
    with MockCacheOpenData() as mocked:
        shp_borough = mocked.download_boroughs()

    assert shp_borough.shape[0] > 10

def test_muni_dissolved():

    with MockCacheOpenData() as mocked:
        shp_admReg = mocked.download_dissolved_muni()
 
    assert shp_admReg.shape[0] == 1338

def test_muni_dissolved_raw():

    with MockCacheOpenData() as mocked:
        shp_admReg, shp_raw = mocked.download_dissolved_muni_and_raw(filter_out_unknown_muni=False)

    assert shp_admReg.shape[0] == 1345
    assert shp_raw.shape[0] == shp_admReg.shape[0] + 2
 



def test_qa_adm_boundaries():

    # For some reason the number of features is different bepending on the precision - 1/20 or 1/100
    dict_check_num_records = {
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_METRO: 2,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MRC: 106,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MUNI: 1347,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_ARROND: 41
    }
    
    with MockCacheOpenData() as mocked: 
        dict_results_num_records = { 
            k: mocked.download_adm_bound(geo_level=k).\
                shape[0] \
            for k,v in dict_check_num_records.items()
        }

    diffs = [ abs(dict_results_num_records[k] - dict_check_num_records[k]) \
            for k in dict_check_num_records.keys()]

    assert max(diffs) == 0, f"Fatal error, max difference is { max(diffs)}"



if __name__== "__main__":
    test_admRegs_dissolved()
 