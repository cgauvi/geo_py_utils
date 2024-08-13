import numpy as np
from os import makedirs
from os.path import join
import re
from requests.exceptions import RequestException

from geo_py_utils.utilities_testing.mock_tmp_cache import MockTmpCache

from geo_py_utils.census_open_data.open_data import (
    download_qc_city_neighborhoods, 
    get_qc_city_bbox,
    DownloadQcAdmBoundaries,
    DownloadQcBoroughs,
    DownloadQcDissolvedAdmReg,
    DownloadQcDissolvedMunicipalities
)

DICT_CHECK_NUM_RECORDS = {
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_METRO: 2,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MRC: 106,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MUNI: 1345,
            DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_ARROND: 41
    }

NUM_UNIQUE_OPUS = len(set([v  for v  in DownloadQcDissolvedAdmReg.DICT_MAPPING.values() if v is not None]))
NUM_UNIQUE_ADM_REGIONS = len(set([re.sub('[-–]','',s) for s in DownloadQcDissolvedAdmReg.DICT_MAPPING.keys()]))

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
    try:
        download_qc_city_neighborhoods()
    except RequestException as e:
        print(f'Error with network: {e} - skipping test')

def test_qc_city_bbox():
    dict_bbox = get_qc_city_bbox()

    assert dict_bbox['min_lat'] < dict_bbox['max_lat']
    assert dict_bbox['min_lng'] < dict_bbox['max_lng']


def test_admRegs_dissolved_no_opus():
    # Need to separate test_admRegs_dissolved_no_opus_all and test_admRegs_dissolved_with_opus: otherwise we will use the same cached file (which is undesirable)
    try:
        with MockCacheOpenData() as mocked:
            shp_admReg_no_opus = mocked.download_adm_bound_dissolved()
            assert shp_admReg_no_opus.shape[0] == NUM_UNIQUE_ADM_REGIONS, 'Fatal error! incorrect number of unique regions'
            assert np.all(np.isin(DownloadQcDissolvedAdmReg.ADM_REG_DISSOLVE_ID_COL, shp_admReg_no_opus.columns))
            assert shp_admReg_no_opus.groupby(DownloadQcDissolvedAdmReg.ADM_REG_DISSOLVE_CODE_COL)[DownloadQcDissolvedAdmReg.ADM_REG_DISSOLVE_CODE_COL].size().sum() == NUM_UNIQUE_ADM_REGIONS , 'Fatal error! some regions are repeated '

    except RequestException as e:
        print(f'Error (potentially with network): {e} - skipping test')


def test_admRegs_dissolved_with_opus():

    try:
        with MockCacheOpenData() as mocked:
            shp_admReg_with_opus = mocked.download_adm_bound_dissolved(add_opus_region_names=True)

            assert shp_admReg_with_opus.shape[0] == NUM_UNIQUE_ADM_REGIONS, 'Fatal error! left join fails '
            assert np.all(np.isin([DownloadQcDissolvedAdmReg.ADM_REG_DISSOLVE_ID_COL, 'DOMAINE_OPUS'], shp_admReg_with_opus.columns))
            assert shp_admReg_with_opus.groupby(DownloadQcDissolvedAdmReg.ADM_REG_DISSOLVE_CODE_COL)[DownloadQcDissolvedAdmReg.ADM_REG_DISSOLVE_CODE_COL].size().sum() == NUM_UNIQUE_ADM_REGIONS

            
            assert shp_admReg_with_opus.groupby('DOMAINE_OPUS')['DOMAINE_OPUS'].size().sum()  == NUM_UNIQUE_OPUS

    except RequestException as e:
        print(f'Error (potentially with network): {e} - skipping test')


def test_boroughs():
    try:
        with MockCacheOpenData() as mocked:
            shp_borough = mocked.download_boroughs()

            assert shp_borough.shape[0] > 10
    except RequestException as e:
        print(f'Error with network: {e} - skipping test')

def test_muni_dissolved():

    try:
        with MockCacheOpenData() as mocked:
            shp_muni_dissolved_no_unknown = mocked.download_dissolved_muni()
    
            NUM_DISSOLVED = 2
            NUM_FILTERED_NO_NAME = 7

            assert shp_muni_dissolved_no_unknown.shape[0] == DICT_CHECK_NUM_RECORDS[DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MUNI] - NUM_FILTERED_NO_NAME - NUM_DISSOLVED
    except RequestException as e:
        print(f'Error with network: {e} - skipping test')

def test_muni_dissolved_raw():

    try:
        with MockCacheOpenData() as mocked:
            shp_muni_dissolved_with_unknown, shp_raw = mocked.download_dissolved_muni_and_raw(filter_out_unknown_muni=False)

            assert shp_raw.shape[0] == DICT_CHECK_NUM_RECORDS[DownloadQcAdmBoundaries.QC_PROV_ADM_BOUND_MUNI]
            assert shp_raw.shape[0] == shp_muni_dissolved_with_unknown.shape[0] + 2

    except RequestException as e:
        print(f'Error with network: {e} - skipping test')



def test_qa_adm_boundaries():

    try:
        # For some reason the number of features is different bepending on the precision - 1/20 or 1/100  
        with MockCacheOpenData() as mocked: 
            dict_results_num_records = { 
                k: mocked.download_adm_bound(geo_level=k).\
                    shape[0] \
                for k,v in DICT_CHECK_NUM_RECORDS.items()
            }

            diffs = [ abs(dict_results_num_records[k] - DICT_CHECK_NUM_RECORDS[k]) \
                    for k in DICT_CHECK_NUM_RECORDS.keys()]

            assert max(diffs) == 0, f"Fatal error, max difference is { max(diffs)}"
    except RequestException as e:
        print(f'Error with network: {e} - skipping test')


if __name__ == '__main__':
    test_admRegs_dissolved_with_opus()