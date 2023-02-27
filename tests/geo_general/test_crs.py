
from geo_py_utils.census_open_data.open_data import download_qc_city_neighborhoods
from geo_py_utils.geo_general.crs import crs_transform, temp_crs_transform

 



def test_new_crs_3857():
    
    new_crs = 3857

    @crs_transform (new_crs = new_crs)
    def crs_transform_qc_city():
        shp_qc_city = download_qc_city_neighborhoods()
        return shp_qc_city

    assert crs_transform_qc_city().crs == new_crs


def test_new_crs_none():
    
    crs_init = download_qc_city_neighborhoods().crs

    @crs_transform ()
    def crs_transform_qc_city():
        shp_qc_city = download_qc_city_neighborhoods()
        return shp_qc_city

    assert crs_transform_qc_city().crs == crs_init


def test_tmp_crs():


    @temp_crs_transform(temp_crs=32198)
    def crs_temp_area_qc_city(df):
        df['area_projected'] = df.area
        return df

    def compute_area():
        shp_qc_city = download_qc_city_neighborhoods()
        return crs_temp_area_qc_city(df = shp_qc_city)

    
    df_with_area = compute_area()
    assert df_with_area.crs != 32198

    assert min(abs(df_with_area.area - df_with_area['area_projected'])) > 0