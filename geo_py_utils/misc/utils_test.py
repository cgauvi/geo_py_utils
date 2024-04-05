import logging

from os.path import join, isdir
from os import mkdir
import pandas as pd
import geopandas as gpd
import random
from shapely.geometry import Polygon
from shutil import rmtree
import string


from geo_py_utils.misc.constants import DATA_DIR

logger = logging.getLogger(__file__)

class MockTmpCache:
    """Callable class to create a tmp mock cache dir at init and remove it at teardown
    """

    def __init__(self, path_to_mock=DATA_DIR):
        self.path_to_mock = path_to_mock
        self.mocked_dir = self._create_mock_cache_dir()
        
    def __enter__(self):
        if not isdir(self.mocked_dir): mkdir(self.mocked_dir)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if isdir(self.mocked_dir): rmtree(self.mocked_dir)
        except Exception as e:
            logger.warn(f'Warning! Couldnt delete mocked cache dir: {self.mocked_dir} at teardown: prob windows permission issue: {e}')


    def _create_mock_cache_dir(self) -> str:
        """Create a new subdir for tests

        Returns:
            str: name of random dir
        """

        if not isdir(self.path_to_mock):
            raise ValueError(f'Fatal error! cannot create random subdir of {self.path_to_mock} - parent dir does not exist')

        random_string = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))
        mocked_data_dir = join(self.path_to_mock, random_string)

        try:
            if isdir(mocked_data_dir): rmtree(mocked_data_dir)
            mkdir(mocked_data_dir)
        except Exception as e:
            logger.warn(f'Warning! Couldnt delete mocked cache dir: {mocked_data_dir} at teardown: prob windows permission issue: {e}')

        return mocked_data_dir

# Easier to all have in caps, but for tests, useful
ID_COL_NAME= ' id'
INT_COL_NAME = 'integer_data'
REAL_COL_NAME = 'real_data'
STR_COL_NAME = 'text_data'
MIXED_COL_NAME = 'mixed_data'
GEOG_COL_NAME = 'GEOGRAPHY'

SFKL_EXPECTED_MAPPING = {
    ID_COL_NAME: "FIXED",
    INT_COL_NAME: "FIXED",
    REAL_COL_NAME: 'REAL',
    STR_COL_NAME: 'TEXT',
    MIXED_COL_NAME: 'TEXT',
    GEOG_COL_NAME: 'GEOGRAPHY'
}

BASE_DATA =  {
    ID_COL_NAME: [0, 1],
    REAL_COL_NAME: [0.152, 0.1],
    INT_COL_NAME: [8,7],
    STR_COL_NAME: ['some 8898 ', 'text é ç !"/ '],
    MIXED_COL_NAME: [3,'mixed']
    }

SAMPLE_GDF =   gpd.GeoDataFrame(
                    data = BASE_DATA,
                    geometry=[
                        Polygon(((-71.5, 46.5), (-71.6, 46.4),  (-71.5, 46.7),   (-71.5, 46.5))) ,
                        Polygon(((-71.5, 46.5), (-72, 46.4),  (-71.5, 46.7),   (-71.5, 46.5)))
                    ],
                    crs=4326
                )

SAMPLE_GDF_SAFE = SAMPLE_GDF.drop(columns=[MIXED_COL_NAME])
     
SAMPLE_DF = pd.DataFrame(
    data = BASE_DATA
    )

SAMPLE_DF_SAFE = SAMPLE_DF.drop(columns=[MIXED_COL_NAME])

# All of US polygon (including puerto ric, us virgin islands)
# Source: 'https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_nation_5m.zip'
# Hardcode polygon to avoid ssl and firewall issues
WKT_US_CVX = 'POLYGON ((-168.151555 -14.552548999999999, -170.759768 -14.37374, -170.846854 -14.322885999999999, -179.14733999999999 51.276781, -178.858248 51.820966, -171.743398 63.782970999999996, -166.838969 68.337186, -166.224187 68.873175, -166.21443299999999 68.879524, -162.302314 70.204227, -162.214989 70.230361, -162.046276 70.280699, -161.879266 70.329269, -159.585714 70.80947499999999, -159.528682 70.820849, -156.62014 71.34420899999999, -156.56865 71.352561, 173.107249 52.993227999999995, 179.66332699999998 52.022940999999996, 179.773922 51.970693, 179.77847 51.962216999999995, 179.467581 51.371629, 144.755664 13.276696, 144.723803 13.246122, 144.652096 13.230492, -168.143595 -14.552403, -168.151555 -14.552548999999999))'
US_ALL_CVX_GDF = gpd.GeoDataFrame(
    data= {"name":['us_all_cvx']}, 
    geometry=gpd.GeoSeries.from_wkt([WKT_US_CVX]),
    crs=4326
    )

# CONUS
# https://www.arcgis.com/home/item.html?id=b07a9393ecbd430795a6f6218443dccc - downloaded - filtered not Hawaii, Alaska - dissolved - convex hull
WKT_US_CONUS = 'POLYGON ((-80.58781927144103 24.956376350707178, -97.43460728713083 25.84555735153529, -98.20097928784458 26.055732351731034, -118.42010630667511 32.806114358017815, -120.16738630830238 33.92416235905908, -120.23854830836866 34.01088535913985, -120.64129330874374 34.57233735966274, -121.88227730989951 36.306943361278215, -123.72190131161278 38.92477136371626, -124.34530631219337 40.252430364952744, -124.39263831223747 40.43523736512299, -124.55961731239297 42.83245736735557, -124.73276931255424 48.149989372307914, -124.71717531253971 48.37755737251986, -122.76511931072172 48.99974637309931, -95.15186728500487 49.37173037344575, -69.23029626086353 47.4533343716591, -68.23080725993267 47.352148371564866, -67.79101125952309 47.06100337129372, -66.96927125875777 44.82865536921469, -80.35566527122482 25.158233350895173, -80.58781927144103 24.956376350707178))'
US_CONUS_CVX_GDF = gpd.GeoDataFrame(
    data= {"name":['us_conus_cvx']}, 
    geometry=gpd.GeoSeries.from_wkt([WKT_US_CONUS]),
    crs=4326
)