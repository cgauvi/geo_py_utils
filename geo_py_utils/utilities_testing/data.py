from os.path import join
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, LineString, Point
from shapely import wkt
from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.census_open_data.open_data import DEFAULT_QC_CITY_NEIGH_URL

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
    STR_COL_NAME: ['some 8898 ', 'text é ç !"/ "123,526" '],
    MIXED_COL_NAME: [3,'mixed']
    }

GDF_BBOX_DONNACONNA = 

 
gpd.GeoDataFrame(
                    data = {'index':[0]},
                    geometry=wkt.loads('Polygon ((-72 46.39999999999999858, -71.5 46.39999999999999858, -71.5 46.70000000000000284, -72 46.70000000000000284, -72 46.39999999999999858))'),
                    crs=4326
                )

# This creates 2 triangular polygons over donnacona
SAMPLE_GDF =   gpd.GeoDataFrame(
                    data = BASE_DATA,
                    geometry=[
                        Polygon(((-71.5, 46.5), (-71.6, 46.4),  (-71.5, 46.7),   (-71.5, 46.5))) ,
                        Polygon(((-71.5, 46.5), (-72, 46.4),  (-71.5, 46.7),   (-71.5, 46.5)))
                    ],
                    crs=4326
                )

BASE_DATA_2 = BASE_DATA.copy()
BASE_DATA_2.update({ID_COL_NAME:[2,3], STR_COL_NAME: ['new1', 'new2']})
# same geometry as SAMPLE_GDF
SAMPLE_GDF_2 =   gpd.GeoDataFrame(
                    data = BASE_DATA_2,
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

# Class that holds the data on the quebec city neighborhood tables 
class QcCityTestData:
    SPATIAL_LITE_DB_PATH = join(DATA_DIR, "test.db")
    SPATIAL_LITE_TBL_QC = "qc_city_test_tbl"
    SQL_LITE_TBL_CSV_QC = 'csv_qc_city_test_tbl'
    QC_CITY_NEIGH_URL = DEFAULT_QC_CITY_NEIGH_URL
    SPATIAL_LITE_TBL_GEOMETRY_COL_NAME = 'GEOMETRY'


class LocalGDFData:
    SPATIAL_LITE_DB_PATH = join(DATA_DIR, "samplegdf_in.sqlite")
    SPATIAL_LITE_TBL_QC = "mytest"
    SPATIAL_LITE_TBL_GEOMETRY_COL_NAME = 'GEOMETRY'
    DATA = SAMPLE_GDF
 

EPS=10**-6
ext = [(0, 0), (0, 2), (0+EPS, 2), (0+EPS, 0), (0, 0)]
GDF_SLIVER = gpd.GeoDataFrame( pd.DataFrame({'index':[0]}), geometry=gpd.GeoSeries(Polygon(ext)))


ext = [(0, 0), (1, 1), (0, 1), (1, 0), (0, 0)]
GDF_INVALID_SELF_INTERSECTING= gpd.GeoDataFrame( pd.DataFrame({'index':[0]}), geometry=gpd.GeoSeries(Polygon(ext)))


ext = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
int = [(0.5, 0), (1.5, 0), (1.5, 1), (0.5, 1), (0.5, 0)]
GDF_INVALID_EDGE_TOUCHES_INTERIOR = gpd.GeoDataFrame( pd.DataFrame({'index':[0]}), geometry=gpd.GeoSeries( Polygon(ext, [int])))

# fixes the polygon by removing the interior ring completely
#GDF_EDGE_INTERIOR.buffer(0) 


ext = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
int = [(1, 0), (0, 1), (0.5, 1.5), (1.5, 0.5), (1, 0)][::-1]
GDF_INVALID_RING_TOUCHES_TWO_POINTS = gpd.GeoDataFrame( pd.DataFrame({'index':[0]}), geometry=gpd.GeoSeries( Polygon(ext, [int])))

#GDF_INVALID_RING_TOUCHES_TWO_POINTS.is_valid == False
#GDF_INVALID_RING_TOUCHES_TWO_POINTS.buffer(0).is_valid == True # fixes the polygon by spliting it into 2 parts
#GDF_INVALID_RING_TOUCHES_TWO_POINTS.geom_type == 'Polygon'
#GDF_INVALID_RING_TOUCHES_TWO_POINTS.buffer(0).geom_type == 'MultiPolygon'

LIST_INVALID_GDF = [GDF_INVALID_SELF_INTERSECTING, GDF_INVALID_EDGE_TOUCHES_INTERIOR, GDF_INVALID_RING_TOUCHES_TWO_POINTS ]



GDF_GRID =   gpd.GeoDataFrame(
                    data = {'index': [0,1,2,3]},
                    geometry=[
                        Polygon(((0,0), (0,1),  (1,1),   (0,1))) ,
                        Polygon(((0,1), (0,2),  (1,2),   (1,1))),
                        Polygon(((1,0), (1,1),  (2,1),   (2,0))),
                        Polygon(((1,1), (1,2),  (2,2),   (2,1)))
                    ],
                    crs=4326
                )

# One side completely aligned with GDF_GRID
# The other slightly off so as to create a sliver with area EPS and preimeter 2(EPS+1)
GDF_SQUARE_OVER_GRID =   gpd.GeoDataFrame(
                    data = {'index': [0,]},
                    geometry=[
                        Polygon(((0,0), (2-EPS,0),  (2-EPS,2),   (0,2))) 
                    ],
                    crs=4326
                )


GDF_EMPTY =   gpd.GeoDataFrame(
                    data = { },
                    geometry=[  ],
                    crs=4326
                )


GDF_LINE_OVER_SQUARE_EDGE = gpd.GeoDataFrame(
                    data = {'index': [0,]},
                    geometry=[  LineString([[0, 0], [1, 0]]) ],
                    crs=4326
                )


GDF_POINT_OVER_SQUARE_VERTEX = gpd.GeoDataFrame(
                    data = {'index': [0,]},
                    geometry=[  Point([0, 0]) ],
                    crs=4326
                )
