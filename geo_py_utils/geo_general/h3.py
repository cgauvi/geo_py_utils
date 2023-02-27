import geopandas as gpd
import h3

from geo_py_utils.geo_general.geo_utils import get_geodataframe_from_list_coord


def get_h3_hex_from_gpd(shp: gpd.GeoDataFrame,
                        resolution: int = 13) -> gpd.GeoDataFrame:

    assert isinstance(shp, gpd.GeoDataFrame)

    # Convert to geographic coordinates 
    shp_geo = shp.to_crs(4326)

    # Get the h3 indices from the lat,lng
    h3_indices = [h3.geo_to_h3(p.y, p.x, resolution=resolution)
                  for p in shp_geo.geometry.centroid]
    list_coords =  [h3.h3_to_geo_boundary(h=hindex, geo_json=True) for hindex in h3_indices]

    # Convert to geodataframe 
    shp_h3 = get_geodataframe_from_list_coord(list_coords, crs=4326) 

    # Add the h3 index
    shp_h3['h3_index'] = h3_indices

    return shp_h3
