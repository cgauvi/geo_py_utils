import branca
import folium
from folium.features import Map, GeoJson, GeoJsonTooltip
from folium import LayerControl
import json
import logging 
import numpy as np
import pandas as pd
from sklearn.base import TransformerMixin
import warnings

from geo_py_utils.geo_general.bbox import get_bbox_group_geometries, get_bbox_centroid


logger = logging.getLogger(__name__)
EPSILON = 10**-3

def get_default_map_fun_weight_numeric_data(
        df: pd.DataFrame, 
        col_numeric_name: str,
        default_size : int =1,
        scaler = lambda x: x,
        force_positive = True
):
    # QA
    if not col_numeric_name in df.columns:
        raise ValueError(f'Fatal error! {col_numeric_name} not in the columns!')
    
    if df[col_numeric_name].dtype == 'O':
        raise ValueError(f'Fatal error! {col_numeric_name} not numeric!')

    # Scaler 
    is_sklearn = isinstance(scaler, TransformerMixin)
    
    if scaler is None:
        scaler = lambda x: x
    elif is_sklearn:
        scaler = scaler.fit(df[[col_numeric_name]])
    
    def eval_sklearn (dat) : 
        with warnings.catch_warnings(action='ignore'):
            return scaler.transform(np.array(dat).reshape(-1,1))[0][0]
    min_val = eval_sklearn(df[col_numeric_name]).min()


    # Wrapper over scaler eval to force positive values
    def eval_scaler(dat):
        if is_sklearn:
            val= eval_sklearn(dat)
        else:
            val = scaler(dat)

        if force_positive and min_val <= 0:
            val = val + abs(min_val) + EPSILON

        return val
 
    # Actual function to return
    def _weight(feature):

        # column_color must be numeric
        val = (
            eval_scaler(feature['properties'][col_numeric_name])
            if feature['properties'][col_numeric_name] is not None
            else default_size
        )
        
        # Can get negative values after instantiating this function si best to keep this warning
        if val <= 0:
            logger.warning(f'Warning! non positive weights value {val} produced for the weight function')

        return val

    #QA
    weights = [_weight(f) for f in  json.loads(df.to_json())['features']]

    if min(weights) <= 0 and force_positive:
        raise RuntimeError("Fatal error! got non positive weights even after forcing positive")
    elif min(weights) <= 0 :
        logger.warning('Warning! non positive weights values produced for the weight function')

    return _weight

def get_default_map_fun_color_numeric_data(
        df: pd.DataFrame, 
        col_numeric_name: str,
        default_na_color: str='grey',
        color_map = branca.colormap.linear.YlGnBu_09
    ):

    # QA
    if not col_numeric_name in df.columns:
        raise ValueError(f'Fatal error! {col_numeric_name} not in the columns!')
    
    if df[col_numeric_name].dtype == 'O':
        raise ValueError(f'Fatal error! {col_numeric_name} not numeric!')
    
    # Change the scale appropriately 
    min_v, max_v = df[col_numeric_name].min(), df[col_numeric_name].max()

    if min_v == max_v:
        logger.warning(f'Warning {col_numeric_name} is constant! color map will return a single color!')

    cmap = color_map.scale(min_v, max_v )

    def color_(feature):
        # column_color must be numeric
        return (
            cmap(feature['properties'][col_numeric_name])
            if feature['properties'][col_numeric_name] is not None
            else default_na_color
        )
    
    return color_

def  map_simple(
        dict_shp,
        show=False,
        tootltip_fields=None,
        base_tiles="CartoDB positron",
        weight_edge= lambda x :3,
        opacity= lambda x :0.5,
        fill_color = lambda x : 'blue',
        color = lambda x : 'white',
        extra_to_add=None
    ) -> folium.Map:

    """Simple leaflet based on a dict of geodataframes

    Styler is common to all geodataframes in dict.
    opacity, weight_edge, fill_color and color are all functions of `features` in the geojson

    Args:
        dict_shp (dict[gpd.GeoDataFrame]): disct of geodataframes 
        show (bool, optional): Display the geodata when loading map - convenient with large geometries. Defaults to False. 
        tootltip_fields (_type_, optional): _description_. Defaults to None.
        base_tiles (str, optional): _description_. Defaults to "CartoDB positron".
        fill_color (fun, optional). Defaults to `lambda x : 'blue'`
        weight_edge (fun, optional): _description_. Defaults to `lambda x : 3`.
        opacity (fun, optional): _description_. Defaults to `lambda x :0.5`.
        extra_to_add (_type_, optional): extra leafet elements to add. Defaults to None.

    Returns:
        folium.Map: leaflet map 
    """
    
    # Generic styler function
    def styler(feature): 
        return {
                'color': color  (feature),
                'weight': weight_edge (feature),
                'fillOpacity': opacity (feature),
                'fillColor': fill_color (feature)
        }
    
    bbox_bounds = get_bbox_group_geometries(dict_shp, by_feature=False)
    lng_central, lat_central = get_bbox_centroid(bbox_bounds)

    m = Map(
        [lat_central, lng_central],
        tiles=base_tiles,
        zoom_start=10
    )

    # Iterate over dict
    for k, v in dict_shp.items():
        
        if tootltip_fields is not None:
            GeoJson(v.to_json(),
                    name=k,
                    style_function=styler,
                    show=show,
                    tooltip=GeoJsonTooltip(fields=tootltip_fields))\
                .add_to(m)
        else:
            GeoJson(v.to_json(),
                    name=k,
                    style_function=styler,
                    show=show).\
                add_to(m)

    if extra_to_add is not None:
        for extra in extra_to_add:
            extra.add_to(m)

    LayerControl().add_to(m)

    return m