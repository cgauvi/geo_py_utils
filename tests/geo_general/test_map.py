# %% 
import branca
import folium
import json 
import pandas as pd
import pytest
from sklearn.preprocessing import QuantileTransformer, StandardScaler

from geo_py_utils.geo_general.map import (
    get_default_map_fun_color_numeric_data, 
    map_simple,
    get_default_map_fun_weight_numeric_data
)
from geo_py_utils.utilities_testing.data import  SAMPLE_GDF_SAFE, SAMPLE_GDF_2


def test_default_mapping_fun_generates_colors_on_numeric_with_custom_cm():

    # Works on numeric with step color map 
    step = branca.colormap.StepColormap(
        ["green", "yellow", "red"],  caption="step"
    )
    
    for cc in ['integer_data', 'real_data']:
        fun = get_default_map_fun_color_numeric_data(SAMPLE_GDF_SAFE, cc, color_map=step)
        features = json.loads(SAMPLE_GDF_SAFE.to_json())['features']
        colors = [ fun(feat) for feat in features]
        assert len(set(colors)) == SAMPLE_GDF_SAFE[cc].unique().size

    # Fails on text
    with pytest.raises(ValueError):
        _ = get_default_map_fun_color_numeric_data(SAMPLE_GDF_SAFE, 'text_data')        

    # Works on const value but only returns a single color with linear color map 
    linear = branca.colormap.LinearColormap(
        ["blue", "white", "red"], 
    )
    SAMPLE_GDF_SAFE['const'] = 1
    fun = get_default_map_fun_color_numeric_data(SAMPLE_GDF_SAFE, 'const', color_map=linear)
    features = json.loads(SAMPLE_GDF_SAFE.to_json())['features']
    colors = [ fun(feat) for feat in features]
    assert len(set(colors)) == 1


def test_default_mapping_fun_generates_colors_on_numeric_all_layers():

    # To get a color map considering all layers, pass in all layers 

    assert (
        'id' in SAMPLE_GDF_SAFE.columns and 
        'id' in SAMPLE_GDF_2.columns  and 
        len( set([*SAMPLE_GDF_SAFE['id'].values, *SAMPLE_GDF_2['id'].values])) == SAMPLE_GDF_SAFE.shape[0] + SAMPLE_GDF_2.shape[0] 
    )
    fun = get_default_map_fun_color_numeric_data(
        pd.concat([SAMPLE_GDF_SAFE,SAMPLE_GDF_2]),
        col_numeric_name='id',
    )

    features = json.loads(SAMPLE_GDF_SAFE.to_json())['features']
    colors = [ fun(feat) for feat in features]
    assert len(set(colors)) == SAMPLE_GDF_SAFE.shape[0]

    features = json.loads(SAMPLE_GDF_2.to_json())['features']
    colors = [ fun(feat) for feat in features]
    assert len(set(colors)) == SAMPLE_GDF_2.shape[0]


def test_leaflet_generated_on_categorical_after_tweak_with_def_weight():

    SAMPLE_GDF_SAFE['text_data_numeric'] = (
        (SAMPLE_GDF_SAFE['text_data'].astype('category').cat.codes)+1 * 10
    )

    # For cateorical data, use set color map
    cm_set = branca.colormap.linear.Set3_07
    fun_maper_color = get_default_map_fun_color_numeric_data(SAMPLE_GDF_SAFE, 'text_data_numeric', color_map=cm_set)

    num_quants = 2
    weight_fun = get_default_map_fun_weight_numeric_data(
        pd.concat([SAMPLE_GDF_SAFE, SAMPLE_GDF_2]),
        col_numeric_name='id',
        scaler = QuantileTransformer(n_quantiles=num_quants)
    )

    weights = [weight_fun(f) for f in  json.loads(SAMPLE_GDF_SAFE.to_json())['features']]
    assert len(weights) == num_quants

    m=map_simple(
        {'d': SAMPLE_GDF_SAFE},
        color= fun_maper_color,
        weight_edge = weight_fun
    )
    assert isinstance(m, folium.folium.Map)

def test_leaflet_generated_on_numerical_std_scaler_works():
     
    weight_fun = get_default_map_fun_weight_numeric_data(
        pd.concat([SAMPLE_GDF_SAFE, SAMPLE_GDF_2]),
        col_numeric_name='id',
        scaler = StandardScaler()
    )

    weights = [weight_fun(f) for f in  json.loads(SAMPLE_GDF_SAFE.to_json())['features']]
    assert (len(set(weights)) == SAMPLE_GDF_SAFE.shape[0]) 

    m=map_simple(
        {'d': SAMPLE_GDF_SAFE},
        weight_edge = weight_fun
    )
    assert isinstance(m, folium.folium.Map)

 