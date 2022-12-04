import branca
import folium
from folium.features import Map, GeoJson, GeoJsonTooltip
from folium import LayerControl
import pandas as pd

from geo_py_utils.geo_general.bbox import get_bbox_group_geometries, get_bbox_centroid


def map_simple(dict_shp,
               show=False,
               tootltip_fields=None,
               base_tiles="CartoDB positron",
               cmap_shared_all_layers=True,
               column_color=None,
               cmap=branca.colormap.linear.viridis.scale,
               weight_edge=3,
               opacity=0.5,
               extra_to_add=None) -> folium.Map:

    """Simple leaflet based on a dict of geodatframes

    Args:
        dict_shp (_type_): _description_
        show (bool, optional): _description_. Defaults to False.
        tootltip_fields (_type_, optional): _description_. Defaults to None.
        base_tiles (str, optional): _description_. Defaults to "CartoDB positron".
        cmap_shared_all_layers (bool, optional): _description_. Defaults to True.
        column_color (_type_, optional): _description_. Defaults to None.
        cmap (_type_, optional): _description_. Defaults to branca.colormap.linear.viridis.scale.
        weight_edge (int, optional): _description_. Defaults to 3.
        opacity (float, optional): _description_. Defaults to 0.5.
        extra_to_add (_type_, optional): _description_. Defaults to None.

    Returns:
        folium.Map: _description_
    """

    bbox_bounds = get_bbox_group_geometries(dict_shp, by_feature=False)
    lng_central, lat_central = get_bbox_centroid(bbox_bounds)

    m = Map([lat_central, lng_central],
            tiles=base_tiles,
            zoom_start=10)

    # Iterate over dict
    for k, v in dict_shp.items():

        # Color map normalization
        if not cmap_shared_all_layers:
            shp_color = v
        else:
            shp_color = pd.concat(dict_shp)

        # Color map (discrete or continuous)
        if column_color is not None:
            colorscale = cmap(
                shp_color[column_color].min(),  shp_color[column_color].max())

        # Style function
        if column_color is None:
            def styler(feature): return {
                'color': 'white',
                'fillColor': 'blue',
                'weight': weight_edge,
                'fillOpacity': opacity}
        elif column_color is not None:
            def styler(feature): return {
                'color': 'white',
                'weight': weight_edge,
                'fillOpacity': opacity,
                'fillColor':  (colorscale(feature['properties'][column_color])
                               if feature['properties'][column_color] is not None
                               else 'grey'
                               )
            }

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