from importlib import resources
from pathlib import PosixPath

def get_geohash_worst_dim_path() -> PosixPath:
    """Get the path to the csv with the largest size of geohashes by precision

    Returns:
        PosixPath: _description_
    """

    with resources.path("geo_py_utils.data", "geohash_worst_dimensions_meters.csv") as f:
        data_path = f
    
    return data_path


 