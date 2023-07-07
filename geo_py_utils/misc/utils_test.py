import random
from os.path import join, isdir
from os import mkdir
from shutil import rmtree
import string

from geo_py_utils.misc.constants import DATA_DIR


def create_mock_cache_dir() -> str:
    """Create a new subdir for tests

    Returns:
        str: name of random dir
    """

    if not isdir(DATA_DIR): raise ValueError(f'Fatal error! cannot create random subdir of {DATA_DIR} - parent dir does not exist')

    random_string = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))
    mocked_data_dir = join(DATA_DIR, random_string)
    if isdir(mocked_data_dir): rmtree(mocked_data_dir)
    mkdir(mocked_data_dir)

    return mocked_data_dir

