import random
from os.path import join, isdir
from os import mkdir
from shutil import rmtree
import string
import logging

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