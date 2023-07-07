from os import makedirs
from os.path import abspath, dirname, join
from pathlib import Path

HERE = Path(abspath(dirname(__file__)))

class ProjectPaths:
    PATH_PROJET_ROOT = HERE
    DATA_PATH = abspath(join(PATH_PROJET_ROOT, "data"))

    makedirs(DATA_PATH, exist_ok=True)



