
from os.path import join, dirname, abspath, isdir
from pathlib import Path

ROOT_DIR = join(dirname(abspath(__file__)), "..")
DATA_DIR = join(ROOT_DIR,"data")
DEFAULT_GIT_BASH_PATH = Path("C:\\") / "Program Files" / "Git" / "bin" / "bash.exe"

assert isdir(ROOT_DIR) & isdir(DATA_DIR)