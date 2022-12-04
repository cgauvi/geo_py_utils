
 
from os.path import join, dirname, abspath, isdir
 

ROOT_DIR = join(dirname(abspath(__file__)), "..")
DATA_DIR = join(ROOT_DIR,"data")


assert isdir(ROOT_DIR) & isdir(DATA_DIR)