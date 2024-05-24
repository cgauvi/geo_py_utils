import logging
import re
import sys

from geo_py_utils.misc.cli_runner import CLIRunner

logger = logging.getLogger(__file__)



def ogr2ogr_windows_wrapper(orig_ogr_cmd: str):
    """
    ogr2ogr_windows_wrapper Make sure we can find the ogr2ogr executable before calling an ogr2ogr command
    
    1. Fetch the full path (will depend on the python virtual env used)
    2. Once we have the full path, replace the ogr2ogr command in the original cmd string
    3. Call powershell with the new command (git bash fails with the \\ returned)

    On linux/mac, this will just run the regular original command in the shell (usually finds the executable without any issues, even in subshells and with virt envs)

    Args:
        orig_ogr_cmd (str): _description_

    Raises:
        ValueError: _description_
    """

    orig_ogr_cmd="".join(orig_ogr_cmd.splitlines())

    if re.search('.*ogr2ogr.*', orig_ogr_cmd, flags=re.IGNORECASE) is None:
        raise ValueError(f'Fatal error! cmd {orig_ogr_cmd} does not call ogr2ogr')
    
    if 'win' in sys.platform:

        # First try to disable reverting back to base env in subshells (if using conda for virt env)
        try:
            conda_cmd = 'conda config --set auto_activate_base false'
            CLIRunner(use_bash=False).run_cmd(conda_cmd)
        except Exception as e:
            print(f'Failed to disable reverting back to base conda env -{e} ')

        # Get the path (which can change depending on the virtual env used or not and wheter it is activated or not)
        # Particularly important if we run subcomands in subshells
        path_ogr_powershell = CLIRunner(use_bash=False, capture_std_out=True).run_cmd('(get-command ogr2ogr).Path').strip()
        if path_ogr_powershell is None or len(path_ogr_powershell) == 0:
            raise RuntimeError('Fatal error! could not run (get-command ogr2ogr).Path')
        orig_ogr_cmd_stripped = re.sub('ogr2ogr', '', orig_ogr_cmd, count=1,flags=re.IGNORECASE)
        cmd = f'{path_ogr_powershell} {orig_ogr_cmd_stripped}'
    else:
        cmd = orig_ogr_cmd

    # Final ogr2ogr call
    logger.info(cmd)
    CLIRunner(use_bash = False).run_cmd(cmd)