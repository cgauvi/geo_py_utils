from pathlib import Path
import distutils.spawn
import subprocess
import sys
from pathlib import PurePosixPath
 

from geo_py_utils.misc.constants import DEFAULT_GIT_BASH_PATH

class WindowsCLIRunner:

    """Basic convenience class to run a command using bash in windows.
        
    Goal is to facilitate compatibility with initial unix code

    With powershell, you can run pretty much any command directly: 
    For instance: 
    
    - `WindowsBashRunner(use_bash=False).run_cmd(" ls .")` will list the current dir
    - `WindowsBashRunner(use_bash=False).run_cmd(f" ls $PATH_TO_GIC")` will work with an environment variable PATH_TO_GIC

    With git-bash, you can run commands as you would in the git-bash terminal so this is reproducible.  
    Very finicky so not used by default. Works when calling scripts (.sh files) - e.g. 
    -  `WindowsBashRunner(use_bash=True).run_cmd(Path(path.dirname(__file__ )) / 'success.sh')`

    Args:
        git_bash_path (Path, optional): _description_. Defaults to DEFAULT_GIT_BASH_PATH.
        use_bash (bool, optional): Use git bash if True, else powershell. Defaults to True.
        capture_std_out (bool, optional): Will return a string if true representing output, else prints to stdout. Defaults to False.
        capture_std_err (bool, optional): Will add the error message to the ValueError if true, else prints to stderr. Defaults to False.
    """

    def __init__(
        self, 
        git_bash_path: Path = DEFAULT_GIT_BASH_PATH,
        use_bash = False,
        capture_std_out=False,
        capture_std_err=False
        ):


        self.git_bash_path = git_bash_path
        self.use_bash = use_bash

        self.capture_std_out = capture_std_out
        self.capture_std_err = capture_std_err

        self.stdout = subprocess.PIPE if capture_std_out else sys.stdout
        self.stderr = subprocess.PIPE if capture_std_err else sys.stderr

        if use_bash:
            self.check_windows_git_bash()
       
    def check_windows_git_bash(self):
        """
        Make sure the bash util can be found

        Raises:
            FileNotFoundError: _description_
        """

        gitbash_found = distutils.spawn.find_executable(self.git_bash_path) is not None
        if not gitbash_found:
            raise FileNotFoundError(strerror=f'Fatal error! gitbash not found at path {self.git_bash_path}')


    def run_cmd(self, cmd: str) -> str:
        """
        Run the CLI command in Windows

        Args:
            cmd (str): cli cmd to run

        Raises:
            ValueError: _description_

        Returns:
            output if `capture_std_out=True`
        """

        if self.use_bash:
            shell_exe = self.git_bash_path
            shell_name = 'bash'
            cmd = PurePosixPath(f"{cmd}")
        else:
            shell_exe = 'powershell'
            shell_name = 'powershell'
            cmd = f'{cmd}'
        
        p = subprocess.Popen(   
                    [shell_exe, cmd],
                    stdout=self.stdout,
                    stderr=self.stderr,
                    shell=True,
                    universal_newlines=True
                    )

        outs, errs = p.communicate()
        code = p.returncode

        if errs is not None:
            try:
                errs = errs.decode().strip()
            except:
                pass
        
        if outs is not None:
            try:
                outs = outs.decode().strip()
            except:
                pass   

        if code != 0:
            if self.capture_std_err:
                raise ValueError(f'Fatal error trying to run windows cmd `{cmd}` with shell {shell_name} - error: {errs}')
            else:
                raise ValueError(f'Fatal error trying to run windows cmd `{cmd}` with shell {shell_name}')

        return outs

if __name__ == '__main__':


    str_out=WindowsCLIRunner(use_bash=False, capture_std_out=True).run_cmd(f" ls {Path(__file__)}")


    WindowsCLIRunner(use_bash=False).run_cmd(" ls .")
    
     