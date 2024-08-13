
import distutils.spawn
import json
from pathlib import Path
import subprocess
import sys
from typing import Union

from geo_py_utils.misc.constants import DEFAULT_GIT_BASH_PATH

 
class CLIStdErrOut:
    """
    Basic class to provide backward compatibility when priting the results of CLIRunner().run_cmd()

    Attributes:
        errs (str, optional): output of stderrr. Defaults to None
        outs (str, optional): output of stdout. Defaults to None
    """
    def __new__(cls, err: str = None, out: str = None):
        if err is None and out is None:
            return None
        
        obj = json.dumps({'out': out , 'err': err})
        return obj 



class CLIRunner:

    """Basic convenience class to run a CLI on windows or linux using subprocess.
        
    Goal is to facilitate compatibility with initial unix code

    With powershell(windows), you can run pretty much any command directly: 
    For instance: 
    
    - `WindowsBashRunner(use_bash=False).run_cmd(" ls .")` will list the current dir
    - `WindowsBashRunner(use_bash=False).run_cmd(f" ls $PATH_TO_GIC")` will work with an environment variable PATH_TO_GIC

    With git-bash (windows), you can run commands as you would in the git-bash terminal so this is reproducible.  
    Very finicky so not used by default. Works when calling scripts (.sh files) - e.g. 
    -  `WindowsBashRunner(use_bash=True).run_cmd(Path(path.dirname(__file__ )) / 'success.sh')`

    Attributes:
        git_bash_path (Path, optional): _description_. Defaults to DEFAULT_GIT_BASH_PATH.
        use_bash (bool, optional): Use git bash if True, else powershell. Defaults to True.
        capture_std_out (bool, optional): Will return a string if true representing output, else prints to stdout. Defaults to False.
        capture_std_err (bool, optional): Will add the error message to the ValueError if true, else prints to stderr. Defaults to False.
    """

    def __init__(
        self, 
        git_bash_path: Path = DEFAULT_GIT_BASH_PATH,
        use_bash = True,
        capture_std_out=False,
        capture_std_err=False
        ):


        self.git_bash_path = git_bash_path
        self.use_bash = use_bash

        self.capture_std_out = capture_std_out
        self.capture_std_err = capture_std_err

        self.stdout = subprocess.PIPE if capture_std_out else sys.stdout
        self.stderr = subprocess.PIPE if capture_std_err else sys.stderr


       
    def check_windows_git_bash(self):
        """
        Make sure the bash util can be found

        Raises:
            FileNotFoundError: _description_
        """

        gitbash_found = distutils.spawn.find_executable(self.git_bash_path) is not None
        if not gitbash_found:
            raise FileNotFoundError(strerror=f'Fatal error! gitbash not found at path {self.git_bash_path}')


    def run_cmd(self, cmd: str) -> Union[CLIStdErrOut,None]:
        """
        Run the CLI command

        Args:
            cmd (str): cli cmd to run

        Raises:
            ValueError: _description_

        Returns:
            output if `capture_std_out=True`
        """

        if 'win' in sys.platform:
            if self.use_bash:
                self.check_windows_git_bash()

            if self.use_bash:
                shell_exe = self.git_bash_path
                shell_name = 'bash'

                cmd_list = [shell_exe, '-c' , cmd]
            else:
                shell_exe = 'powershell'
                shell_name = 'powershell'

                cmd_list = [shell_exe, cmd]
        else:
            shell_name = 'bash_unix'
            cmd_list = [cmd]
        
        p = subprocess.Popen(   
                    cmd_list,
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
                raise ValueError(f'Fatal error trying to run {sys.platform} cmd `{cmd}` with shell {shell_name} - error: {errs}')
            else:
                raise ValueError(f'Fatal error trying to run  {sys.platform} cmd `{cmd}` with shell {shell_name}')

        return CLIStdErrOut(out=outs,err=errs)

if __name__ == '__main__':

    # %%
    if 'win' in sys.platform:
        rez=CLIRunner(use_bash=False, capture_std_out=True).run_cmd(f" ls {Path(__file__)}")
       
        print(
            f'''
            Results of printing entire results of `ls .`
            {CLIRunner(use_bash=False).run_cmd(" ls .") }  
            '''
        )
        

        dict_out=CLIRunner(capture_std_out=True, capture_std_err=True).run_cmd(f" ls '{Path(__file__)}'")
        str_out = json.loads(dict_out)['out']
        str_err = json.loads(dict_out)['err']
        print(
          f'''
            Results of printing entire results of `ls cli_runner.py`
            out: {str_err}
            err: {str_err}
            '''
        )


    elif 'linux' in sys.platform:
        str_out=CLIRunner(use_bash=False, capture_std_out=True).run_cmd(f" ls {Path(__file__)}")
        CLIRunner(use_bash=False).run_cmd(" ls .")
     
# %%
