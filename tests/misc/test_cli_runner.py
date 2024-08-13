
import json
from os import path
from pathlib import Path, PurePosixPath, WindowsPath
import pytest
import sys

from geo_py_utils.misc.cli_runner import CLIRunner
from geo_py_utils.constants import ProjectPaths

@pytest.mark.windows_specific
def test_powershell_envvars():
    if 'win' in sys.platform:
        assert CLIRunner(use_bash=False).run_cmd(" ls ~ ") is None  # ~ resolved

@pytest.mark.windows_specific
def test_powershell_script_capture_stdout ():
    if 'win' in sys.platform:
        str_return=CLIRunner(use_bash=False, capture_std_out=True).run_cmd(" ls ~")
        assert len(str_return) > 0


@pytest.mark.windows_specific
def test_gitbash_fails_wrong_call():

    # Try to call a script with windows path within bash shell will fail
    if 'win' in sys.platform:
        with pytest.raises(Exception):
            success_script_path = Path(path.dirname(__file__ )) / 'success.sh'
            CLIRunner(use_bash=True).run_cmd(WindowsPath(success_script_path))

@pytest.mark.windows_specific
def test_gitbash_catch_exception():
    if 'win' in sys.platform:
        try:
            error_script_path = Path(path.dirname(__file__ )) / 'error.sh'
            CLIRunner(use_bash=True, capture_std_err=True).run_cmd(PurePosixPath(error_script_path))
        except Exception as e :
            print('This will fail because the error.sh scripts returns an error')
            assert  "error: Failure!" in str(e)

@pytest.mark.windows_specific
def test_gitbash_capture_stdout_windows():

    if 'win' in sys.platform:
 
        success_script_path = Path(path.dirname(__file__ )) / 'success.sh'

        dict_return = CLIRunner(use_bash=True, capture_std_out=True).run_cmd(PurePosixPath(success_script_path))
        str_return = json.loads(dict_return)['out'] # dict_return is a json string
        assert str_return.strip() == 'Success!'

        assert CLIRunner(use_bash=True).run_cmd(PurePosixPath(success_script_path)) is None




@pytest.mark.linux_specific
def test_gitbash_capture_stdout_linux():
    if 'win' not in sys.platform:
        success_script_path = Path(path.dirname(__file__ )) / 'success.sh'

        dict_return = CLIRunner(use_bash=True, capture_std_out=True).run_cmd(success_script_path)
        str_return = json.loads(dict_return)['out'] # dict_return is a json string
        assert str_return.strip() == 'Success!'
        assert CLIRunner(use_bash=True).run_cmd(success_script_path) is None


def test_multiline_statement_works_os_agnostic():


    multiline_cmd = f'echo {PurePosixPath(ProjectPaths.PATH_PROJET_ROOT)}'
    multiline_cmd += """
     | 
    awk -F  '/' '{print $NF}'
    """
    
    # Print `misc` in `geo_py_utils\tests\misc`
    out_mess = json.loads(CLIRunner(use_bash=True, capture_std_out=True).run_cmd(multiline_cmd))['out'].strip()
    assert out_mess == 'geo_py_utils'
       

if __name__ == '__main__':
    test_multiline_statement_works_os_agnostic()