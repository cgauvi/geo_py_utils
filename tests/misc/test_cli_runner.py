
from pathlib import Path
from os import path
from geo_py_utils.misc.cli_runner import CLIRunner
import pytest
import sys

@pytest.mark.windows_specific
def test_powershell_envvars():
    if 'win' in sys.platform:
        assert CLIRunner(use_bash=False).run_cmd("ls ~") is None # no output

@pytest.mark.windows_specific
def test_powershell_script_capture_stdout ():
    if 'win' in sys.platform:
        str_return=CLIRunner(use_bash=False, capture_std_out=True).run_cmd(" ls ~")
        assert len(str_return) > 0

@pytest.mark.windows_specific
def test_gitbash_fails_wrong_call():
    if 'win' in sys.platform:
        #'This will fail with No such file or directory: trying to find `ls xxx`'
        with pytest.raises(Exception):
            CLIRunner(use_bash=True).run_cmd(f" ls ~")

@pytest.mark.windows_specific
def test_gitbash_capture_stderr():
    if 'win' in sys.platform:
        try:
            error_script_path = Path(path.dirname(__file__ )) / 'error.sh'
            CLIRunner(use_bash=True, capture_std_err=True).run_cmd(error_script_path)
        except Exception as e :
            print('This will fail')
            assert  "error: Failure!" in str(e)

@pytest.mark.windows_specific
def test_gitbash_capture_stdout():
    if 'win' in sys.platform:
        success_script_path = Path(path.dirname(__file__ )) / 'success.sh'

        str_return = CLIRunner(use_bash=True, capture_std_out=True).run_cmd(success_script_path)
        assert str_return.strip() == 'Success!'
        assert CLIRunner(use_bash=True).run_cmd(success_script_path) is None



