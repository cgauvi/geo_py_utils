import re
import unicodedata
from typing import List

def remove_accents(str_to_clean):
	
	# First round
	str_cleaned = str_to_clean.translate(
		str.maketrans(
			'ÁÀÂÃÄÅÇÉÈÊËÍÌÎÏÑÓÒÔÕÖØÚÙÛÜÝŸáàâãäåçéèêëíìîïñóòôõöøúùûüýÿœ',
			'AAAAAACEEEEIIIINOOOOOOUUUUYYaaaaaaceeeeiiiinoooooouuuuyyo'
		)
	)
	
	# Just to be sure 
	str_cleaned = unicodedata.normalize('NFKD', str_cleaned).encode('ASCII', 'ignore').decode("ASCII")

	return str_cleaned


def remove_apostrophe(s: str) -> str:
    """Remove all apostrophes: '’‘"

    Args:
        s (str): string to parse that potentially contains apostrophe-like symbols that will fuck up your sql statement and need to be escaped

    Returns:
        str: string without any apostrophes that should run in a sql statement
    """
    return re.sub(  pattern= "[\'’‘\"]", repl="", string = s)


def build_sql_string_in(list_vals: List[str]) -> str:
    """Create a sql statement to test for membership in a group of string.

    Drops duplicates

    build_sql_string_in(['bla','bla', 'blo']) -> ('bla','blo')

    Args:
        list_vals (List[str]): list of strings

    Returns:
        str: single string for sql
    """
    return "({})".\
                    format( 
                            ",".join(
                                [f"'{(str(s))}'" \
                                    for s in set(list_vals)
                                ])
                    )

def build_sql_select_with_fct(var_name: str, dict_fct: dict = {'MAX': '_max'}, wrapper = lambda x: x) -> str:
        
    """Create a sql select statement based on dict of sql functions (e.g. MAX) along with a suffix to add to the new alias (e.g. _max).

    Details: 
        For each dictionary element:
            1. Creates a new var with suffix dict_fct['value'] -> `{var}_{dict_fct['value']}`
            2. and value given by applying a function -> `dict_fct['key'](var_name)`
            3. Optional wrapper that takes a string and returns a string

    Example: 
        build_sql_select_with_fct("bla", {'MAX': '_max'}) = "MAX(bla) as bla_max"
        build_sql_select_with_fct("bla", {'MAX': '_max'}, wrapper = lambda x :  'CASE WHEN x in (..) THEN blo ELSE NULL END') = "MAX(CASE WHEN bla in (..) THEN blo ELSE NULL END) as bla_max"

    Args:
        var_name (str): string representing variable name
        dict_fct (dict): dict with sql function to apply (key) and variable name suffix injected to create the new var (value). Defaults to {'MAX': '_max'}.
        wrapper (function): wrapper function that should take a string and return a string. Defaults to identity (does nothing)

     Returns:
        str: string to use in the `SELECT ... `statement

    """

    str_sql = ",".join(
                    [
                    f"{k}({wrapper(var_name)}) as {var_name}{v}" for k, v in dict_fct.items()
                    ]
    )

    return str_sql

