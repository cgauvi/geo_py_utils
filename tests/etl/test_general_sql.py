from geo_py_utils.etl.general_sql import remove_apostrophe, build_sql_select_with_fct, remove_accents


def test_remove_apostrophe():
 
    assert remove_apostrophe("Some test:'’‘") == "Some test:" # replace all 3 kinds
    assert remove_apostrophe("Some test:''")  == "Some test:" # 
    assert remove_apostrophe("Some test:''") == "Some test:" # 
    assert remove_apostrophe("Some test:\"") == "Some test:" # 


def test_sql_build_fct():
    assert build_sql_select_with_fct("bla", {'MAX': '_max'}) == "MAX(bla) as bla_max"

    assert build_sql_select_with_fct(
        "bla", 
        {'MAX': '_max'}, 
        wrapper = lambda x :  f"CASE WHEN {x} = '' THEN 'blo' ELSE NULL END"
    ) == "MAX(CASE WHEN bla = '' THEN 'blo' ELSE NULL END) as bla_max"


def test_remove_accents():
    assert remove_accents('ÁÀ ça ') == 'AA ca '
    assert remove_accents(' no accents ') == ' no accents '


if __name__ == '__main__':

    test_remove_apostrophe()

   