import pandas as pd

def table_exists(conn, tbl_name) -> bool:
            
    query_tbl_exists = f"""
        select to_boolean(count(1)) as tbl_exists
        from information_schema.tables 
        where table_schema = '{str(conn.schema).upper()}' and 
        table_name = '{str(tbl_name).upper()}';
    """

    df_results = pd.read_sql(query_tbl_exists, conn)

    return df_results.TBL_EXISTS.values[0]


if __name__ == '__main__':
    import dotenv
    from pathlib import Path
    from os.path import abspath, dirname, exists

    from geo_py_utils.etl.snowflake.snowflake_connect import connnect_snowflake_ext_browser
    
    HERE = Path(abspath(dirname(__file__)))  / '..' / '..' / '..'

    path_config = HERE / "config" / "snowflake" / ".env"
    if not exists(path_config):
        raise FileNotFoundError()
    
    dotenv.load_dotenv(path_config, override= True)
    with connnect_snowflake_ext_browser() as conn:
        tbl_exists = table_exists(conn, 'tmp_geo_qc_neigh')

    print(tbl_exists)