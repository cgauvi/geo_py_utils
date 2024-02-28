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


