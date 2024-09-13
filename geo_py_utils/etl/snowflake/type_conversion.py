import pyarrow as pa
import pandas as pd

#Types used for table creation - e.g. `CREATE TABLE bla (foo INT, bar DATE, baz VARCHAR)`
#Note that `select data_type from from information_schema.columns` will return `NUMBER, DATE and TEXT`
DICT_MAP_PA_SFKL_TYPES ={
    pa.uint8():  "NUMBER",
    pa.uint16():  "NUMBER",
    pa.uint32():  "NUMBER",
    pa.uint64():  "NUMBER",
    pa.int8():  "NUMBER",
    pa.int16():  "NUMBER",
    pa.int32():  "NUMBER",
    pa.int64():  "NUMBER",
    pa.string(): "TEXT",
    pa.float16() :"FLOAT",
    pa.float32():"FLOAT",
    pa.float64():"FLOAT",
    pa.date32():"DATE",
    pa.date64():"TIMESTAMP"
}

# TODO: find a better way to deal with time zone: other than hard coding a finite set
for unit_tz in ['s','ms','us','ns']:
    for tz in [None, 'UTC', 'America/New_York']:
        DICT_MAP_PA_SFKL_TYPES.update({ pa.timestamp(unit_tz, tz): "TIMESTAMP" })

DF_MAP_PA_SFKL_TYPES = pd.DataFrame.from_dict(
    DICT_MAP_PA_SFKL_TYPES, 
    orient='index',
    columns=['sfkl_type']
).\
    reset_index().\
    rename(columns={'index':'pa_type'})