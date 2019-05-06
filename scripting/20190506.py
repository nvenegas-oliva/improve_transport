
import pandas as pd
from datetime import datetime
import fastparquet

path = "datasets/test-folder/20190311/home/telefonica/shared/filtered_Lst_Usos_20190303_20190305.CSV"

dtype = {
    'CODIGOENTIDAD': 'int64',
    'NOMBREENTIDAD': 'str',
    'CODIGOSITIO': 'int64',
    'NOMBRESITIO': 'str',
    'NROTARJETA': 'str'
}
df = pd.read_csv(
    path,
    encoding="cp1252",
    sep=";",
    usecols=range(6),
    dtype=dtype)

df.head()


df.columns = [column.lower() for column in df.columns]
# df["fechahoratrx"] = df["fechahoratrx"].apply(lambda a: datetime.strptime(
#    a, "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"))

df["fechahoratrx"] = pd.to_datetime(df["fechahoratrx"], format="%d/%m/%Y %H:%M:%S", errors='coerce')

fastparquet.write(
    filename="datasets/data.parquet",
    data=df,
    compression="GZIP",
    file_scheme='hive',
    write_index=False,
    object_encoding='utf8',
    times='int96')

help(fastparquet.write)
