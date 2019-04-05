
from io import BytesIO, StringIO
from boto3 import resource
from zipfile import ZipFile
from datetime import datetime
import pandas as pd
import re


def load_trx(csv):
    """
    Load .csv transactions dataset in pandas DataFrame.
    """
    dtype = {
        'CODIGOENTIDAD': 'int',
        'NOMBREENTIDAD': 'object',
        'CODIGOSITIO': 'int',
        'NOMBRESITIO': 'object',
        'NROTARJETA': 'object'
    }
    df = pd.read_csv(BytesIO(csv), sep=";", encoding="cp1252",
                     usecols=[i for i in range(6)], dtype=dtype)
    df["FECHAHORATRX"] = pd.to_datetime(
        df["FECHAHORATRX"], format="%d/%m/%Y %H:%M:%S", errors='coerce')
    df.columns = [x.lower() for x in df.columns]
    return df


def to_s3(df, bucket_name, file_name):
    """
    Move dataset to AWS S3.
    """
    folder = "transactions"
    dates = re.findall(r'\d+', file_name)
    download_day, from_day, to_day = dates

    output_file = f"s3://%s/%s/day=%s/from=%s/to=%s/data.parquet" % (
        bucket_name,
        folder,
        download_day,
        from_day,
        to_day)

    df.to_parquet(output_file, compression='gzip')


% % time
files = []
path = "datasets/20190311-small.zip"
# Read the file as a zipfile and process the members
with ZipFile(path, mode='r') as zipf:
    # for subfile in zipf.namelist():
    #     print(subfile)
    zip_name = "20190311/home/telefonica/shared/filtered_Lst_Usos_20190303_20190305.CSV"
    files = [(zip_name, load_trx(zipf.read(zip_name)))]
    # files = [(name, load_trx(zipf.read(name))) for name in zipf.namelist()]


bucket_name = "transantiago"
file_name = "20190330"

to_s3(files[0][1], bucket_name, file_name)


zip_name = "20190311/home/telefonica/shared/filtered_Lst_Usos_20190303_20190305.CSV"
re.findall(r'\d+', zip_name)
