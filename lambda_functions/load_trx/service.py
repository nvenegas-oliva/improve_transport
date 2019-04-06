# -*- coding: utf-8 -*-

import pandas as pd
from io import StringIO
import boto3
import re


def handler(event, context):
    # Your code goes here!
    to_s3(event['csv'], event['bucket_name'], event['file_name'])
    return event


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
    # df = pd.read_csv(BytesIO(csv), sep=";", encoding="cp1252",
    #                 usecols=[i for i in range(6)], dtype=dtype)
    df = pd.read_csv(StringIO(csv), sep=";", encoding="cp1252",
                     usecols=[i for i in range(6)], dtype=dtype)
    df["FECHAHORATRX"] = pd.to_datetime(
        df["FECHAHORATRX"], format="%d/%m/%Y %H:%M:%S", errors='coerce')
    df.columns = [x.lower() for x in df.columns]
    return df


def to_s3(csv, bucket_name, file_name):
    """
    Move .csv to AWS S3 in .parquet format.
    """
    # Convert .csv to pandas DataFrame.
    df = load_trx(csv)

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
