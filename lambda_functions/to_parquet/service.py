# -*- coding: utf-8 -*-
from io import BytesIO
from io import StringIO
import json
import boto3
from zipfile import ZipFile
import pandas as pd
import re


def handler(event, context):
    event = event['Records'][0]['s3']
    print(json.dumps({"event": event}, indent=4, sort_keys=True))
    to_parquet(bucket_name=event['bucket']['name'], object_key=event['object']['key'])
    return


def to_parquet(bucket_name, object_key):
    # Retrieve object
    s3 = boto3.client('s3')
    lambda_client = boto3.client('lambda', region_name="us-east-1")

    # Take dataset from S3
    print("Retrieving the object...")
    s3_object = s3.get_object(Bucket=bucket_name, Key=object_key)
    dataset = s3_object['Body'].read()
    print("Done!")
    # Decompress .zip and load trxs as DataFrame
    with BytesIO(dataset) as tf:
        tf.seek(0)
        # Read the file as a zipfile and process the members
        with ZipFile(tf, mode='r') as zipf:
            for file_name in zipf.namelist():

                # Process .csv file
                print("Load %s file..." % file_name)
                csv = zipf.read(file_name)
                # file_name = object_key + "/" + file_name
                # print("Saving %s file..." % file_name)
                # to_s3(csv=csv, bucket_name=bucket_name, file_name=file_name)
                del csv


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
