
from io import BytesIO
import json
import boto3
from zipfile import ZipFile
import pandas as pd
import re

print('++Loading function++')


def lambda_handler(event, context):
    event = event['Records'][0]['s3']
    print("event[object]=" + json.dumps(event['object'], indent=4, sort_keys=True))

    # Retrieve object
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=event['bucket']['name'], Key=event['object']['key'])
    dataset = response['Body'].read()

    # Decompress .zip and load trxs as DataFrame
    with BytesIO(dataset) as tf:
        tf.seek(0)
        # Read the file as a zipfile and process the members
        with ZipFile(tf, mode='r') as zipf:
            files = [(name, load_trx(zipf.read(name))) for name in zipf.namelist()]

        # Save as parquet
        for file in files:
            file_name = event['object']['key'] + file[0]
            print("Saving %s..." % file_name)
            to_s3(df=file[1], bucket_name=event['bucket']['name'], file_name=file_name)


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


if __name__ == '__main__':
    event = {
        "Records": [
            {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "1970-01-01T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "EXAMPLE"
                },
                "requestParameters": {
                    "sourceIPAddress": "127.0.0.1"
                },
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": "dtpm-transactions",
                        "ownerIdentity": {
                            "principalId": "EXAMPLE"
                        },
                        "arn": "arn:aws:s3:::example-bucket"
                    },
                    "object": {
                        "key": "test-lambda/20180818.zip",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901"
                    }
                }
            }
        ]
    }
    lambda_handler(event, None)
