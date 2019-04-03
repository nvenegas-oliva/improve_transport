import json
import boto3

print('++Loading function++')


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    print("event[object]=" + json.dumps(event['object'], indent=4, sort_keys=True))
    event = event['Records'][0]['s3']

    s3 = boto3.client('s3')
    zip_file = s3.get_object(Bucket=event['bucket']['name'], Key=event['object']['key'])


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
                        "key": "test-lambda/download_trx.log",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901"
                    }
                }
            }
        ]
    }
    lambda_handler(event, None)
