
import os
from io import BytesIO
import argparse
from zipfile import ZipFile, BadZipfile
import re
import pandas as pd


def build_filename(raw_name):
    day, from_day, to_day = re.findall(r"[\d+]{8}", raw_name)
    return "day=%s/from=%s/to=%s" % (day, from_day, to_day)


def main(args):
    BUCKET = "dtpm-transactions"
    if args.environment == "local":
        input_path = "datasets/test-folder/*.zip"
        output_path = "./"
    elif args.environment == "cloud":
        input_path = "s3n://dtpm-transactions/test-folder/*.zip"
        output_path = "s3n://dtpm-transactions/parquet/"


def convert_dataset(dataset, bucket):
    """
    Convert dataset (stream) to parquet.
    """
    output_path = "parquet"

    # Decompress file
    try:
        with ZipFile(dataset, mode='r') as zipf:
            decompressed_file = [(
                # (file_name, DataFrame)
                build_filename(path + file_name), create_df(zipf.read(file_name))
            ) for file_name in zipf.namelist()]
    except BadZipfile:
        print("BadZipfile")

    # Write in parquet all sub-files.
    for file_name, df in decompressed_file:
        df.to_parquet("s3://%s/%s/%s/data.parquet" %
                      (bucket, output_path, file_name),
                      compression="gzip",
                      engine="fastparquet")


def create_df(decompressed_file):
    """
    Generate a pandas DataFrame from a .csv compressed file.
    """
    dtype = {
        'CODIGOENTIDAD': 'int64',
        'NOMBREENTIDAD': 'str',
        'CODIGOSITIO': 'int64',
        'NOMBRESITIO': 'str',
        'NROTARJETA': 'str'
    }
    try:
        df = pd.read_csv(
            BytesIO(decompressed_file),
            encoding="cp1252",
            sep=";",
            usecols=range(6),
            dtype=dtype)
    except:
        return None
    df["FECHAHORATRX"] = pd.to_datetime(
        df["FECHAHORATRX"], format="%d/%m/%Y %H:%M:%S", errors='coerce')
    df.columns = [x.lower() for x in df.columns]
    return df


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("environment", help="'cloud' or 'local'")
    args = parser.parse_args()
    print("args.environment=" + args.environment)
    main(args)
    path = "datasets/20181103.zip"

    list(map(lambda a: a[0], decompressed_file))
    decompressed_file[0][1].info()

    if not os.path.exists(decompressed_file[0][0]):
        os.makedirs(decompressed_file[0][0])

    decompressed_file[0][1].to_parquet("%s/data.parquet" %
                                       decompressed_file[0][0], compression="gzip", engine="fastparquet")

    help(decompressed_file[0][1].to_parquet)
