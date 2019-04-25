
from io import BytesIO
import argparse
from zipfile import ZipFile, BadZipfile
from datetime import datetime
import re
import pandas as pd


def build_filename(raw_name):
    day, from_day, to_day = re.findall(r"[\d+]{8}", raw_name)
    return "day=%s/from=%s/to=%s" % (day, from_day, to_day)


def main(args):

    if args.environment == "local":
        input_path = "datasets/test-folder/*.zip"
        output_path = "./"
    elif args.environment == "cloud":
        input_path = "s3n://dtpm-transactions/test-folder/*.zip"
        output_path = "s3n://dtpm-transactions/parquet/"


def create_df(decompressed_file):
    """Generate a pandas DataFrame from a .csv compressed file"""
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
    return df


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("environment", help="'cloud' or 'local'")
    args = parser.parse_args()
    print("args.environment=" + args.environment)
    main(args)
    path = "datasets/20181103.zip"
    try:
        with ZipFile(path, mode='r') as zipf:
            decompressed_file = [(file_name, create_df(zipf.read(file_name)))
                                 for file_name in zipf.namelist()]
    except BadZipfile:
        print("BadZipfile")
    list(map(lambda a: a[0], decompressed_file))
    decompressed_file[0][1].info()
