
from io import BytesIO
from zipfile import ZipFile, BadZipfile
from datetime import datetime
import re
import argparse


def build_filename(raw_name):
    day, from_day, to_day = re.findall(r"[\d+]{8}", raw_name)
    return "day=%s/from=%s/to=%s" % (day, from_day, to_day)


def extract_files(compressed_name, stream):
    """
    Extract .csv files from a .zip file and load in different DataFrames.
    """
    with BytesIO(stream) as tf:
        tf.seek(0)
        # Read the file as a zipfile and process the members
        try:
            with ZipFile(tf, mode='r') as zipf:
                return [(build_filename(compressed_name + file_name), zipf.read(file_name)) for file_name in zipf.namelist()]
        except BadZipfile:
            return []
# datetime.strptime(row[0], "%d/%m/%Y %H:%M:%S")


def prepare_csv(file_name, table):
    try:
        return list(map(lambda row: [file_name, datetime.strptime(row[0], "%d/%m/%Y %H:%M:%S"), int(row[1]),
                                     row[2], int(row[3]), row[4],
                                     row[5]], table))
    except ValueError:
        return list(map(lambda row: [file_name, row[0], int(row[1]),
                                     row[2], int(row[3]), row[4],
                                     row[5]], table))


def main(args):

    if args.environment == "local":
        input_path = "datasets/test-folder/*.zip"
        output_path = "./"
    elif args.environment == "cloud":
        input_path = "s3n://dtpm-transactions/test-folder/*.zip"
        output_path = "s3n://dtpm-transactions/parquet/"


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("environment", help="'cloud' or 'local'")
    args = parser.parse_args()
    print("args.environment=" + args.environment)
    main(args)
