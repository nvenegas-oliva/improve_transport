
from io import BytesIO
import argparse
from zipfile import ZipFile, BadZipfile
import re
from boto3 import resource
from memory_profiler import profile
import logging
import pandas as pd

# Set different levels of logging
logging.basicConfig(
    level=logging.ERROR,
    # filename='app.log',
    filemode='w',
    format='[%(name)s] [%(levelname)s] [%(asctime)s] - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def build_filename(raw_name):
    """
    Build file name using dates in raw_name.
    """
    day, from_day, to_day = re.findall(r"[\d+]{8}", raw_name)
    return "day=%s/from=%s/to=%s" % (day, from_day, to_day)


@profile
def convert_dataset(dataset, file_name, bucket):
    """
    Convert dataset (from stream) to DataFrame and save as .parquet in s3.
    """
    output_path = "parquet"
    logger.debug("Converting %s" % file_name)
    # Decompress file
    try:
        decompressed_file = []
        with ZipFile(dataset, mode='r') as zipf:
            for sub_file in zipf.namelist():
                logger.debug("Processing subfile %s" % sub_file)
                # Add (file_name, df)
                decompressed_file.append((
                    build_filename(file_name + sub_file),
                    create_df(zipf.read(sub_file))
                ))

    except BadZipfile:
        # logging.error("Exception occurred", exc_info=True)
        logging.error("Bad ZipFile")
    else:
        # Write in parquet all sub-files.
        for file_name, df in decompressed_file:
            output_dir = "s3://%s/%s/%s/data.parquet" % (bucket, output_path, file_name)
            df.to_parquet(output_dir, compression="gzip", engine="fastparquet")


@profile
def create_df(decompressed_file):
    """
    Generate a pandas DataFrame using the correct format.
    """
    dtype = {
        'CODIGOENTIDAD': 'int64',
        'NOMBREENTIDAD': 'str',
        'CODIGOSITIO': 'int64',
        'NOMBRESITIO': 'str',
        'NROTARJETA': 'str'
    }
    df = pd.read_csv(
        BytesIO(decompressed_file),
        encoding="cp1252",
        sep=";",
        usecols=range(6),
        dtype=dtype)
    df["FECHAHORATRX"] = pd.to_datetime(
        df["FECHAHORATRX"], format="%d/%m/%Y %H:%M:%S", errors='coerce')
    df.columns = [x.lower() for x in df.columns]
    return df


@profile
def main(args):
    BUCKET = "dtpm-transactions"
    if args.environment == "local":
        input_path = "datasets/test-folder/*.zip"
        output_path = "./"
    elif args.environment == "cloud":
        input_path = "s3n://dtpm-transactions/test-folder/*.zip"
        output_path = "s3n://dtpm-transactions/parquet/"

    # Get list of files to transform
    # file = "test-folder-small/20180818.zip"  # Small file
    # file = "test-folder-small/20181011.zip"  # Zip error file
    file = "20181103.zip"  # Big file

    s3 = resource("s3")
    obj = s3.Bucket(BUCKET).Object(file)

    logger.debug("Retrieving object s3://%s/%s" % (BUCKET, file))
    with BytesIO(obj.get()["Body"].read()) as stream:
        logger.debug("Object retrieved")
        # Rewind the file
        stream.seek(0)
        convert_dataset(stream, obj.key, BUCKET)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("environment", help="'cloud' or 'local'")
    args = parser.parse_args()
    logger.info("args.environment = %s" % args.environment)
    main(args)
