
import os
from io import BytesIO
from zipfile import ZipFile, BadZipfile
from datetime import datetime
import re
import argparse

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


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
    conf = SparkConf().setMaster("local[4]").setAppName("transport")
    sc = SparkContext(conf=conf)

    sqlContext = SQLContext(sc)

    if args.environment == "local":
        input_path = "datasets/test-folder.small/*.zip"
        output_path = "./"
    elif args.environment == "cloud":
        input_path = "s3n://dtpm-transactions/test-folder-small/*.zip"
        output_path = "s3n://dtpm-transactions/parquet/"

    rdd = sc.binaryFiles(input_path).flatMap(lambda a: extract_files(a[0], a[1]))
    print("rdd.count()=%d" % rdd.count())
    # Decode bytes and convert it in a list of strings
    rdd = rdd.mapValues(lambda file: BytesIO(file).read().decode('cp1252').split('\n'))

    # Drop header and last (and empty) row
    rdd = rdd.mapValues(lambda table: table[1:-1])

    # Change type of columns
    rdd = rdd.flatMap(lambda a: prepare_csv(a[0], a[1]))

    header = ['FILE_NAME',
              'FECHAHORATRX',
              'CODIGOENTIDAD',
              'NOMBREENTIDAD',
              'CODIGOSITIO',
              'NOMBRESITIO',
              'NROTARJETA']
    header = list(map(lambda a: a.lower(), header))

    df = rdd.toDF(header)

    days = [file.file_name for file in df.select('file_name').distinct().collect()]

    for directory in days:
        if not os.path.exists(directory):
            os.makedirs(directory)
        df_day = df.select(df.columns[1:]).where(df.file_name == directory)
        df_day.write.parquet(output_path + directory + "/data.parquet", compression="gzip")


if __name__ == "__main__":

    """conf = SparkConf().setMaster("local[4]").setAppName("transport")
    sc = SparkContext(conf=conf)

    # sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAI6YJJLTG7TUBMVXA")
    # sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")

    sqlContext = SQLContext(sc)

    input_path = "s3n://dtpm-transactions/test-folder-small/*.zip"
    # output_path = "s3://dtpm-transactions/parquet/"
    rdd = sc.binaryFiles(input_path)
    rdd.collect()
    print("rdd.count()=%d" % rdd.count())"""

    parser = argparse.ArgumentParser()
    parser.add_argument("environment", help="'cloud' or 'local'")
    args = parser.parse_args()
    print("args.environment=" + args.environment)
    main(args)

    """input_path = "s3n://dtpm-transactions/test-folder-small/*.zip"
    rdd = sc.binaryFiles(input_path).flatMap(lambda a: extract_files(a[0], a[1]))
    rdd.collect()
    print("rdd.count()=%d" % rdd.count())"""
