from pyspark import SparkConf, SparkContext
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
import re


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
        except:
            return []


conf = SparkConf()
sc = SparkContext(conf=conf)
"""
input_path = "s3n://dtpm-transactions/test-folder-small/*.zip"
rdd = sc.binaryFiles(input_path).flatMap(lambda a: extract_files(a[0], a[1]))
rdd.collect()
print("rdd.count()=%d" % rdd.count())
"""
print(sc._conf.getAll())
