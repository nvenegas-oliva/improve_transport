
from pyspark import SparkConf, SparkContext
import re
from io import BytesIO
from boto3 import resource
from zipfile import ZipFile

import pandas as pd
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("transport").getOrCreate()
bucket_name = "dtpm-transactions"

s3 = resource("s3")
bucket = s3.Bucket(bucket_name)

for obj in bucket.objects.all():
    # Do not convert the respaldo or another folder.
    if bool(re.match(r"^[0-9]*.zip", obj.key)):
        print(obj.key)
        response = obj.get()
        dataset = response['Body'].read()
        # Uncompress .zip
        # Validate zip structure
        # Validate do not exist files with the same name

        # Create RDD or DataFrame

        # Build eschema

        # Save as parquet
        break

type(dataset)
with BytesIO(dataset) as tf:
    tf.seek(0)

    # Read the file as a zipfile and process the members
    with ZipFile(tf, mode='r') as zipf:
        # for subfile in zipf.namelist():
        #     print(subfile)
        data = {name: zipf.read(name) for name in zipf.namelist()}


cad = "home/telefonica/shared/filtsered_Lst_Usos_20180813_20180815.CSV"
root_path = "home/telefonica/shared/filtsered_Lst_Usos_"
bool(re.match(r"%s[0-9]{8}_[0-9]{8}.CSV" % root_path, cad))

len(data.keys())
data.keys()
type(data['home/telefonica/shared/filtered_Lst_Usos_20180812_20180814.CSV'])
key = 'home/telefonica/shared/filtered_Lst_Usos_20180812_20180814.CSV'
df_1 = pd.read_csv(BytesIO(data[key]), sep=";", encoding="cp1252", usecols=[i for i in range(6)])
df_1.info()
len(df_1)
help(spark.read.csv)
spark.read.csv(BytesIO(data[key]).read().decode('cp1252'), sep=";").printSchema()

type(BytesIO(data[key]).read().decode('cp1252'))

csv = BytesIO(data[key]).read().decode('cp1252').split('\n')
len(csv)
type(csv)
csv[:100]
len(csv.split('\n'))

spark.stop()
conf = SparkConf().setMaster("local[4]").setAppName("transport")
sc = SparkContext(conf=conf)

df_2 = sc.parallelize(csv)
sc
df_2 = df_2.map(lambda a: a.split(";"))
type(df_2)
print(df_2.toDebugString().decode())
df_2.take(5)
