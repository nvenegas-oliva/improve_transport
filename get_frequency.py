from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("transport").getOrCreate()

path = "s3://dtpm-transactions/parquet/"

df = spark.read.parquet(path)
result = df.select("nombresitio", F.date_format('fechahoratrx', 'yyyy-MM-dd').
                   alias('day')).groupby('nombresitio', 'day').count()

output_path = "s3://dtpm-transactions/result.csv"
# result.coalesce(1).write.csv(path)
result. \
    repartition(1). \
    write. \
    mode("overwrite"). \
    format("com.databricks.spark.csv"). \
    option("header", "true").save("result.csv")
