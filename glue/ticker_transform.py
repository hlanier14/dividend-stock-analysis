import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
import boto3
import json

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3 = boto3.resource('s3')

dynamic_frame_read = glueContext.create_dynamic_frame.from_catalog(database = "${GlueDB}", table_name = "ticker-${pRawFolder}")
data_frame = dynamic_frame_read.toDF()

data_frame = data_frame.withColumn("updatedat", F.to_utc_timestamp(F.col("updatedat"), "UTC").cast(TimestampType()))

latest_updatedat_values = (
    data_frame
    .select("updatedat")
    .distinct()
    .orderBy("updatedat", ascending=False)
    .limit(2)
    .collect()
)

latest_df = (
    data_frame
    .filter(data_frame["updatedat"] == latest_updatedat_values[0]["updatedat"])
    .select("symbol")
    .distinct()
)

previous_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), latest_df.schema)

if len(latest_updatedat_values) == 2:
    previous_df = (
        data_frame
        .filter(data_frame["updatedat"] == latest_updatedat_values[1]["updatedat"])
        .select("symbol")
        .distinct()
    )

new_symbols = latest_df.subtract(previous_df)

new_symbols_list = [row['symbol'] for row in new_symbols.collect()]
previous_symbols_list = [row['symbol'] for row in previous_df.collect()]

data = {'New_Tickers': new_symbols_list, 'Old_Tickers': previous_symbols_list}

s3object = s3.Object('${pS3BucketName}', '${pTickerFolder}/${pTransformFolder}/data_input.json')
s3object.put(
    Body=(bytes(json.dumps(data).encode('UTF-8')))
)

job.commit()