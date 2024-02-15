import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import job
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date,split


# @params: [pyspark_etl]

args = getResolvedOptions( sys.argv, ['pyspark_etl'])
sc = SparkContext()
glue_context = GlueContext(sc)

# Initialize my job

job =Job(GlueContext(sc))
job.init(args['pyspark_etl'],args)
dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database='dbname',table_name='tbname',transformation_ctx ="datasrc")

df = dynamic_frame.toDF()
df.show()
print("Dataframe converted")

# convert column names with special characters
df = df.withColumn("date_Added", to_date(split(df["date"], " ").getItem(0).cast("string"), 'MM/dd/yyyy')) \
    .withColumn("month", split(col("date_Added"), "-").getItem(1)) \
    .withColumn("day", split(col("date_Added"), "-").getItem(2)) \
    .orderBy('date_Added')
print("Dataframe sorted")

partitioned_dataframe = df.repartition("day")
print("Dataframe repartitioned")

# Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(partitioned_dataframe, glue_context, "dynamic_frame_write",
                                          transformation_ctx = "applymapping1")
print("Dataframe converted to dynamic frame")

glue_context.write_dynamic_frame.from_options(frame=dynamic_frame_write, connection_type="s3", connection_options=dict(
    path="s3://bucket-name/", partitionKeys=["year", "month"]), format="parquet",
                                              transformation_ctx="datasink2")
print("Dynamic frame saved in s3")

# Commit file read to Job Bookmark
job.commit()
print("Job completed!!!")