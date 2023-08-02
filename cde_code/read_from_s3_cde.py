from pyspark.sql import SparkSession
from pprint import pprint
import pandas as pd

spark = SparkSession\
        .builder\
        .appName("Write_To_S3")\
        .enableHiveSupport()\
        .getOrCreate()

#S3 Object URI to Read
s3_read_location = "s3a://ankity-cdp-aw-delete/data/data/"

#S3 Write Location
s3_write_location = "s3a://ankity-cdp-aw-delete/data/" 





# # Read CSV file from S3
spark_df = spark.read.format("parquet").option("header","true").load(s3_read_location)

pandas_df = spark_df.toPandas()
print(pandas_df)

# def write_row_to_file(dataframe):
#     df = dataframe.select("content")
# #   file_path = f"s3://{s3_bucket}/{s3_prefix}{name}.txt"
# #   s3_client.put_object(Body=name, Bucket=s3_bucket, Key=f"{s3_prefix}{name}.txt")
#     #df.write.text(f"{s3_write_location}")
#     return df

# #limit data
# df_subset = df.limit(20)
# final_df = write_row_to_file(df_subset)

# final_df.show()




# Write transformed file to S3
#df.write.text(f"{s3_write_location}/housing_orc")