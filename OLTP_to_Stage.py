# Databricks notebook source
import urllib
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime
# MOUNT_NAME = "/mnt/election-project-stage"
MOUNT_NAME = "/mnt/temp"

try:
    dbutils.fs.ls(MOUNT_NAME)
except:
    file_type = "csv"
    first_row_is_header = "true"
    delimiter = ","
    aws_keys_df = spark.read.format(file_type)\
    .option("header", first_row_is_header)\
    .option("sep", delimiter)\`
    .load("dbfs:/FileStore/yahoo_rama_accessKeys.csv")
    ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
    SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
    ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")
    AWS_S3_BUCKET = "temp161"
    SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
    dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

def get_data(value):
    df = (spark.read
      .format("mysql")
      .option("dbtable", value)
      .option("host", "election.cij2kputxcro.ap-south-1.rds.amazonaws.com")
      .option("port", 3306)
      .option("database", "election_oltp")
      .option("user", "admin")
      .option("password", "mars3623")
      .load()
    )
    return df

# COMMAND ----------

def to_stage(df , value):
    df = df.withColumn(f"{value}_stage_timestamp" , F.current_timestamp())
    df.write.parquet(f"{MOUNT_NAME}/" + value.lower() + "_stage/" + 
                  "as_on_dt=" + str(datetime.datetime.now()).split()[0] + "/" + 
                  value.lower() + "_stage.parquet")
    print(f"{value} Done")
    return df

# COMMAND ----------

def job(list_ = ['Candidates','Candidates_has_Constiuncy','Candidates_has_Crime_Record','Constituency','Counting','Crime_Record','Employee','Party',
'Polling_Both_has_Employee','Polling_Station','Risk','Voters','candidate_education_level']):
    for value in list_:
        to_stage(get_data("_".join([value if value == "has" else value.capitalize() for value in value.split("_")])) , value.lower())
    return "All Done , Data in S3 Stage as Parquet"

# COMMAND ----------

job()
