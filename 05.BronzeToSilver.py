#PreProcessing /Preingestion Layer 
#Read Data from raw layer in any format and ingest in parquet in preingestion payer 

#config

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

spark=SparkSession.builder.appName("BronzeToSilver").getOrCreate()

#config_file_path = 'D://config.json'
#config_file_path = '/home/hadoop/config.json'
#
#def read_config_from_json(json_file):
#    with open(json_file, 'r') as file:
#        config = json.load(file)
#    return config
#
#config_data = read_config_from_json(config_file_path)

config_data={
  "tables": ["address","city","complaint","country","plan_postpaid","plan_prepaid","staff","subscriber"],
  "host": "jdbc:postgresql://database-1.cjwy40s80e0c.ap-south-1.rds.amazonaws.com:5432/prod",
  "username": "puser",
  "pwd": "ppassword",
  "driver": "org.postgresql.Driver",
  "bronze_layer_path": "s3://gproject1/bronze/",
  "silver_layer_path": "s3://gproject1/Sliver_Data/",
  "gold_layer_path": "s3://gproject1/gold_data/",
  "platinum_layer_path": "s3://gproject1/report_Data/",
  "sub_dtl_tgt_tbl": "subscriber_details",
  "cmp_dtl_tgt_tbl": "complaint_details",
  "revenue_tbl": "revenue_report"
}

# Access parameters from the config data
table_list = config_data.get("tables", [])
bronze_layer_path = config_data.get("bronze_layer_path", "")
silver_layer_path = config_data.get("silver_layer_path", "")

print(table_list)
print(bronze_layer_path)
print(silver_layer_path)


#read csv
def read_csv(spark,path,delimiter=',',inferschema='false',header='false'):
    df=spark.read.format("CSV").option("header",header).option("inferschema", inferschema).option("delimiter", delimiter).load(path)
    return df

#write parquet
def write_data_parquet_fs(spark,df,path):
    df.write.format("parquet").save(path)
    print("Data Successfully return in FS") 

for table in table_list:
    print("Data Load Started for ",table)
    df=read_csv(spark, bronze_layer_path+table,delimiter=',',inferschema='true',header='true')
    df.show()
    write_data_parquet_fs(spark, df, silver_layer_path+table)

print("********Job Successfully Completed**********")