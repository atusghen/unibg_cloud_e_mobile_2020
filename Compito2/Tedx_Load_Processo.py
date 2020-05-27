#!/usr/bin/env python3
# -*- coding: utf-8 -*-


###### TEDx-Load-Processo
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job




##### FROM FILES
tedx_dataset_path = "s3://minigcc-tedx-data/tedx_dataset.csv"
tedx_dataset_path2= "s3://minigcc-tedx-data/tedxxx.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read.option("header","true").option("quote", "\"").option("escape", "\"").csv(tedx_dataset_path)
tedx2_dataset =spark.read.option("header","true").option("quote", "\"").option("escape", "\"").csv(tedx_dataset_path2)

tedx_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")
tedx2_dataset_agg=tedx2_dataset.withColumnRenamed("idx", "pd")
tedx_dataset = tedx_dataset.join(tedx2_dataset_agg, tedx_dataset.idx == tedx2_dataset_agg.pd, "left").drop("pd")
tedx3_dataset=tedx_dataset


## READ TAGS DATASET
tags_dataset_path = "s3://minigcc-tedx-data/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)



# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left").drop("idx_ref").select(col("idx").alias("id"), col("*")).drop("idx")

tedx_dataset_agg.printSchema()

## READ WATCH_NEXT DATASET
watch_next_dataset_path = "s3://minigcc-tedx-data/watch_next2.csv"
watch_next_dataset = spark.read.option("header","true").csv(watch_next_dataset_path).dropDuplicates()



# CREATE THE AGGREGATE MODEL, ADD WATCH_NEXT TO TEDX_DATASET
watch_next_dataset_agg = watch_next_dataset.groupBy(col("idx").alias("idx_ref1")).agg(collect_list("url").alias("watch_next_url"))
watch_next_dataset_agg.printSchema()
tedx_dataset_agg2 = tedx_dataset_agg.join(watch_next_dataset_agg, tedx_dataset_agg.id == watch_next_dataset_agg.idx_ref1, "left").drop("idx_ref1").select(col("id").alias("_id"), col("*")).drop("id")

tedx_dataset_agg2.printSchema()

# CREATE THE AGGREGATE MODEL, ADD WATCH_NEXT2 TO TEDX_DATASET
tedx3_dataset_agg=tedx3_dataset.groupBy(col("NomeEvento").alias("Event")).agg(collect_list("url").alias("watch_next_url2"))
tedx3_dataset_agg.printSchema()
tedx_dataset_agg3 = tedx_dataset_agg2.join(tedx3_dataset_agg, tedx_dataset_agg2.NomeEvento == tedx3_dataset_agg.Event, "left").drop("Event").select(col("NomeEvento").alias("Evento"), col("*")).drop("NomeEvento")

mongo_uri = "mongodb://cluster0-shard-00-00-kkfnv.mongodb.net:27017,cluster0-shard-00-01-kkfnv.mongodb.net:27017,cluster0-shard-00-02-kkfnv.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx",
    "collection": "tedz_data",
    "username": "admin",
    "password": "9OuRAu0a42M8sqVB",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg3, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
