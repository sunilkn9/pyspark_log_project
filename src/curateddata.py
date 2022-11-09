from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
import logging

class Started:
    spark = SparkSession.builder.appName(" ").config('spark.jars.packages', 'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').enableHiveSupport().getOrCreate()
    curated = spark.read.csv("C:\\Users\\Sunil Kumar\\Downloads\\cleansed_log_details.csv",header=True, inferSchema=True)

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def data_from_s3_curated(self):
        spark = SparkSession.builder.appName(" ").config('spark.ui.port', '4050').config(
            "spark.master", "local").enableHiveSupport().getOrCreate()
        df = spark.read.csv("C:\\Users\\Sunil Kumar\\Downloads\\cleansed_log_details_data.csv", header=True, inferSchema=True)
        df.show()

        """ aggregation"""
        curated = df.drop("referrer") \
                     .na.fill("Na")
        curated.show()

        def split_date(val):
         return " ".join(val.split(":")[:2])

        split_date_udf = udf(lambda x: split_date(x), StringType())
        cnt_cond = lambda cond: sum(when(cond, 1).otherwise(0))
        log_per_device =  curated.withColumn("day_hour", split_date_udf(col("datetime"))).groupBy("day_hour", "ip") \
                          .agg(cnt_cond(col('method') == "PUT").alias("no_put"), \
                               cnt_cond(col('method') == "POST").alias("no_post"), \
                               cnt_cond(col('method') == "HEAD").alias("no_head"), \
                              ).orderBy(asc("day_hour")).withColumn("row_id", monotonically_increasing_id())\
                              .select("row_id", "day_hour","ip","no_put","no_post","no_head")

        log_per_device.show()

