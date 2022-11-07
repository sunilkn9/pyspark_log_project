from pyspark.sql import *
from pyspark.sql.functions import *
import logging
import pyspark.sql.functions as F

class Start:
    spark = SparkSession.builder.master("local[1]").appName("").config('spark.jars.packages', 'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').enableHiveSupport().getOrCreate()
    df = spark.read.option("delimiter", " ").csv("C:\\Users\\Sunil Kumar\\Downloads\\299999 (1).text")

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_s3(self):
        try:
            self.df =self.spark.read.option("delimiter", " ").csv("C:\\Users\\Sunil Kumar\\Downloads\\299999 (1).text")
            self.df.show()

        except Exception as err:
             logging.error('Exception was thrown in connection %s' % err)
             print("Error is {}".format(err))
             sys.exit(1)

        else:
            self.df.printSchema()

    def extract_columns(self):
        self.df = (self.df.select(
            F.monotonically_increasing_id().alias('row_id'),
            F.col("_c0").alias("ip"),
            F.split(F.col("_c3"), " ").getItem(0).alias("datetime"),
            F.split(F.col("_c5"), " ").getItem(0).alias("method"),
            F.split(F.col("_c5"), " ").getItem(1).alias("request"),
            F.col("_c6").alias("status_code"),
            F.col("_c7").alias("size"),
            F.col("_c8").alias("referrer"),
            F.col("_c9").alias("user_agent")
                 ))

        self.df.show(truncate = False)
    def remove_character(self):
        # Remove any special characters in the request column(% ,- ? =)
        self.df = self.df.withColumn('datetime', regexp_replace('datetime', '\[|\]|', ''))

        self.df.show(truncate = False)
    def connect_to_snowflake(self):
         self.sfOptions = {
            "sfURL": r"https://tm57257.europe-west4.gcp.snowflakecomputing.com/",
            "sfAccount": "",
            "sfUser": "",
            "sfPassword":"",
            "sfDatabase":"SUNIL_DB",
            "sfSchema":"PUBLIC",
            "sfWarehouse":"COMPUTE_WH",
            "sfRole":"ACCOUNTADMIN"
        }

         self.df.write.format("snowflake").options(**self.sfOptions).option("dbtable", "{}".format(r"raw_log_details")).mode(
        "overwrite").options(header=True).save()

    def write_to_hive(self):
        pass
        # **************************
        #self.df.write.csv("   ", mode="append", header=True)
        #self.df.write.saveAsTable('raw_log_details')


if __name__ == "__main__":
    # Start
    start = Start()
    try:
        start.read_from_s3()
    except Exception as e:
        logging.error('Error at %s', 'Reading from S3 Sink', exc_info=e)
        sys.exit(1)

    try:
        start.extract_columns()
    except Exception as e:
        logging.error('Error at %s', 'extract_column_regex', exc_info=e)
        sys.exit(1)

    try:
        start.remove_character()
    except Exception as e:
        logging.error('Error at %s', 'extract_column_regex', exc_info=e)
        sys.exit(1)

    try:
        start.connect_to_snowflake()
    except Exception as e:
        logging.error('Error at %s', 'extract_column_regex', exc_info=e)
        sys.exit(1)

    # try:
    #    start.write_to_csv()
    #   logging.info("Writing to Raw Layer S3 Successfull!")
    #except Exception as e:
    #   logging.error('Error at %s', 'write_to_s3', exc_info=e)
    #    sys.exit(1)

    try:
      start.write_to_hive()
    except Exception as e:
      logging.error('Error at %s', 'write to hive', exc_info=e)


