from pyspark.sql import *
from pyspark.sql.functions import *
import logging


class Starting:
    spark = SparkSession.builder.master("local[1]").appName("").config('spark.jars.packages', 'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').enableHiveSupport().getOrCreate()
    df = spark.read.csv("C:\\Users\\Sunil Kumar\\Downloads\\raw_details_log.csv",header = True)

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_csv(self):
        try:
            self.df = self.spark.read.csv("C:\\Users\\Sunil Kumar\\Downloads\\raw_log_data.csv",header = True)
            self.df.show()

        except Exception as err:
             logging.error('Exception was thrown in connection %s' % err)
             print("Error is {}".format(err))
             sys.exit(1)

        else:
            self.df.printSchema()

# raw_df.show(10, truncate=False)


    def cleansed_data(self):
        self.df = self.df.withColumn('datetime',to_timestamp('datetime','dd/MMM/yyyy:HH:mm:ss'))\
                      .withColumn("request", regexp_replace("request", "[@\+\#\$\%\^\!\-\,]+", "")) \
                      .withColumn('status_code', col('status_code').cast('int')) \
                      .withColumn('size', col('size').cast('int')) \
                      .withColumn("referrer", regexp_replace("referrer", "-", "Null")) \
                       .withColumn('referrer_present', when(col('referrer') == 'Null', "N").otherwise("Y")) \
                      .withColumn("size", round(col("size") / 1000, 2)) \
                       .withColumn('method', regexp_replace('method', 'GET', 'PUT'))\
                       .withColumn('datetime',date_format(col("datetime"), "MM-dd-yyyy:HH:mm:ss"))

        self.df.show(truncate=False)

    def write_to_s3(self):
        self.df.write.csv("   ", mode="append", header=True)

    def connect_to_snowflake(self):
        self.sfOptions = {
            "sfURL": r"https://tm57257.europe-west4.gcp.snowflakecomputing.com/",
            "sfAccount": "tm57257",
            "sfUser": "TESTDATA",
            "sfPassword": "Welcome@1",
            "sfDatabase": "SUNIL_DB",
            "sfSchema": "PUBLIC",
            "sfWarehouse": "COMPUTE_WH",
            "sfRole": "ACCOUNTADMIN"
        }

        self.df.write.format("snowflake").options(**self.sfOptions).option("dbtable",
                                                                           "{}".format(r"cleansed_log_details")).mode(
            "overwrite").options(header=True).save()
    def write_to_hive(self):
        pass
        # **************************
        #self.df.write.csv("  ", mode="append", header=True)
        #self.df.write.saveAsTable('cleanse_log_details')

if __name__ == "__main__":
    # Start
    starting = Starting()

    try:
        starting.read_from_csv()
    except Exception as e:
        logging.error('Error at %s', 'extract_column_regex', exc_info=e)
        sys.exit(1)

    try:
        starting.cleansed_data()
    except Exception as e:
        logging.error('Error at %s', 'extract_column_regex', exc_info=e)
        sys.exit(1)

    try:
        starting.write_to_hive()
    except Exception as e:
        logging.error('Error at %s', 'extract_column_regex', exc_info=e)
        sys.exit(1)

    try:
        starting.connect_to_snowflake()
    except Exception as e:
        logging.error('Error at %s', 'extract_column_regex', exc_info=e)
        sys.exit(1)