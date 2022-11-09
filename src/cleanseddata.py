from pyspark.sql import *
from pyspark.sql.functions import *
import logging


class Starting:
    spark = SparkSession.builder.master("local[1]").appName("").config('spark.jars.packages', 'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').enableHiveSupport().getOrCreate()
    df = spark.read.csv("C:\\Users\\Sunil Kumar\\Downloads\\raw_log_details.csv",header = True)

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_csv(self):
        try:
            self.df = self.spark.read.csv("C:\\Users\\Sunil Kumar\\Downloads\\raw_log_details.csv",header = True)
            self.df.show()

        except Exception as err:
             logging.error('Exception was thrown in connection %s' % err)
             print("Error is {}".format(err))
             sys.exit(1)

        else:
            self.df.printSchema()

# raw_df.show(10, truncate=False)


    def cleansed_data(self):
        self.df = self.df.drop(col("row_id")).dropDuplicates().withColumn("row_id", monotonically_increasing_id())\
                       .withColumn('datetime',to_timestamp('datetime','dd/MMM/yyyy:HH:mm:ss'))\
                       .withColumn('status_code', col('status_code').cast('int')) \
                       .withColumn('size', col('size').cast('int')) \
                       .withColumn('referrer_present', when(col('referrer') == 'Null', "N").otherwise("Y")) \
                       .withColumn("size", round(col("size") / 1000, 2)) \
                       .withColumn('method', regexp_replace('method', 'GET', 'PUT')) \
                      .withColumn('datetime',date_format(col("datetime"), "MM-dd-yyyy:HH:mm:ss")) \
                        .select('row_id', 'ip', 'datetime', 'method', 'request', 'status_code', 'size', 'referrer', 'user_agent')


        self.df.show(truncate=False)

    def write_to_s3(self):
        self.df.write.csv("   ", mode="append", header=True)

    def connect_to_snowflake(self):
        self.sfOptions = {
            "sfURL": r"",
            "sfAccount": "su57550",
            "sfUser": "sunil",
            "sfPassword": "",
            "sfDatabase": "SUNILDB",
            "sfSchema": "PUBLIC",
            "sfWarehouse": "COMPUTE_WH",
            "sfRole": "ACCOUNTADMIN"
        }

        self.df.coalesce(1).write.format("snowflake").options(**self.sfOptions).option("dbtable",
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
