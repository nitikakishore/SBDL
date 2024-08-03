from pyspark.sql import SparkSession
from lib.ConfigLoader import get_spark_conf

## create spark session for LOCAL env and for QA/PROD environment
def get_spark_session(env):
    if env == "LOCAL":
        ## set master & log4J properties manually for LOCAL, for other environments,
        # the master copy & log4J will be taken from the spark.submit cli
        return SparkSession.builder \
            .config(conf=get_spark_conf(env)) \
            .config('spark.sql.autoBroadcastJoinThreshold',-1) \
            .config('spark.sql.adaptive.enabled','false') \
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties') \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .master("local[2]") \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .config(conf=get_spark_conf(env)) \
            .enableHiveSupport() \
            .getOrCreate()