import configparser
from pyspark import SparkConf

## to get the configurations from the sbdl.conf file in the lib folder about the environments i.e. LOCAL/QL/PROD in the main method
def get_config(env):
    config = configparser.ConfigParser()
    config.read("conf/sbdl.conf")
    conf = {}
    ## Read the values from the file according to the environment passed as parameter and store it in the dictionary
    for (key,val) in config.items(env):
        conf[key] = val
    return conf


## to create the Spark Session we are getting the configuration from the spark.conf file in the lib folder,
## this will be called in the main method and create the spark session
def get_spark_conf(env):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")
    for (key,val) in config.items():
        spark_conf.set(key,val)
    return spark_conf

## this is to load the data according to the environment and allows to filter data according to requirement
def get_data_filter(env, data_filter):
    conf = get_config(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]
