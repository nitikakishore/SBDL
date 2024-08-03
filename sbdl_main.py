import sys
import uuid

from pyspark.sql.functions import struct, col, to_json

from lib import ConfigLoader, Utils, DataLoader, Transformations
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]
    job_run_id = "SBDL-" + str(uuid.uuid4())

    print("Initializing SBDL Job in " + job_run_env + " Job ID: " + job_run_id)
    conf = ConfigLoader.get_config(job_run_env)
    enable_hive = True if conf["enable.hive"] == "true" else False
    try:
        hive_db = conf["hive.database"]
    except Exception as ex:
        print(ex)
## creates spark session
    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)

## creates the logger, since logger requires the spark session so cannot be created
    # before the creation of spark session
    logger = Log4j(spark)
## read the account dataframe and transform it
    logger.info("Reading SBDL Account DF")
    accounts_df = DataLoader.read_accounts(spark, job_run_env, enable_hive, hive_db)
    print("Account DF")
    accounts_df.show(10,truncate=False)
    contract_df = Transformations.get_contract(accounts_df)
    print("Contract DF")
    contract_df.show(10,truncate=False)
## read the party dataframe and transform it
    logger.info("Reading SBDL Party DF")
    parties_df = DataLoader.read_parties(spark, job_run_env, enable_hive, hive_db)
    print("Party DF")
    parties_df.show(10,truncate=False)
    relations_df = Transformations.get_relations(parties_df)
    print("Relations DF")
    relations_df.show(10,truncate=False)
## read the address dataferame and transform it
    logger.info("Reading SBDL Address DF")
    address_df = DataLoader.read_address(spark, job_run_env, enable_hive, hive_db)
    print("Address DF")
    address_df.show(10,truncate=False)
    relation_address_df = Transformations.get_address(address_df)
    print("relation Address DF")
    relation_address_df.show(10,truncate=False)
## joining party and address
    logger.info("Join Party Relations and Address")
    party_address_df = Transformations.join_party_address(relations_df, relation_address_df)
    print("Party Address DF")
    party_address_df.show(10,truncate=False)
## now joining accout and party, data_df creates the payload
    logger.info("Join Account and Parties")
    data_df = Transformations.join_contract_party(contract_df, party_address_df)
    print("Data DF")
    data_df.show(10,truncate=False)
## applying header event header to each record in the data_df

    logger.info("Apply Header and create Event")
## final_df is the fully transformed dataframe with all the requirements and
    # this will be finally sent to the kafka topic
    final_df = Transformations.apply_header(spark, data_df)
    print("Final DF")
    final_df.show(truncate=False)
    logger.info("Preparing to send data to Kafka")
## kafka takes only key value pair, it should be a 2 column dataframe,
    # so we are selecting the columns to be sent and specifying the key and the value column
    ##  we are taking everything using * and package it into a struct.
    # The value should be a json ,so we are converting it to a json string using the to_json function
    kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key"),
                                  to_json(struct("*")).alias("value"))
    print("kafka_kv_df")
    kafka_kv_df.show(truncate=False)
    input("Press Any Key")

    kafka_kv_df.write.format("noop").mode("overwrite").save("test_data\noop")

    # Keep it in vault or other secure place, authorize application to extract it from there

    api_key = conf["kafka.api_key"]
    api_secret = conf["kafka.api_secret"]

    kafka_kv_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
        .option("topic", conf["kafka.topic"]) \
        .option("kafka.security.protocol", conf["kafka.security.protocol"]) \
        .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(api_key, api_secret)) \
        .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"]) \
        .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"]) \
        .save()

    logger.info("Finished sending data to Kafka")