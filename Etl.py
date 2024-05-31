import sys
import os
import logging
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_json, struct
from pyspark.sql.types import *
from pyspark.sql.window import Window
from time import time, sleep
import yaml
from functools import wraps

# Load configuration
def load_config(config_path='config.yaml'):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

# Set up logging
config = load_config()
logging.basicConfig(level=getattr(logging, config['logging']['level']),
                    format=config['logging']['format'])
logger = logging.getLogger(__name__)

# Retry decorator
def retry(tries, delay=3, backoff=2):
    def decorator_retry(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < tries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error in {func.__name__}: {e}")
                    attempts += 1
                    if attempts < tries:
                        sleep(delay * (backoff ** (attempts - 1)))
                        logger.info(f"Retrying {func.__name__} ({attempts}/{tries})...")
            raise Exception(f"Failed {func.__name__} after {tries} attempts")
        return wrapper
    return decorator_retry

# Get environment variables
def get_environment_variables(config):
    parquet_files_str = os.getenv(config['paths']['parquet_files'])
    output_file = os.getenv(config['paths']['output_file'])
    if not parquet_files_str or not output_file:
        logger.error("Environment variables not available")
        return None, None
    return parquet_files_str.split(), output_file

# Initialize Spark session
def initialize_spark(config):
    spark_builder = SparkSession.builder.appName(config['appName'])
    spark_builder = spark_builder.config("spark.sql.parquet.binaryAsString", config['spark']['parquet_binary_as_string'])
    spark_builder = spark_builder.config("spark.sql.execution.arrow.enabled", config['spark']['execution_arrow_enabled'])
    spark_builder = spark_builder.config("spark.port.maxRetries", config['spark']['port_max_retries'])
    if config['spark']['enable_hive_support']:
        spark_builder = spark_builder.enableHiveSupport()
    return spark_builder.getOrCreate()

# Load SQL queries from file
def load_queries(file_path):
    queries = {}
    with open(file_path, 'r') as file:
        content = file.read()
        query_blocks = content.split('--')
        for block in query_blocks:
            if block.strip():
                header, query = block.strip().split('\n', 1)
                queries[header.strip()] = query.strip()
    return queries

# Function with retry logic for loading data
@retry(tries=3)
def load_gim_import_df(spark, queries):
    run_date = datetime.now() - timedelta(days=1)
    start_ts = run_date.strftime('%Y-%m-%d 00:00:00')
    end_ts = run_date.strftime('%Y-%m-%d 23:59:59')
    query = queries['gim_import_query'].format(start_ts=start_ts, end_ts=end_ts)
    df = spark.sql(query).dropDuplicates()
    if df.count() == 0:
        logger.warning("No data found for the given date range.")
    logger.info("Loaded gim_import_df")
    return df

@retry(tries=3)
def load_mapping_data(spark, config):
    resource_group_map_df = spark.read.parquet(config['paths']['resource_group_map_path'])
    interaction_purpose_map_df = spark.read.parquet(config['paths']['interaction_purpose_map_path'])
    logger.info("Loaded mapping data")
    return resource_group_map_df, interaction_purpose_map_df

# Function without retry logic for transformations
def transform_gim_staging_df(spark, queries):
    df = spark.sql(queries['transform_gim_staging_query'])
    logger.info("Transformed gim_staging_df")
    return df

def transform_gim_output_df(spark, queries):
    df = spark.sql(queries['transform_gim_output_query'])
    logger.info("Transformed gim_output_df")
    return df

def add_additional_features(gim_output_df):
    gim_output_df = gim_output_df.withColumn("additional_features", to_json(struct("offered", "engage_time", "hold_time", "wrap_time")))
    gim_output_df = gim_output_df.drop("offered", "accepted", "engage_time", "hold_time", "wrap_time")
    gim_output_df.createOrReplaceTempView("gim_output_df")
    logger.info("Added additional features")
    return gim_output_df

# Function with retry logic for inserting data
@retry(tries=3)
def insert_into_hive(spark, gim_output_df, table_name):
    gim_output_df.createOrReplaceTempView("temp_gim_output_df")
    insert_query = f"""
    INSERT INTO {table_name}
    SELECT * FROM temp_gim_output_df
    """
    spark.sql(insert_query)
    logger.info(f"Data inserted into Hive table {table_name}")

def main():
    start_time = time()
    config = load_config()
    parquet_files, output_file = get_environment_variables(config)
    if not parquet_files or not output_file:
        return
    
    queries = load_queries(config['queries_file'])
    
    spark = initialize_spark(config)
    logger.info("Spark session initialized")
    
    try:
        # Load data
        gim_import_df = load_gim_import_df(spark, queries)
        gim_import_df.createOrReplaceTempView("gim_staging_df")

        # Transform data
        gim_staging_df = transform_gim_staging_df(spark, queries)
        resource_group_map_df, interaction_purpose_map_df = load_mapping_data(spark, config)
        gim_staging_df.createOrReplaceTempView("gim_staging_df")
        resource_group_map_df.createOrReplaceTempView("resource_group_map_df")
        interaction_purpose_map_df.createOrReplaceTempView("interaction_purpose_map_df")

        # Final transformation and insertion
        gim_output_df = transform_gim_output_df(spark, queries)
        gim_output_df = add_additional_features(gim_output_df)
        insert_into_hive(spark, gim_output_df, config['hive']['table_name'])
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    
    finally:
        spark.stop()
        logger.info(f"Spark session stopped. Total runtime: {time() - start_time} seconds")

if __name__ == "__main__":
    main()
