"""
    spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""
import __main__

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from os import environ, listdir, path

import json

from dependencies import logging


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=None,
                files=None, spark_config=None, enable_hive=False):
    """
    Initialize a Spark session, configure the Spark application, and load configuration files if applicable.

    This function starts a Spark session on the worker node, registering the Spark application with the cluster. When called via spark-submit,
    only the 'app_name' argument is considered. The remaining arguments are intended for interactive testing from a Python console.

    Additionally, the function looks for a file whose name ends with 'config.json' to be sent with the Spark job. If found, the file is opened,
    and its contents are interpreted as valid JSON for ETL job configuration. This configuration is returned as the last element of the tuple
    resulting from this function. If the file is not found, the return tuple contains references only to the Spark session and Spark logger,
    with None for the configuration.

    :param app_name: Name of the Spark application.
    :param master: Cluster connection details (default: local[*]).
    :param jar_packages: List of Spark JAR package names (optional).
    :param files: List of files to be sent to the Spark cluster (master and workers) (optional).
    :param spark_config: Dictionary of key-value pairs for configuration (optional).
    :param enable_hive: Allow the use of Hive.
    :return: A tuple containing references to the Spark session, logger, and the configuration dictionary (if available).
    """

    # Initialize SparkSession builder
    spark_builder = (
        SparkSession
        .builder
        .appName(app_name)  
        .master(master)
    )

    # Configure Spark JAR packages
    spark_jars_packages = ','.join(list(jar_packages))
    spark_builder.config('spark.jars.packages', spark_jars_packages)

    # Configure additional files for Spark job
    spark_files = ','.join(list(files))
    spark_builder.config('spark.files', spark_files)

    # Set custom configurations from the provided dictionary
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # Configure Hive support if enabled
    if enable_hive:
        spark_builder.enableHiveSupport()

    spark_sess = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_files)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    try:

        if config_files:
            path_to_config_file = path.join(spark_files_dir, config_files[0])
            with open(path_to_config_file, 'r') as config_file:
                config_dict = json.load(config_file)
            spark_logger.warn(f'Loaded config from {config_files[0]}')
        else:
            spark_logger.warn('No config file found')
            config_dict = None

    except Exception as e:
        # Lida com exceções e registra um aviso
        spark_logger.error(f'Error loading config file: {str(e)}')
        config_dict = None


    return spark_sess, spark_logger, config_dict

