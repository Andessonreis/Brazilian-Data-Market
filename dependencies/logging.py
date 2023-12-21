"""
logging
~~~~~~~

This module contains a class that wraps the log4j object instantiated
by the active SparkContext, enabling Log4j logging for PySpark using.
"""
class Log4j:
    """
    Wrapper class for Log4j JVM object.

    :param spark: SparkSession object.
    """

    def __init__(self, spark):
        """
        Initialize the logger with Spark application details.

        :param spark: SparkSession instance
        """
        # Get Spark app details to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        # Access Log4j from the JVM
        log4j = spark._jvm.org.apache.log4j

        # Create a message prefix using app details
        message_prefix = '[' + app_name + '-' + app_id + ']'

        # Set up the logger
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """
        Log an error.

        :param message: Error message to write to the log
        :return: None
        """
        self.logger.error(message)

    def warn(self, message):
        """
        Log a warning.

        :param message: Warning message to write to the log
        :return: None
        """
        self.logger.warn(message)

    def info(self, message):
        """
        Log information.

        :param message: Information message to write to the log
        :return: None
        """
        self.logger.info(message)