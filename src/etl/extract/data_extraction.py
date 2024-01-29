"""
data_extraction.py
~~~~~~~~~~~~~~~~~~

This module contains essential functions for our data pipeline, performing ETL to extract and prepare data from various sources.
Executed as a job on a cluster, it optimizes distributed processing to handle large datasets.
This packager encapsulates critical functionalities, contributing to data cohesion and quality.
Emphasizing modularity and efficiency, with the goal of providing valuable insights.
The process focuses on extracting data from a generic location, promoting clear and concise code.
"""

def extract_csv_data(spark_session, file_path):
    """
    Extracts data from a CSV file using Apache Spark.

    Args:
    - spark_session (SparkSession): An instance of Apache Spark Session for distributed data processing.
    - file_path (str): The path to the CSV file.

    Returns:
    - DataFrame: A Spark DataFrame representing the CSV data.
    """
    # Read CSV data into a Spark DataFrame
    try:
        spark_data = (
            spark_session
            .read
            .csv(file_path, header=True, inferSchema=True)
        )
        return spark_data

    except Exception as e:
        print(f"Error extracting CSV data: {str(e)}")
        return None