"""
Program: Inspect and Count Records in Parquet Files
Author: Hirushiharan Thevendran
Organization: UoM Distributed Computing Concepts for AI module mini project
Created On: 06/15/2024
Last Modified By: Hirushiharan
Last Modified On: 06/16/2024

Program Description: This script initializes a Spark session, reads Parquet files from HDFS, displays some records for inspection,
counts the number of records in each partition, and prints the counts.

Python Version: 3.9-slim
"""

from pyspark.sql import SparkSession
from datetime import datetime

hadoop_path = "/user/hadoop/telecom_data/"
hadoop_files = ["processed_data", "busiest_hour", "high_value_users", "low_usage_users", "call_data_by_tower", "call_duration_per_user"]


def log(message, level="INFO"):
    """
    Log messages with a timestamp and a specific log level.

    Args:
        message (str): The message to log.
        level (str): The log level (e.g., INFO, WARN, ERROR).

    Returns:
        None
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} [{level}] {message}")

def create_spark_session():
    """
    Create and configure a Spark session.

    Returns:
        SparkSession: Configured Spark session.
    """
    return SparkSession.builder \
        .appName("Inspect and Count Records in Parquet Files") \
        .getOrCreate()

def read_parquet_files(spark, file_path):
    """
    Read Parquet files from HDFS into a Spark DataFrame.

    Args:
        spark (SparkSession): Spark session.
        file_path (str): Path to the Parquet files.

    Returns:
        DataFrame: Spark DataFrame containing the loaded data.
    """
    log(f"Reading Parquet files from {file_path}...", "INFO")
    return spark.read.parquet(file_path)

def show_sample_records(df, num_records=10):
    """
    Display a few records from the DataFrame to inspect the data.

    Args:
        df (DataFrame): Spark DataFrame.
        num_records (int): Number of records to display.

    Returns:
        None
    """
    log(f"Showing first {num_records} records for inspection...", "INFO")
    df.show(num_records)

def count_records_in_partition(iterator):
    """
    Count the number of records in each partition.

    Args:
        iterator (iterator): Iterator over the partition records.

    Returns:
        list: A list containing the count of records in the partition.
    """
    count = sum(1 for _ in iterator)
    return [count]

def count_records_per_partition(rdd):
    """
    Apply the count function to each partition and collect the results.

    Args:
        rdd (RDD): The underlying RDD of the DataFrame.

    Returns:
        list: List containing the count of records in each partition.
    """
    log("Counting records in each partition...", "INFO")
    return rdd.mapPartitions(count_records_in_partition).collect()

def print_partition_counts(counts):
    """
    Print the number of records in each partition.

    Args:
        counts (list): List containing the count of records in each partition.

    Returns:
        None
    """
    for idx, count in enumerate(counts):
        log(f"Partition {idx} (corresponds to part-0000{idx}.parquet): {count} records", "INFO")

def main():
    log("Starting Spark session...", "INFO")
    spark = create_spark_session()

    try:
        for dataframe in hadoop_files:
            file_path = f"{hadoop_path}{dataframe}"
        
            # Read Parquet files from HDFS
            df = read_parquet_files(spark, file_path)
        
            # Show a few records to inspect the data
            show_sample_records(df, num_records=10)

            # Get the underlying RDD
            rdd = df.rdd

            # Get the number of partitions (each partition corresponds to a file)
            num_partitions = rdd.getNumPartitions()
            log(f"Data is divided into {num_partitions} partitions.", "INFO")

            # Count the number of records in each partition
            counts_per_partition = count_records_per_partition(rdd)

            # Print the counts
            print_partition_counts(counts_per_partition)

    except Exception as e:
        log(f"An error occurred: {str(e)}", "ERROR")

    finally:
        # Stop the Spark session
        spark.stop()
        log("Spark session stopped.", "INFO")

if __name__ == "__main__":
    main()
