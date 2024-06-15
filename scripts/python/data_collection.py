"""
Program: Telecommunication Traffic Data Collection
Author: Hirushiharan Thevendran
Organization: UoM Distributed Computing Concepts for AI module mini project
Created On: 06/14/2024
Last modified By: Hirushiharan
Last Modified On: 06/15/2024

Program Description: A program to load data from a MySQL view into HDFS using Spark. This script connects to a MySQL database,
retrieves data from a specific view, and writes it into HDFS in Parquet format.

Python Version: 3.9-slim
"""

import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Load environment variables from the .env file
load_dotenv()

# MySQL database credentials
MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER", "root") # Default to root if not set
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost") # Default to localhost if not set
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))  # Default to 3306 if not set

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
        .appName("Telecom DB MySQL View to HDFS") \
        .config("spark.jars", "/usr/local/spark/jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "300") \
        .config("spark.sql.broadcastTimeout", "3600") \
        .getOrCreate()

def load_data_from_mysql(spark, mysql_props):
    """
    Load data from MySQL view into a Spark DataFrame.

    Args:
        spark (SparkSession): Spark session.
        mysql_props (dict): MySQL connection properties.

    Returns:
        DataFrame: Spark DataFrame containing the loaded data.
    """
    log("Loading data from MySQL view...", "INFO")
    return spark.read \
        .format("jdbc") \
        .option("url", mysql_props["url"]) \
        .option("driver", mysql_props["driver"]) \
        .option("dbtable", mysql_props["dbtable"]) \
        .option("user", mysql_props["user"]) \
        .option("password", mysql_props["password"]) \
        .load()

def write_data_to_hdfs(df):
    """
    Write Spark DataFrame to HDFS in Parquet format.

    Args:
        df (DataFrame): Spark DataFrame to write.

    Returns:
        None
    """
    log("Writing data to HDFS...", "INFO")
    df.write \
        .mode("overwrite") \
        .parquet("/user/hadoop/telecom_data")
    log("Data successfully written to HDFS.", "INFO")

def main():
    log("Starting data collection process...", "INFO")
    spark = create_spark_session()
    
    # Define MySQL connection properties
    mysql_props = {
        "url": f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
        "driver": "com.mysql.cj.jdbc.Driver",
        "dbtable": "(SELECT * FROM call_details_view) AS call_details_view",
        "user": MYSQL_USER,
        "password": MYSQL_ROOT_PASSWORD
}

    try:
        # Load data from MySQL view into a DataFrame
        df = load_data_from_mysql(spark, mysql_props)

        # Write DataFrame to HDFS in Parquet format
        write_data_to_hdfs(df)

    except Exception as e:
        log(f"An error occurred: {str(e)}", "ERROR")

    finally:
        # Stop SparkSession
        spark.stop()
        log("Spark session stopped.", "INFO")

if __name__ == "__main__":
    main()