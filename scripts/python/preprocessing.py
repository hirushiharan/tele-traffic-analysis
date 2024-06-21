
"""
Program: Telecommunication Traffic Data Collection
Author: Hirushiharan Thevendran
Organization: UoM Distributed Computing Concepts for AI module mini project
Created On: 06/15/2024
Last modified By: Hirushiharan
Last Modified On: 06/20/2024

Program Description: A program to load data from a MySQL view into HDFS using Spark. This script connects to a MySQL database,
retrieves data from a specific view, and writes it into HDFS in Parquet format after processing the data after pre-processing the data.
This script performs the following preprocessing steps:

    1. Remove null rows: Check for values values in each row and remove.
    
    2. Calculate the Total Call Duration per User: Calculate the total call duration per user.
    
    3. Find the busiest hour of the day based on call start and end times: Calculate the busiest hour based on Start date & time and End date & time.
    
    4. Group Customers Based on Their Usage Patterns: Calculate the total call duration per user and categorize them as high-value or low-usage customers.
    
    5. Aggregate Call Data by the Tower Location: Aggregate call data by the tower location, calculating the total call duration and the number of users per tower location.
    
    6. Identify and Remove Duplicate Records: Identify and remove duplicate records from the dataset.

Python Version: 3.9-slim
"""

import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
from pyspark.sql.functions import count
from pyspark.sql.functions import row_number
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.functions import col, hour, concat, isnan, when

hadoop_path = "/user/hadoop/telecom_data/"
hadoop_files = ["processed_data", "busiest_hour", "high_value_users", "low_usage_users", "call_data_by_tower", "call_duration_per_user"]

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
        .appName("Preprocessing Telecom Data") \
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

def remove_null_rows(df):
    """
    Remove rows with any null values.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with rows containing null values removed.
    """
    log("Removing rows with any null values...", "INFO")
    
    df = df.dropna()
    
    return df
    
def calculate_total_call_duration_per_user(df):
    """
    Calculate the total call duration per user.

    Args:
        df (DataFrame): Spark DataFrame.

    Returns:
        DataFrame: Spark DataFrame with the total call duration per user.
    """
    log("Calculating total call duration per user...", "INFO")
    call_duration_per_user = df.groupBy("caller_user_name").agg({"call_duration": "sum"}).withColumnRenamed("sum(call_duration)", "total_call_duration")
    return call_duration_per_user

def find_busiest_hour(df, spark):
    """
    Find the busiest hour of the day based on call start and end times.

    Args:
        df (DataFrame): Spark DataFrame.

    Returns:
        Row: Row containing the busiest hour information.
    """
    log("Finding the busiest hour of the day...", "INFO")

    # Combine call start and end times into a single timestamp column
    df = df.withColumn("call_start_time", concat(col("call_start_date"), lit(" "), col("call_start_time")))
    df = df.withColumn("call_end_time", concat(col("call_end_date"), lit(" "), col("call_end_time")))
    
    # Convert to timestamp type
    df = df.withColumn("call_start_time", col("call_start_time").cast("timestamp"))
    df = df.withColumn("call_end_time", col("call_end_time").cast("timestamp"))
    
    # Extract the hour of the day from the combined timestamp column
    df = df.withColumn("call_start_hour", hour("call_start_time"))
    df = df.withColumn("call_end_hour", hour("call_end_time"))

    # Combine the start and end hours
    df = df.withColumn("call_hour", concat(col("call_start_hour"), lit("-"), col("call_end_hour")))
    
    # Count the number of calls for each hour
    busiest_hour = df.groupBy("call_hour").count().orderBy(col("call_hour").asc())
    
    # Convert the result to a DataFrame
    busiest_hour_df = spark.createDataFrame([busiest_hour])
    
    return busiest_hour_df

def group_customers_by_usage(df):
    """
    Group customers based on their usage patterns.

    Args:
        df (DataFrame): Spark DataFrame.

    Returns:
        DataFrame: Spark DataFrame with customers grouped based on usage patterns.
    """
    log("Grouping customers based on their usage patterns...", "INFO")
    # Calculate the total call duration per user
    call_duration_per_user = calculate_total_call_duration_per_user(df)
    
    # Define window specification with partition by 'caller_user_name'
    window_spec = Window.partitionBy("caller_user_name").orderBy(desc("total_call_duration"))
    
    # Rank users based on their total call duration
    call_duration_per_user = call_duration_per_user.withColumn("rank", row_number().over(window_spec))
    
    # Filter to identify high-value and low-usage customers
    high_value_users = call_duration_per_user.filter(col("rank") <= lit(100))  # Assuming top 100 users are high-value customers
    low_usage_users = call_duration_per_user.filter(col("rank") > lit(100))    # Assuming remaining users are low-usage customers
    
    return high_value_users, low_usage_users


def aggregate_call_data_by_tower(df):
    """
    Aggregate call data by the tower location.

    Args:
        df (DataFrame): Spark DataFrame.

    Returns:
        DataFrame: Spark DataFrame with call data aggregated by tower location.
    """
    log("Aggregating call data by the tower location...", "INFO")
    call_data_by_location = df.groupBy("caller_tower_name").agg({"call_duration": "sum", "caller_user_name": "count"}) \
                        .withColumnRenamed("sum(call_duration)", "total_call_duration") \
                        .withColumnRenamed("count(caller_user_name)", "total_users")
    return call_data_by_location

def remove_duplicate_records(df):
    """
    Identify and remove duplicate records from the dataset.

    Args:
        df (DataFrame): Spark DataFrame.

    Returns:
        DataFrame: Spark DataFrame with duplicate records removed.
    """
    log("Identifying and removing duplicate records...", "INFO")
    df = df.dropDuplicates()
    return df

def write_data_to_hdfs(df, output_path):
    """
    Write Spark DataFrame to HDFS in Parquet format.

    Args:
        df (DataFrame): Spark DataFrame to write.
        output_path (str): Output path in HDFS.

    Returns:
        None
    """
    log(f"Writing data to HDFS: {output_path}", "INFO")
    df.write \
        .mode("overwrite") \
        .parquet(output_path)
    log(f"Data successfully written to HDFS: {output_path}", "INFO")

def main():
    log("Starting data preprocessing process...", "INFO")
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
        df = remove_null_rows(df)
        df = remove_duplicate_records(df)        
        busiest_hour  = find_busiest_hour(df, spark)
        high_value_users, low_usage_users = group_customers_by_usage(df)
        call_data_by_tower_df = aggregate_call_data_by_tower(df)
        call_duration_per_user_df = calculate_total_call_duration_per_user(df)

        # Write DataFrames to HDFS in Parquet format
        write_data_to_hdfs(df, f"{hadoop_path}{hadoop_files[0]}")
        write_data_to_hdfs(busiest_hour, f"{hadoop_path}{hadoop_files[1]}")
        write_data_to_hdfs(high_value_users, f"{hadoop_path}{hadoop_files[2]}")
        write_data_to_hdfs(low_usage_users, f"{hadoop_path}{hadoop_files[3]}")
        write_data_to_hdfs(call_data_by_tower_df, f"{hadoop_path}{hadoop_files[4]}")
        write_data_to_hdfs(call_duration_per_user_df, f"{hadoop_path}{hadoop_files[5]}")

        log("Data preprocessing completed.", "INFO")

    except Exception as e:
        log(f"An error occurred: {str(e)}", "ERROR")

    finally:
        # Stop SparkSession
        spark.stop()
        log("Spark session stopped.", "INFO")

if __name__ == "__main__":
    main()
