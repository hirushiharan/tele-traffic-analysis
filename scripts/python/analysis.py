"""
Program: Telecommunication Traffic Data Analysis
Author: Hirushiharan Thevendran
Organization: UoM Distributed Computing Concepts for AI module mini project
Created On: 06/15/2024
Last Modified By: Hirushiharan
Last Modified On: 06/21/2024

Program Description: A program to perform data analysis on telecommunication traffic data. This script reads processed data 
from HDFS in Parquet format and conducts various analyses including descriptive statistics, user-based aggregations, 
tower-based aggregations, hourly call analysis, and trend analysis.

Python Version: 3.9-slim
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum as Fsum, mean, stddev, variance, hour, to_date, date_format
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

hadoop_path = "/user/hadoop/telecom_data/"
hadoop_files = ["processed_data", "busiest_hour", "high_value_users", "low_usage_users", "call_data_by_tower", "call_duration_per_user"]

def create_visualization(df, x_col, y_col, plot_type="bar", title="", xlabel="", ylabel="", top_n=None, xticks_rotation=0):
    """
    Create a dynamic visualization based on the provided parameters.

    Args:
        df (DataFrame): Input DataFrame.
        x_col (str): Column name for x-axis.
        y_col (str): Column name for y-axis.
        plot_type (str): Type of plot (e.g., "bar", "line", "hist").
        title (str): Title of the plot.
        xlabel (str): Label for x-axis.
        ylabel (str): Label for y-axis.
        top_n (int, optional): Number of top records to display (for bar plots).
        xticks_rotation (int, optional): Rotation angle for x-ticks.

    Returns:
        None
    """
    pd_df = df.toPandas()

    if top_n:
        pd_df = pd_df.head(top_n)
    
    plt.figure(figsize=(12, 8))
    
    if plot_type == "bar":
        sns.barplot(x=x_col, y=y_col, data=pd_df)
    elif plot_type == "line":
        sns.lineplot(x=x_col, y=y_col, data=pd_df, marker="o")
    elif plot_type == "hist":
        sns.histplot(pd_df[x_col], bins=30, kde=True)
    
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=xticks_rotation)
    plt.show()

def descriptive_statistics(df):
    """
    Calculate and display descriptive statistics for total call duration.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        None
    """
    stats = df.select(
        mean("total_call_duration").alias("mean_duration"),
        stddev("total_call_duration").alias("stddev_duration"),
        variance("total_call_duration").alias("variance_duration")
    ).first()
    
    quantiles = df.approxQuantile("total_call_duration", [0.5], 0.01)
    
    print(f"Mean call duration: {stats['mean_duration']}")
    print(f"Standard deviation of call duration: {stats['stddev_duration']}")
    print(f"Variance of call duration: {stats['variance_duration']}")
    print(f"Median call duration: {quantiles[0]}")

    # Visualization
    create_visualization(df, x_col="total_call_duration", y_col=None, plot_type="hist", title="Distribution of Call Durations", xlabel="Call Duration", ylabel="Frequency")

def user_based_aggregations(df):
    """
    Perform user-based aggregations and visualize the results.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        None
    """
    user_stats = df.groupBy("caller_user_name").agg(
        Fsum("total_call_duration").alias("total_call_duration"),
        avg("total_call_duration").alias("avg_call_duration"),
        count("total_call_duration").alias("total_calls")
    )
    user_stats.show(truncate=False)
    
    # Visualization
    create_visualization(user_stats, x_col="caller_user_name", y_col="total_call_duration", plot_type="bar", title="Total Call Duration per User (Top 20)", xlabel="User", ylabel="Total Call Duration", top_n=20, xticks_rotation=45)

def tower_based_aggregations(df):
    """
    Perform tower-based aggregations and visualize the results.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        None
    """
    tower_stats = df.groupBy("caller_tower_name").agg(
        Fsum("total_call_duration").alias("total_call_duration"),
        count("caller_tower_name").alias("total_users")
    )
    tower_stats.show(truncate=False)
    
    # Visualization
    create_visualization(tower_stats, x_col="caller_tower_name", y_col="total_call_duration", plot_type="bar", title="Total Call Duration by Tower (Top 20)", xlabel="Tower", ylabel="Total Call Duration", top_n=20, xticks_rotation=45)

def hourly_analysis(df):
    """
    Perform hourly call analysis and visualize the results.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        None
    """
    hourly_stats = df.withColumn("call_hour", hour("call_start_time")).groupBy("call_hour").agg(
        count("call_duration").alias("total_calls"),
        Fsum("call_duration").alias("total_call_duration")
    )
    hourly_stats.show(truncate=False)
    
    # Visualization
    create_visualization(hourly_stats, x_col="call_hour", y_col="total_calls", plot_type="line", title="Total Calls by Hour of Day", xlabel="Hour of Day", ylabel="Total Calls", xticks_rotation=0)

def trend_analysis(df):
    """
    Perform trend analysis on daily and monthly call data and visualize the results.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        None
    """
    daily_trends = df.withColumn("call_date", to_date("call_start_time")).groupBy("call_date").agg(
        count("call_duration").alias("total_calls"),
        Fsum("call_duration").alias("total_call_duration")
    )
    
    monthly_trends = df.withColumn("call_month", date_format("call_start_time", "yyyy-MM")).groupBy("call_month").agg(
        count("call_duration").alias("total_calls"),
        Fsum("call_duration").alias("total_call_duration")
    )
    
    daily_trends.orderBy("call_date").show(truncate=False)
    monthly_trends.orderBy("call_month").show(truncate=False)
    
    # Visualization
    create_visualization(daily_trends.orderBy("call_date"), x_col="call_date", y_col="total_calls", plot_type="line", title="Daily Total Calls", xlabel="Date", ylabel="Total Calls", xticks_rotation=45)
    create_visualization(monthly_trends.orderBy("call_month"), x_col="call_month", y_col="total_calls", plot_type="line", title="Monthly Total Calls", xlabel="Month", ylabel="Total Calls", xticks_rotation=45)

def analyze_total_call_duration_per_user(spark):
    """
    Load and analyze total call duration per user.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        None
    """
    df = spark.read.parquet(f"{hadoop_path}{hadoop_files[5]}")
    print("Total Call Duration per User Data:")
    descriptive_statistics(df)
    user_based_aggregations(df)

def analyze_busiest_hour(spark):
    """
    Load and display data for the busiest hour.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        None
    """
    df = spark.read.parquet(f"{hadoop_path}{hadoop_files[1]}")
    print("Busiest Hour Data:")
    df.show(truncate=False)

def analyze_high_value_users(spark):
    """
    Load and display data for high-value users.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        None
    """
    df = spark.read.parquet(f"{hadoop_path}{hadoop_files[2]}")
    print("High Value Users Data:")
    df.show(truncate=False)

def analyze_low_usage_users(spark):
    """
    Load and display data for low-usage users.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        None
    """
    df = spark.read.parquet(f"{hadoop_path}{hadoop_files[3]}")
    print("Low Usage Users Data:")
    df.show(truncate=False)

def analyze_call_data_by_tower(spark):
    """
    Load and analyze call data by tower location.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        None
    """
    df = spark.read.parquet(f"{hadoop_path}{hadoop_files[4]}")
    print("Call Data by Tower:")
    tower_based_aggregations(df)

def main():
    """
    Main function to perform data analysis on telecommunication data.

    Returns:
        None
    """
    spark = SparkSession.builder.appName("Analysis").getOrCreate()
    
    try:
        # Analyze each dataset
        analyze_total_call_duration_per_user(spark)
        analyze_busiest_hour(spark)
        analyze_high_value_users(spark)
        analyze_low_usage_users(spark)
        analyze_call_data_by_tower(spark)
        
        processed_data_df = spark.read.parquet(f"{hadoop_path}{hadoop_files[0]}")
        hourly_analysis(processed_data_df)
        trend_analysis(processed_data_df)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
