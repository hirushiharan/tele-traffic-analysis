from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum as Fsum, mean, stddev, variance, hour, to_date, date_format
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def descriptive_statistics(df):
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
    pd_df = df.select("total_call_duration").toPandas()
    plt.figure(figsize=(10, 6))
    sns.histplot(pd_df["total_call_duration"], bins=30, kde=True)
    plt.title("Distribution of Call Durations")
    plt.xlabel("Call Duration")
    plt.ylabel("Frequency")
    plt.show()

def user_based_aggregations(df):
    user_stats = df.groupBy("caller_user_name").agg(
        Fsum("total_call_duration").alias("total_call_duration"),
        avg("total_call_duration").alias("avg_call_duration"),
        count("total_call_duration").alias("total_calls")
    )
    user_stats.show(truncate=False)
    
    # Visualization
    pd_user_stats = user_stats.toPandas()
    plt.figure(figsize=(12, 8))
    sns.barplot(x="caller_user_name", y="total_call_duration", data=pd_user_stats.head(20))
    plt.title("Total Call Duration per User (Top 20)")
    plt.xlabel("User")
    plt.ylabel("Total Call Duration")
    plt.xticks(rotation=45)
    plt.show()

def tower_based_aggregations(df):
    tower_stats = df.groupBy("caller_tower_name").agg(
        Fsum("total_call_duration").alias("total_call_duration"),
        count("caller_tower_name").alias("total_users")
    )
    tower_stats.show(truncate=False)
    
    # Visualization
    pd_tower_stats = tower_stats.toPandas()
    plt.figure(figsize=(12, 8))
    sns.barplot(x="caller_tower_name", y="total_call_duration", data=pd_tower_stats.head(20))
    plt.title("Total Call Duration by Tower (Top 20)")
    plt.xlabel("Tower")
    plt.ylabel("Total Call Duration")
    plt.xticks(rotation=45)
    plt.show()

def hourly_analysis(df):
    hourly_stats = df.withColumn("call_hour", hour("call_start_time")).groupBy("call_hour").agg(
        count("call_duration").alias("total_calls"),
        Fsum("call_duration").alias("total_call_duration")
    )
    hourly_stats.show(truncate=False)
    
    # Visualization
    pd_hourly_stats = hourly_stats.toPandas()
    plt.figure(figsize=(12, 8))
    sns.lineplot(x="call_hour", y="total_calls", data=pd_hourly_stats, marker="o")
    plt.title("Total Calls by Hour of Day")
    plt.xlabel("Hour of Day")
    plt.ylabel("Total Calls")
    plt.xticks(range(0, 24))
    plt.show()

def trend_analysis(df):
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
    pd_daily_trends = daily_trends.orderBy("call_date").toPandas()
    pd_monthly_trends = monthly_trends.orderBy("call_month").toPandas()

    plt.figure(figsize=(12, 8))
    sns.lineplot(x="call_date", y="total_calls", data=pd_daily_trends, marker="o")
    plt.title("Daily Total Calls")
    plt.xlabel("Date")
    plt.ylabel("Total Calls")
    plt.xticks(rotation=45)
    plt.show()

    plt.figure(figsize=(12, 8))
    sns.lineplot(x="call_month", y="total_calls", data=pd_monthly_trends, marker="o")
    plt.title("Monthly Total Calls")
    plt.xlabel("Month")
    plt.ylabel("Total Calls")
    plt.xticks(rotation=45)
    plt.show()

def analyze_total_call_duration_per_user(spark):
    df = spark.read.parquet("/user/hadoop/telecom_data/call_duration_per_user")
    print("Total Call Duration per User Data:")
    descriptive_statistics(df)
    user_based_aggregations(df)

def analyze_busiest_hour(spark):
    df = spark.read.parquet("/user/hadoop/telecom_data/busiest_hour")
    print("Busiest Hour Data:")
    df.show(truncate=False)

def analyze_high_value_users(spark):
    df = spark.read.parquet("/user/hadoop/telecom_data/high_value_users")
    print("High Value Users Data:")
    df.show(truncate=False)

def analyze_low_usage_users(spark):
    df = spark.read.parquet("/user/hadoop/telecom_data/low_usage_users")
    print("Low Usage Users Data:")
    df.show(truncate=False)

def analyze_call_data_by_tower(spark):
    df = spark.read.parquet("/user/hadoop/telecom_data/call_data_by_tower")
    print("Call Data by Tower:")
    tower_based_aggregations(df)

def main():
    spark = SparkSession.builder.appName("Analysis").getOrCreate()
    
    # Analyze each dataset
    analyze_total_call_duration_per_user(spark)
    analyze_busiest_hour(spark)
    analyze_high_value_users(spark)
    analyze_low_usage_users(spark)
    analyze_call_data_by_tower(spark)
    
    spark.stop()

if __name__ == "__main__":
    main()
