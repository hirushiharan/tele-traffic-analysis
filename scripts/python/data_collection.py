import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Load environment variables from the .env file
load_dotenv()

# MySQL database credentials
MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER", "root") # Default to root if not set
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost") # Default to localhost if not set
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))  # Default to 3306 if not set

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Telecom DB MySQL View to HDFS") \
    .config("spark.jars", "/usr/local/spark/jars/mysql-connector-j-8.0.33.tar.gz") \
    .getOrCreate()

# Define MySQL connection properties
mysql_props = {
    "url": "jdbc:mysql://" + MYSQL_HOST + ":"+ MYSQL_PORT + "/" + MYSQL_DATABASE,
    "driver": "com.mysql.jdbc.Driver",
    "dbtable": "(SELECT * FROM call_details_view) AS call_details_view",
    "user": MYSQL_USER,
    "password": MYSQL_ROOT_PASSWORD
}

# Load data from MySQL view into a DataFrame
df = spark.read \
    .format("jdbc") \
    .option("url", mysql_props["url"]) \
    .option("driver", mysql_props["driver"]) \
    .option("dbtable", mysql_props["dbtable"]) \
    .option("user", mysql_props["user"]) \
    .option("password", mysql_props["password"]) \
    .load()

# Write DataFrame to HDFS in Parquet format
df.write \
    .parquet("/user/hadoop/telecom_data")

# Stop SparkSession
spark.stop()
