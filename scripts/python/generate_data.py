"""
Program: Telecommunication Traffic Analysis
Author: Hirushiharan Thevendran
Organization: UoM Distributed Computing Concepts for AI module mini project 
Created On: 06/12/2024
Last modified By: Hirushiharan
Last Modified On: 06/14/2024

Program Description: A program to generate synthetic data for your MySQL tables, including users, cell towers, and calls, you can use 
Python with libraries such as faker for generating fake data and pandas for data manipulation. Below is a Python script that generates 
synthetic data and inserts it into your MySQL tables. The script aims to generate data to achieve a total size of approximately 5MB for 
the call_details_view view.

Python Version: 3.9-slim
"""

import os
import time
import random
import pandas as pd
import mysql.connector
from faker import Faker
from dotenv import load_dotenv
from mysql.connector import Error
from datetime import datetime, timedelta

# Load environment variables from the .env file
load_dotenv()

# MySQL database credentials
MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
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

def generate_user_data(fake, num_users):
    """
    Generate synthetic user data.

    Args:
        fake (Faker): Faker instance for generating fake data.
        num_users (int): Number of users to generate data for.

    Returns:
        List of tuples: Each tuple contains phone number, user name, and user email.
    """
    start_time = time.time()
    user_data = []
    log("Generating User Data...", "INFO")
    for _ in range(num_users):
        phone_number = fake.random_int(min=1000000000, max=9999999999)
        user_name = fake.user_name()
        user_email = fake.email()
        user_data.append((phone_number, user_name, user_email))
    log(f"User Data Generated in {round(time.time() - start_time, 2)} seconds.", "INFO")
    return user_data

def generate_cell_tower_data(fake, num_towers):
    """
    Generate synthetic cell tower data.

    Args:
        fake (Faker): Faker instance for generating fake data.
        num_towers (int): Number of cell towers to generate data for.

    Returns:
        List of tuples: Each tuple contains tower name and tower location.
    """
    start_time = time.time()
    tower_data = []
    log("Generating Tower Data...", "INFO")
    for _ in range(num_towers):
        tower_name = fake.company()
        tower_location = fake.city()
        tower_data.append((tower_name, tower_location))
    log(f"Tower Data Generated in {round(time.time() - start_time, 2)} seconds.", "INFO")
    return tower_data

def generate_call_data(fake, num_calls, num_users, num_towers):
    """
    Generate synthetic call data.

    Args:
        fake (Faker): Faker instance for generating fake data.
        num_calls (int): Number of calls to generate data for.
        num_users (int): Number of users to reference in call data.
        num_towers (int): Number of towers to reference in call data.

    Returns:
        List of tuples: Each tuple contains call details including user and tower references.
    """
    start_time = time.time()
    call_data = []
    log("Generating Calls Data...", "INFO")
    for _ in range(num_calls):
        caller_user_id = random.randint(1, num_users)
        callee_user_id = random.randint(1, num_users)
        caller_tower_id = random.randint(1, num_towers)
        callee_tower_id = random.randint(1, num_towers)
        call_start_datetime = fake.date_time_this_year()
        call_end_datetime = call_start_datetime + timedelta(seconds=random.randint(10, 3600))
        call_type = random.choice(['incoming', 'outgoing'])
        call_data.append((
            caller_user_id, callee_user_id,
            caller_tower_id, callee_tower_id,
            call_start_datetime, call_end_datetime, call_type
        ))
    log(f"Calls Data Generated in {round(time.time() - start_time, 2)} seconds.", "INFO")
    return call_data

# Function to insert data into MySQL tables
def insert_data_into_mysql(user_data, tower_data, call_data, batch_size=1000):
    """
    Insert generated synthetic data into MySQL database.

    Args:
        user_data (list): List of user data tuples.
        tower_data (list): List of tower data tuples.
        call_data (list): List of call data tuples.
        batch_size (int): Number of rows to insert per batch.

    Returns:
        None
    """
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_ROOT_PASSWORD,
            database=MYSQL_DATABASE,
            port=MYSQL_PORT
        )
        if connection.is_connected():
            log("SQL Connection Successful", "INFO")
            cursor = connection.cursor()

            # Insert data into Users table
            start_time = time.time()
            user_query = "INSERT INTO users (PHONE_NUMBER, USER_NAME, USER_EMAIL) VALUES (%s, %s, %s)"
            for i in range(0, len(user_data), batch_size):
                batch = user_data[i:i+batch_size]
                cursor.executemany(user_query, batch)
                connection.commit()
            log(f"Inserted {len(user_data)} rows into Users table in {round(time.time() - start_time, 2)} seconds.", "INFO")

            # Insert data into Cell Towers table
            start_time = time.time()
            tower_query = "INSERT INTO cell_towers (TOWER_NAME, TOWER_LOCATION) VALUES (%s, %s)"
            for i in range(0, len(tower_data), batch_size):
                batch = tower_data[i:i+batch_size]
                cursor.executemany(tower_query, batch)
                connection.commit()
            log(f"Inserted {len(tower_data)} rows into Towers table in {round(time.time() - start_time, 2)} seconds.", "INFO")

            # Insert data into Calls table
            start_time = time.time()
            call_query = """INSERT INTO calls 
                            (CALLER_USER_ID, CALEE_USER_ID, CALLER_TOWER_ID, CALEE_TOWER_ID, 
                            CALL_START_DATE_TIME, CALL_END_DATE_TIME, CALL_TYPE) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            for i in range(0, len(call_data), batch_size):
                batch = call_data[i:i+batch_size]
                cursor.executemany(call_query, batch)
                connection.commit()
            log(f"Inserted {len(call_data)} rows into Calls table in {round(time.time() - start_time, 2)} seconds.", "INFO")

    except Error as err:
        log(f"Error: {err}", "ERROR")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            log("MySQL connection is closed", "INFO")

def main():
    fake = Faker()

    # Set the desired number of users, towers, and calls
    num_users = 10000
    num_towers = 500
    desired_num_calls = 10000000  # Adjust this number to achieve approximately 5MB size for the view

    # Generate user data
    user_data = generate_user_data(fake, num_users)

    # Generate tower data
    tower_data = generate_cell_tower_data(fake, num_towers)

    # Generate call data
    call_data = generate_call_data(fake, desired_num_calls, num_users, num_towers)

    # Insert generated data into MySQL tables
    insert_data_into_mysql(user_data, tower_data, call_data)

if __name__ == "__main__":
    main()
