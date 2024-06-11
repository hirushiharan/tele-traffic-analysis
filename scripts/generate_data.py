"""
    Program: Telecommunication Traffic Analysis
    Author: Hirushiharan Thevendran
    Organization: UoM Distributive 
    Created On: 06/12/2024
    Last modified By: Hirushiharan
    Last Modified On: 06/12/2024

    Program Description: A program to generate synthetic data for your MySQL tables, including users, cell towers, and calls, you can use 
    Python with libraries such as faker for generating fake data and pandas for data manipulation. Below is a Python script that generates 
    synthetic data and inserts it into your MySQL tables. The script aims to generate data to achieve a total size of approximately 5MB for 
    the call_details_view view.

    Python Version: 3.09-slim
"""

import os
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# # Use the provided SERP API key
MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_HOST = os.getenv("MYSQL_HOST")

# Function to generate fake user data
def generate_user_data(fake, num_users):
    """
        Generate user data.

        Args:
            fake (obj): Faker() object.
            num_users (integer): Number of users to generate data.

        Returns:
            user_data: List of synthetic user data.
    """
    user_data = []
    for _ in range(num_users):
        phone_number = fake.random_int(min=6000000000, max=9999999999)
        user_name = fake.user_name()
        user_email = fake.email()
        user_data.append((phone_number, user_name, user_email))
    return user_data

# Function to generate fake cell tower data
def generate_cell_tower_data(fake, num_towers):
    """
        Generate tower data.

        Args:
            fake (obj): Faker() object.
            num_towers (integer): Number of towers to generate data.

        Returns:
            tower_data: List of synthetic tower data.
    """
    tower_data = []
    for _ in range(num_towers):
        tower_name = fake.company()
        tower_location = fake.city()
        tower_data.append((tower_name, tower_location))
    return tower_data

# Function to generate fake call data
def generate_call_data(fake, num_calls, num_users, num_towers):
    """
        Generate call data.

        Args:
            fake (obj): Faker() object.
            num_calls (integer): Number of towers to generate data.
            num_users (integer): Number of users to generate data.
            num_towers (integer): Number of towers to generate data.

        Returns:
            call_data: List of synthetic call data.
    """
    call_data = []
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
    return call_data

# Function to insert data into MySQL tables
def insert_data_into_mysql(user_data, tower_data, call_data):
    """
        Insert generated synthetic data to the SQL database.

        Args:
            user_data: List of synthetic user data.
            tower_data: List of synthetic tower data.
            call_data: List of synthetic call data.

        Returns: Void
            
    """
    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_ROOT_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = connection.cursor()

    # Insert data into Users table
    user_query = "INSERT INTO users (PHONE_NUMBER, USER_NAME, USER_EMAIL) VALUES (%s, %s, %s)"
    cursor.executemany(user_query, user_data)
    connection.commit()
    print("Inserted {} rows into Users table.".format(len(user_data)))

    # Insert data into Cell Towers table
    tower_query = "INSERT INTO cell_towers (TOWER_NAME, TOWER_LOCATION) VALUES (%s, %s)"
    cursor.executemany(tower_query, tower_data)
    connection.commit()
    print("Inserted {} rows into Cell Towers table.".format(len(tower_data)))

    # Insert data into Calls table
    call_query = """INSERT INTO calls 
                    (CALLER_USER_ID, CALEE_USER_ID, CALLER_TOWER_ID, CALEE_TOWER_ID, 
                    CALL_START_DATE_TIME, CALL_END_DATE_TIME, CALL_TYPE) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    cursor.executemany(call_query, call_data)
    connection.commit()
    print("Inserted {} rows into Calls table.".format(len(call_data)))

    connection.close()

# Main function to generate and insert data
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
