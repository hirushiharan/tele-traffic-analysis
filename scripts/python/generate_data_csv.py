"""
Program: Telecom Traffic CSV Data Generator
Author: Hirushiharan Thevendran
Organization: UoM Distributed Computing Concepts for AI module mini project
Created On: 06/21/2024
Last modified By: Hirushiharan
Last Modified On: 06/21/2024

Program Description: 
This script generates synthetic telecom traffic data using the Faker library. The data includes call records with
various details such as caller and callee information, tower locations, call start and end times, call duration, 
and call type. The generated data is saved to a CSV file.

Python Version: 3.9
"""

import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Define the number of rows of data to generate
num_rows = 100000

# Define lists for sample tower names and call types
call_types = ['Incoming', 'Outgoing']

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

def generate_call_record():
    """
    Generate a single synthetic call record.

    Returns:
        dict: A dictionary containing the details of a call record.
    """
    call_id = random.randint(1000000, 9999999)
    caller_user_name = fake.user_name()
    callee_user_name = fake.user_name()
    caller_tower_name = fake.company()
    callee_tower_name = fake.company()
    call_start_date_time = fake.date_time_this_year()
    call_duration = timedelta(seconds=random.randint(1, 3600))  # Call duration between 1 second and 1 hour
    call_end_date_time = call_start_date_time + timedelta(seconds=random.randint(10, 3600))
    call_type = random.choice(call_types)
    return {
        'CALL_ID': call_id,
        'CALLER_USER_NAME': caller_user_name,
        'CALLEE_USER_NAME': callee_user_name,
        'CALLER_TOWER_NAME': caller_tower_name,
        'CALLEE_TOWER_NAME': callee_tower_name,
        'CALL_START_DATE_TIME': call_start_date_time,
        'CALL_END_DATE_TIME': call_end_date_time,
        'CALL_DURATION': call_duration,
        'CALL_TYPE': call_type
    }

def main():
    """
    Main function to generate synthetic telecom traffic data and save it to a CSV file.
    """
    try:
        # Generate the data
        log(f"Generating {num_rows} rows of synthetic telecom traffic data...")
        data = [generate_call_record() for _ in range(num_rows)]
        
        # Create a DataFrame
        df = pd.DataFrame(data)
        
        # Write the DataFrame to a CSV file
        csv_file_path = 'documents/telecom_traffic_data.csv'
        df.to_csv(csv_file_path, index=False)
        
        log(f"Data generation complete. File saved as '{csv_file_path}'.")
    
    except Exception as e:
        log(f"An error occurred during data generation: {e}", level="ERROR")

if __name__ == "__main__":
    main()
