"""
Program: MySQL Data Export to Excel
Author: Hirushiharan Thevendran
Organization: UoM Distributed Computing Concepts for AI module mini project 
Created On: 06/19/2024
Last modified By: Hirushiharan
Last Modified On: 06/19/2024

Program Description: This script connects to a MySQL database, retrieves data from a specified view, and exports the data
to an Excel file. The script utilizes environment variables for database credentials, ensuring security and flexibility.

Python Version: 3.9-slim
"""

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime, timedelta

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

def load_env_variables():
    """
    Load environment variables from the .env file.

    Returns:
        dict: A dictionary containing MySQL database credentials.
    """
    load_dotenv()
    return {
        "MYSQL_ROOT_PASSWORD": os.getenv("MYSQL_ROOT_PASSWORD"),
        "MYSQL_DATABASE": os.getenv("MYSQL_DATABASE"),
        "MYSQL_USER": os.getenv("MYSQL_USER", "root"),  # Default to root if not set
        "MYSQL_HOST": os.getenv("MYSQL_HOST", "localhost"),  # Default to localhost if not set
        "MYSQL_PORT": int(os.getenv("MYSQL_PORT", 3306))  # Default to 3306 if not set
    }

def create_sqlalchemy_engine(credentials):
    """
    Create a SQLAlchemy engine using the provided credentials.

    Args:
        credentials (dict): A dictionary containing MySQL database credentials.

    Returns:
        Engine: A SQLAlchemy engine instance.
    """
    connection_string = (
        f"mysql+mysqlconnector://{credentials['MYSQL_USER']}:"
        f"{credentials['MYSQL_ROOT_PASSWORD']}@{credentials['MYSQL_HOST']}/"
        f"{credentials['MYSQL_DATABASE']}"
    )
    return create_engine(connection_string)

def fetch_data_to_dataframe(engine, query):
    """
    Fetch data from the database using the provided SQL query and load it into a Pandas DataFrame.

    Args:
        engine (Engine): A SQLAlchemy engine instance.
        query (str): The SQL query to execute.

    Returns:
        DataFrame: A Pandas DataFrame containing the fetched data.
    """
    return pd.read_sql(query, engine)

def export_dataframe_to_excel(df, file_path):
    """
    Export the provided Pandas DataFrame to an Excel file.

    Args:
        df (DataFrame): The Pandas DataFrame to export.
        file_path (str): The path to the Excel file.

    Returns:
        None
    """
    df.to_excel(file_path, index=False)

def main():
    """
    Main function to perform data export from a MySQL view to an Excel file.

    Returns:
        None
    """
    try:
        # Load environment variables
        credentials = load_env_variables()
        
        # Validate loaded environment variables
        if not all(credentials.values()):
            raise ValueError("Some MySQL credentials are missing in the .env file.")

        # Create SQLAlchemy engine
        engine = create_sqlalchemy_engine(credentials)
        
        # SQL query to fetch data from the call_details_view
        query = "SELECT * FROM call_details_view"
        
        # Fetch data from MySQL and load into a DataFrame
        df = fetch_data_to_dataframe(engine, query)
        
        # Define the Excel file path
        excel_file_path = 'documents/call_details_view.xlsx'
        
        # Export DataFrame to an Excel file
        export_dataframe_to_excel(df, excel_file_path)

        log(f"Data exported successfully to {excel_file_path}", "INFO")
    except Exception as err:
        log(f"Error: {err}", "ERROR")

if __name__ == "__main__":
    main()
