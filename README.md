# Telecom Data Management

This repository contains scripts for managing and analyzing telecom data. It includes scripts for collecting, preprocessing, analyzing, and managing telecom data stored in a MySQL database.

## Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Project Structure](#project-structure)
4. [File Descriptions](#file-descriptions)
5. [Usage](#usage)
6. [Authors](#authors)
7. [License](#license)

## Introduction

This project aims to facilitate the management and analysis of telecom data. It provides a set of scripts to perform various tasks such as data collection, preprocessing, analysis, and database management. The scripts are designed to work with a MySQL database containing telecom-related tables such as `users`, `cell_towers`, and `calls`.

## Prerequisites

Before running the scripts, make sure you have the following installed:

- Docker
- MySQL database
- Python 3.9 or later
- Apache Spark
- Apache Hadoop

## Project Structure

The project structure is as follows:

    telecom-traffic-analysis/
    │
    ├── notebooks/
    │ └── tele_traffic_analysis.ipynb    # Jupyter notebook for data analysis, visualization, and other tasks.
    │
    ├── scripts/
    │ ├── python
    │ │ ├── generate_data.py             # Script to generate synthetic telecom data.
    │ │ ├── preprocessing.py             # Script to load data from a MySQL view into HDFS using Spark and perform preprocessing tasks.
    │ │ ├── analysis.py                  # Script to perform data analysis on telecommunication data.
    │ │ └── read_hdfs_data.py            # Script to read Parquet files from HDFS, inspect and count the records in each partition.
    │ │
    │ ├── sql
    │ │ ├── table-row-count.sql          # Script to retrieve the total count of rows in each table of the telecom database.
    │ │ ├── truncate.sql                 # Script to truncate all tables in the telecom db, with foreign key checks.
    │ │ └── table-size.sql               # Script to calculate the size of each table in the telecom database in megabytes.
    │
    ├── docker/
    │ ├── mysql-connector
    │ │ └── mysql-connector-j-8.0.33.jar
    │ ├── Dockerfile-hadoop              # Docker config file for Apache Hadoop.
    │ ├── Dockerfile-spark               # Docker config file for Apache Spark.
    │ ├── Dockerfile-python              # Docker config file for Python.
    │ ├── entrypoint.sh                  # Apache Hadoop command to set JAVA_HOME.
    │ ├── 1_init_db.sql                  # Script to create initial MySQL tables.
    │ └── 2_create_views.sql             # Script to create MySQL view.
    │
    ├── .env                             # Environment variables to contain all secret keys.
    ├── docker-compose.yml               # Docker config file.
    ├── requirements.txt                 # Contains Python dependencies.
    └── README.md

## Usage

To use the scripts, follow these steps:

1. **Preprocessing Data**:
   - Run `preprocessing.py` to load data from a MySQL view into HDFS using Spark and perform preprocessing tasks.

2. **Inspecting and Counting Records**:
   - Run `read_hdfs_data.py` to read Parquet files from HDFS, inspect records, and count the number of records in each partition.

3. **Analyzing Data**:
   - Run `analysis.py` to perform data analysis on telecommunication data.

4. **Generating Synthetic Data**:
   - Run `generate_data.py` to generate synthetic telecom data for testing and analysis.

## Authors

- Hirushiharan Thevendran – [LinkedIn](https://www.linkedin.com/in/hirushiharan-thevendran-a08a82152?lipi=urn%3Ali%3Apage%3Ad_flagship3_profile_view_base_contact_details%3B54o2t%2B3cRw6IQKiNxmk27A%3D%3D)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
