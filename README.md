# Healthcare-Data-Analytics-Pipeline

## Purpose

The purpose of this project is to build a robust and scalable big data pipeline for processing and analyzing healthcare data from the MIMIC-III clinical database. The project aims to support both batch and real-time analytics to uncover critical insights such as average patient age by MapReduce task, patient monitoring trends, length-of-stay predictions, readmission risk assessments and another analytics. By leveraging powerful big data technologies like Hadoop and Hive. The system will efficiently handle large volumes of clinical data and deliver meaningful healthcare analytics.

This project not only demonstrates technical proficiency in big data processing but also applies these technologies in a high-impact domain—healthcare—to improve decision-making and patient outcomes.

## Objectives

- Extract raw healthcare data from CSV files.

- Clean and preprocess data using Pandas.

- Convert cleaned data to Parquet format for efficient storage.

- Store Parquet files in HDFS within a Docker container.

- Create Hive external tables for SQL-based analysis.

## Pipeline Overview

This project focuses on preparing MIMIC-III clinical data for big data processing by performing the following key tasks:

1. Data Extraction
   Load raw clinical data from the following MIMIC-III CSV files:
    - PATIENTS.csv
    - ADMISSIONS.csv
    - ICUSTAYS.csv
    - DIAGNOSES_ICD.csv
    - LABEVENTS.csv
    - D_LABITEMS.csv

3. Data Cleaning and Preprocessing using Pandas:
- Perform comprehensive data preparation in Python with Pandas:
  - Clean inconsistencies and handle missing/null values.
  - Filter and retain relevant clinical records for analysis.
  - Standardize column types (e.g., date/time formats, integers, strings).
  - Output cleaned datasets as:
    - PATIENTS_processed.csv and PATIENTS.parquet
    - Parquet files for all other datasets (e.g., ADMISSIONS.parquet, ICUSTAYS.parquet, etc.)
      
3. Data Storage in HDFS
   Store all cleaned Parquet files and PATIENTS_processed.csv in a Hadoop Distributed File System (HDFS) using a Dockerized environment on Ubuntu.

4. MapReduce Task
   To determine average patient age.

5. Hive Table Creation
   Define and create external Hive tables for all Parquet datasets to support efficient SQL-based batch analytics.

## 📌 Data Pipeline Architecture

![Flow](https://github.com/user-attachments/assets/e0a28f74-2bd3-41c8-bb05-744bbf9febc2)

## Technologies Used

- Python (Pandas, PyArrow)
- Ubuntu (as the host operating system)
- Hadoop (HDFS)
- Apache Hive
- Docker

## Contact Information

- Name: Mohamed Osama Abdulrhman
- Email: mohamedosama8599@gmail.com

## Project Structure

<pre> 
healthcare-project/
├── mimic-data-parquet/
│   ├── ADMISSIONS.parquet
│   ├── DIAGNOSES_ICD.parquet
│   ├── D_LABITEMS.parquet
│   ├── ICUSTAYS.parquet
│   ├── LABEVENTS.parquet
│   └── PATIENTS.parquet
├── PATIENTS_processed.csv
├── avg_age/
│   ├── AverageAge.java         # MapReduce job to calculate average patient age
    └── avg_result.txt          # Output/results from MapReduce job
├── create_tables.hql           # Hive script to create tables
├── mimic_analytics.hql         # Hive analytics queries on MIMIC data
├── hive-site.xml               # Hive configuration file
└── analytics_results.txt       # Output/results from analytics queries
<pre>
