# Manual for Healthcare Data Analytics Pipeline

![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-66CCFF?logo=apache-hadoop&logoColor=black)
![Hive](https://img.shields.io/badge/Hive-FDEE21?logo=apache-hive&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)

This repository ([MohamedOsamaAbdulrhman/Healthcare-Data-Analytics-Pipeline](https://github.com/MohamedOsamaAbdulrhman/Healthcare-Data-Analytics-Pipeline)) contains a pipeline for processing the MIMIC-III Clinical Database (demo version) using Docker, Hadoop, Hive, and MapReduce.

## Table of Contents

- [Overview](#overview)
- [Purpose](#purpose)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Running the Pipeline](#running-the-pipeline)
  - [Step 1: Clean Data with Pandas](#step-1-clean-data-with-pandas)
  - [Step 2: Copy Parquet Files to HDFS](#step-2-copy-parquet-files-to-hdfs)
  - [Step 3: Run MapReduce Job for Average Age](#step-3-run-mapreduce-job-for-average-age)
  - [Step 4: Set Up Hive Tables](#step-4-set-up-hive-tables)
  - [Step 5: HiveQL Analytics Queries](#step-5-HiveQL-Analytics-Queries)

## Overview

This user manual provides step-by-step instructions to run the **Healthcare Data Analytics Pipeline**, which processes the MIMIC-III Clinical Database (demo version) to analyze healthcare data. The pipeline uses Docker containers with Hadoop, Hive, and MapReduce to ingest, store, and analyze data from six CSV files: `ADMISSIONS.csv`, `ICUSTAYS.csv`, `LABEVENTS.csv`, `PATIENTS.csv`, `DIAGNOSES_ICD.csv`, and `D_LABITEMS.csv`. It performs tasks like calculating average patient age via MapReduce and analyzing admission trends via Hive queries.

### Purpose

The pipeline:

- Cleans and converts MIMIC-III CSV files to Parquet format.
- Stores data in Hadoop Distributed File System (HDFS).
- Runs MapReduce jobs to compute metrics (e.g., average patient age) on PATIENTS.csv.
- Creates Hive external tables for querying.
- Performs analytical queries (e.g., admission counts by date).

### Prerequisites

- **Python 3.6+**: For data cleaning with Pandas.
- **Docker**: Installed with Docker Compose (download) to run the docker-hadoop-spark multi-container environment.
- **Python 3.12+**
- **Java 8**: For running Hadoop MapReduce jobs (pre-built JAR assumed).
- **Git**: For cloning the docker-hadoop-spark repository.
- **Dependencies**:
```bash
pip install pandas pyarrow python-hdfs
```
Install these in a virtual environment and inside the namenode container for the conversion script.

- **Dataset**: MIMIC-III Clinical Database Demo v1.4 from PhysioNet, with (ADMISSIONS.parquet, D_LABITEMS.parquet, DIAGNOSES_ICD.parquet, ICUSTAYS.parquet, LABEVENTS.parquet, PATIENTS.parquet) stored in HDFS at:
  - /user/hive/warehouse/mimiciii/PATIENTS/PATIENTS.parquet
  - /user/hive/warehouse/mimiciii/ADMISSIONS/ADMISSIONS.parquet
  - /user/hive/warehouse/mimiciii/D_LABITEMS/D_LABITEMS.parquet
  - /user/hive/warehouse/mimiciii/DIAGNOSES_ICD/DIAGNOSES_ICD.parquet
  - /user/hive/warehouse/mimiciii/ICUSTAYS/ICUSTAYS.parquet
  - /user/hive/warehouse/mimiciii/LABEVENTS/LABEVENTS.parquet
  
PATIENTS_processed.csv stored in HDFS at /user/input/PATIENTS_processed/PATIENTS_processed.csv

- **Environment**: Linux system (e.g., Ubuntu) with sufficient resources (at least 8GB RAM, 4 CPU cores) to run Docker containers for Hadoop, Hive, and supporting services.

## Setup

1. **Install Docker**:

   - Ensure Docker Compose is included.

2. **Clone the Repository**:

   ```bash
   git clone https://github.com/abdelrhmanmousa/Healthcare-Data-Analytics-Pipeline
   cd Healthcare-Data-Analytics-Pipeline
   ```

3. **Start Docker Containers**:

   - Run Docker Compose to set up Hadoop, Hive, and Spark containers:
     ```bash
     docker-compose up -d
     ```
   - Verify containers are running:
     ```bash
     docker ps
     ```

4. **Download MIMIC-III Dataset**:

   - Register at [PhysioNet](https://physionet.org/content/mimiciii-demo/1.4/) and download the demo dataset.
   - Extract the following files to a local directory (e.g., `data/mimic-iii/`):
     - `ADMISSIONS.csv`
     - `DIAGNOSES_ICD.csv`
     - `D_LABITEMS.csv`
     - `ICUSTAYS.csv`
     - `LABEVENTS.csv`
     - `PATIENTS.csv`

5. **Install Python Dependencies**:
   ```bash
   pip install pandas pyarrow
   ```

## Running the Pipeline

### Step 1: Clean Data with Pandas

1. Open Jupyter notebook and create a Python scripts (ADMISSIONS Cleansing.ipynb, D_LABITEMS Cleansing.ipynb, DIAGNOSES_ICD Cleansing.ipynb, ICUSTAYS Cleansing.ipynb, LABEVENTS Cleansing.ipynb, PATIENTS Cleansing.ipynb) to clean the MIMIC-III CSV files.

2. **Output**: Parquet files in `~/mimic-data-parquet`.

### Step 2: Copy Parquet Files and CSV file to HDFS

1. Copy Parquet files to the `namenode` container:
   ```bash
   docker cp /home/mo/PATIENTS_processed.csv namenode:/tmp/PATIENTS_processed.csv
   docker cp /home/mo/mimic-data-parquet/ADMISSIONS.parquet namenode:/tmp/ADMISSIONS.parquet
   docker cp /home/mo/mimic-data-parquet/DIAGNOSES_ICD.parquet namenode:/tmp/DIAGNOSES_ICD.parquet
   docker cp /home/mo/mimic-data-parquet/D_LABITEMS.parquet namenode:/tmp/D_LABITEMS.parquet
   docker cp /home/mo/mimic-data-parquet/ICUSTAYS.parquet namenode:/tmp/ICUSTAYS.parquet
   docker cp /home/mo/mimic-data-parquet/LABEVENTS.parquet namenode:/tmp/LABEVENTS.parquet
   docker cp /home/mo/mimic-data-parquet/PATIENTS.parquet namenode:/tmp/PATIENTS.parquet
   ```
2. Access the `namenode` container:

   ```bash
   docker exec -it namenode bash
   ```

3. Upload csv file to HDFS:

   ```bash
   hdfs dfs -mkdir -p /user/mo/mimic3
   hdfs dfs -put /tmp/PATIENTS_processed.csv /user/mo/mimic3/
   ```

4. Upload Parquet files to HDFS:

   ```bash
   # Create HDFS directory structure
   hdfs dfs -mkdir -p /user/hive/warehouse/mimiciii/ADMISSIONS
   hdfs dfs -mkdir -p /user/hive/warehouse/mimiciii/DIAGNOSES_ICD
   hdfs dfs -mkdir -p /user/hive/warehouse/mimiciii/D_LABITEMS
   hdfs dfs -mkdir -p /user/hive/warehouse/mimiciii/ICUSTAYS
   hdfs dfs -mkdir -p /user/hive/warehouse/mimiciii/LABEVENTS
   hdfs dfs -mkdir -p /user/hive/warehouse/mimiciii/PATIENTS

   # Upload Parquet files to HDFS
   hdfs dfs -put /tmp/ADMISSIONS.parquet /user/hive/warehouse/mimiciii/ADMISSIONS/ADMISSIONS.parquet
   hdfs dfs -put /tmp/DIAGNOSES_ICD.parquet /user/hive/warehouse/mimiciii/DIAGNOSES_ICD/DIAGNOSES_ICD.parquet
   hdfs dfs -put /tmp/D_LABITEMS.parquet /user/hive/warehouse/mimiciii/D_LABITEMS/D_LABITEMS.parquet
   hdfs dfs -put /tmp/ICUSTAYS.parquet /user/hive/warehouse/mimiciii/ICUSTAYS/ICUSTAYS.parquet
   hdfs dfs -put /tmp/LABEVENTS.parquet /user/hive/warehouse/mimiciii/LABEVENTS/LABEVENTS.parquet
   hdfs dfs -put /tmp/PATIENTS.parquet /user/hive/warehouse/mimiciii/PATIENTS/PATIENTS.parquet

   # Set permissions for Hive access
   hdfs dfs -chown -R hdfs:hdfs /user/hive/warehouse/mimiciii
   hdfs dfs -chmod -R 755 /user/hive/warehouse/mimiciii
   ```

### Step 3: Run MapReduce Job for Average Age

1. Install Java

```bash
sudo apt update
sudo apt install default-jdk -y
javac -version
java -version
```

2. Install Hadoop CLI

```bash
sudo apt install hadoop -y
hadoop version
```

3. Create the Project Directory

```bash
mkdir -p ~/avg_age
cd ~/avg_age
```

4. Create and Edit Java File

```bash
nano AverageAge.java
```

```bash
import java.io.IOException;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageAge {

    public static class AgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text word = new Text("age");
        private final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 4 || fields[2].equals("dob")) return;

            try {
                LocalDate dob = LocalDate.parse(fields[2], formatter);
                LocalDate dod = fields[3].isEmpty() ? LocalDate.now() : LocalDate.parse(fields[3], formatter);
                int age = Period.between(dob, dod).getYears();

                if (age > 0 && age < 120) {
                    context.write(word, new IntWritable(age));
                }
            } catch (DateTimeParseException e) {
                // Skip invalid dates
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0, count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            double avg = count == 0 ? 0.0 : (double) sum / count;
            context.write(new Text("Average Age"), new DoubleWritable(avg));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average age");
        job.setJarByClass(AverageAge.class);
        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

5. Compile the Java Code

```bash
javac -classpath `hadoop classpath` -d . AverageAge.java
```

6. Create the JAR File

```bash
jar -cvf AverageAge.jar *
```

7. Run the MapReduce Job

```bash
hadoop jar AverageAge.jar AverageAge hdfs://localhost:9010/user/mo/mimic3/ hdfs://localhost:9010/user/output/average_age
```

8. View the Result

```bash
hdfs dfs -cat /user/output/average_age/part-r-00000
```

9. Copy Output to Local `avg_age` Directory

```bash
hdfs dfs -get /user/output/average_age ~/avg_age/output
```

### Step 4: Set Up Hive Tables

1. Access the `hive-server` container:
   ```bash
   docker exec -it hive-server bash
   ```
2. Start Hive CLI:
   ```bash
   hive
   ```
3. Create external tables for Parquet files:

   ```sql
   CREATE DATABASE IF NOT EXISTS mimiciii;

   USE mimiciii;

   -- Table for ADMISSIONS
   CREATE EXTERNAL TABLE admissions (
    row_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    admittime STRING,
    dischtime STRING,
    deathtime STRING,
    admission_type STRING,
    admission_location STRING,
    discharge_location STRING,
    insurance STRING,
    language STRING,
    religion STRING,
    marital_status STRING,
    ethnicity STRING,
    edregtime STRING,
    edouttime STRING,
    diagnosis STRING,
    hospital_expire_flag BOOLEAN,
    has_chartevents_data BOOLEAN
   )
   STORED AS PARQUET
   LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/ADMISSIONS.parquet';

   -- Table for DIAGNOSES_ICD
   CREATE EXTERNAL TABLE diagnoses_icd (
    row_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    seq_num BIGINT,
    icd9_code STRING
   )
   STORED AS PARQUET
   LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/DIAGNOSES_ICD.parquet';

   -- Table for D_LABITEMS
   CREATE EXTERNAL TABLE d_labitems (
    row_id BIGINT,
    itemid BIGINT,
    label STRING,
    fluid STRING,
    category STRING,
    loinc_code STRING
   )
   STORED AS PARQUET
   LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/D_LABITEMS.parquet';

   -- Table for ICUSTAYS
   CREATE EXTERNAL TABLE icustays (
    row_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    icustay_id BIGINT,
    dbsource STRING,
    first_careunit STRING,
    last_careunit STRING,
    first_wardid BIGINT,
    last_wardid BIGINT,
    intime STRING,
    outtime STRING,
    los DOUBLE
   )
   STORED AS PARQUET
   LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/ICUSTAYS.parquet';

   -- Table for LABEVENTS
   CREATE EXTERNAL TABLE labevents (
    row_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    itemid BIGINT,
    charttime STRING,
    value STRING,
    valuenum DOUBLE,
    valueuom STRING,
    flag STRING
   )
   STORED AS PARQUET
   LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/LABEVENTS.parquet';

   -- Table for PATIENTS
   CREATE EXTERNAL TABLE patients (
    row_id BIGINT,
    subject_id BIGINT,
    gender STRING,
    dob STRING,
    dod STRING,
    dod_hosp STRING,
    dod_ssn STRING,
    expire_flag BOOLEAN
   )
   STORED AS PARQUET
   LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/PATIENTS.parquet';
   ```

4. Copy script to the hive-server container:
   ```bash
   docker cp create_tables.hql hive-server:/create_tables.hql
   ```
5. Run the script:
   ```bash
   docker exec -it hive-server hive -f /create_tables.hql
   ```

## Step 5: HiveQL Analytics Queries

1. mimic_analytics:

   ```sql
   USE mimiciii;

   -- Query 1: Average Length of Stay per Diagnosis
   SELECT
    a.diagnosis,
    ROUND(AVG(CAST(
        (UNIX_TIMESTAMP(TO_DATE(FROM_UTC_TIMESTAMP(a.dischtime, 'UTC'))) -
         UNIX_TIMESTAMP(TO_DATE(FROM_UTC_TIMESTAMP(a.admittime, 'UTC')))) / 86400.0
    AS DOUBLE), 2) AS avg_los_days
   FROM admissions a
   WHERE a.dischtime IS NOT NULL AND a.admittime IS NOT NULL
   GROUP BY a.diagnosis
   ORDER BY avg_los_days DESC
   LIMIT 10;

   -- Query 2: Distribution of ICU Readmissions
   WITH icu_counts AS (
    SELECT
        subject_id,
        COUNT(DISTINCT icustay_id) AS icu_stay_count
    FROM icustays
    GROUP BY subject_id
   )
   SELECT
    icu_stay_count,
    COUNT(subject_id) AS patient_count,
    ROUND(100 * COUNT(subject_id) / SUM(COUNT(subject_id)) OVER (), 2) AS percentage
   FROM icu_counts
   GROUP BY icu_stay_count
   ORDER BY icu_stay_count;

   -- Query 3: Mortality Rates by Demographic Groups
   SELECT
    p.gender,
    a.ethnicity,
    COUNT(*) AS total_admissions,
    SUM(CASE WHEN a.hospital_expire_flag THEN 1 ELSE 0 END) AS deaths,
    ROUND(100 * SUM(CASE WHEN a.hospital_expire_flag THEN 1 ELSE 0 END) / COUNT(*), 2) AS mortality_rate
   FROM admissions a
   JOIN patients p ON a.subject_id = p.subject_id
   GROUP BY p.gender, a.ethnicity
   ORDER BY mortality_rate DESC;

   -- Query 4: Top 5 Most Common ICD-9 Diagnoses
   SELECT
    d.icd9_code,
    COUNT(*) AS diagnosis_count
   FROM diagnoses_icd d
   GROUP BY d.icd9_code
   ORDER BY diagnosis_count DESC
   LIMIT 5;

   -- Query 5: Abnormal Lab Test Frequency by Care Unit
   SELECT
    i.first_careunit,
    l.flag,
    COUNT(*) AS test_count,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY i.first_careunit), 2) AS percentage
   FROM labevents l
   JOIN icustays i ON l.hadm_id = i.hadm_id
   WHERE l.flag IS NOT NULL AND l.flag != ''
   GROUP BY i.first_careunit, l.flag
   ORDER BY i.first_careunit, percentage DESC;

   -- Query 6: Patient Age Distribution at Admission
   SELECT
    FLOOR(DATEDIFF(TO_DATE(FROM_UTC_TIMESTAMP(a.admittime, 'UTC')),
                   TO_DATE(FROM_UTC_TIMESTAMP(p.dob, 'UTC'))) / 365.25) AS age,
    COUNT(*) AS patient_count
   FROM admissions a
   JOIN patients p ON a.subject_id = p.subject_id
   WHERE a.admittime IS NOT NULL AND p.dob IS NOT NULL
   GROUP BY FLOOR(DATEDIFF(TO_DATE(FROM_UTC_TIMESTAMP(a.admittime, 'UTC')),
                        TO_DATE(FROM_UTC_TIMESTAMP(p.dob, 'UTC'))) / 365.25)
   ORDER BY age;
   ```

2. Copy script to the hive-server container:
   ```bash
   docker cp mimic_analytics.hql hive-server:/mimic_analytics.hql
   ```
3. Run the script:
   ```bash
   docker exec -it hive-server hive -f /mimic_analytics.hql
   ```
4. Save results:
   ```bash
   docker exec -it hive-server hive -f /mimic_analytics.hql > analytics_results.txt
   docker cp hive-server:/analytics_results.txt
   ```

