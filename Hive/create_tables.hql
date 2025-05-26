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
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/ADMISSIONS';

-- Table for DIAGNOSES_ICD
CREATE EXTERNAL TABLE diagnoses_icd (
    row_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    seq_num BIGINT,
    icd9_code STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/DIAGNOSES_ICD';

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
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/D_LABITEMS';

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
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/ICUSTAYS';

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
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/LABEVENTS';

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
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/mimiciii/PATIENTS';
