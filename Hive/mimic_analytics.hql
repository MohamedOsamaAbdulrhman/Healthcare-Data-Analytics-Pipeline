USE mimiciii;

-- Query 1: Average Length of Stay per Diagnosis
SELECT 
    a.diagnosis,
    ROUND(AVG(DATEDIFF(TO_DATE(a.dischtime), TO_DATE(a.admittime))), 2) AS avg_los_days
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
    FLOOR(DATEDIFF(TO_DATE(a.admittime), TO_DATE(p.dob)) / 365.25) AS age,
    COUNT(*) AS patient_count
FROM admissions a
JOIN patients p ON a.subject_id = p.subject_id
WHERE a.admittime IS NOT NULL AND p.dob IS NOT NULL
GROUP BY FLOOR(DATEDIFF(TO_DATE(a.admittime), TO_DATE(p.dob)) / 365.25)
ORDER BY age;
