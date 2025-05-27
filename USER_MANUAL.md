#  User Manual – Healthcare Data Analytics Pipeline (Batch Processing)

This manual documents all the detailed steps carried out in the project, including environment setup using Docker, data cleaning with pandas, data transformation using PyArrow, HDFS ingestion, Hive SQL batch processing, and a Java MapReduce program.


---

##  Objective

To implement a Big Data batch analytics pipeline for healthcare data using the MIMIC-III dataset. The pipeline enables:

- HDFS for distributed storage
- Hive for SQL-like batch queries
- MapReduce for average age calculation
- Docker to simulate the big data environment

---

##  Tools Used

| Tool          | Purpose                                      |
|---------------|----------------------------------------------|
| Docker        | Running Hadoop + Hive in containers          |
| Hadoop HDFS   | Store large structured files                 |
| Hive          | SQL engine for querying big data             |
| MapReduce     | Custom job to calculate average patient age  |
| PyArrow       | Used for converting cleaned data to Parquet  |
| Pandas        | For cleaning and preparing the dataset       |

---

##  Dataset

- **Source:** MIMIC-III Clinical Database Demo v1.4
- **Files Extracted and Processed:**
  - PATIENTS.csv
  - ADMISSIONS.csv
  - ICUSTAYS.csv
  - LABEVENTS.csv
  - DIAGNOSES_ICD.csv

---

##  Step 1: Data Cleaning (Python)

- Cleaned each CSV using pandas:
  - Converted date columns using `pd.to_datetime()`
  - Dropped duplicates
  - Preserved null values intentionally for medical meaning
- Exported cleaned data back to `.csv`
- Then converted to `.parquet` using `pyarrow` with Spark-compatible schema

---

##  Step 2: Docker Environment Setup

1. Cloned the Hadoop/Spark/Hive repo:
   ```bash
   git clone https://github.com/Marcel-Jan/docker-hadoop-spark.git
   cd docker-hadoop-spark
   docker compose up -d
   ```

2. Entered the `namenode` container:
   ```bash
   docker exec -it namenode bash
   ```

---

##  Step 3: Upload Parquet Files to HDFS

1. From host to container:
   ```bash
   docker cp patients_cleaned.parquet namenode:/tmp/
   ```

2. From container to HDFS:
   ```bash
   hdfs dfs -mkdir -p /user/mimic/cleaned_data/patients
   hdfs dfs -put /tmp/patients_cleaned.parquet /user/mimic/cleaned_data/patients/
   ```

3. Repeated the same for:
   - admissions
   - diagnoses
   - icustays
   - labevents

---

##  Step 4: Create Hive External Tables

1. Logged into Hive:
   ```bash
   hive
   ```

2. Created table example:
   ```sql
   CREATE EXTERNAL TABLE IF NOT EXISTS mimic_cleaned.patients (
     row_id BIGINT,
     subject_id BIGINT,
     gender STRING,
     dob TIMESTAMP,
     dod TIMESTAMP,
     dod_hosp TIMESTAMP,
     dod_ssn TIMESTAMP,
     expire_flag INT
   )
   STORED AS PARQUET
   LOCATION '/user/mimic/cleaned_data/patients/';
   ```

3. Same done for admissions, diagnoses_icd, icustays, labevents.

---

##  Step 5: Hive Analytics Queries

###  Average Length of Stay per Diagnosis:
```sql
SELECT d.icd9_code,
       COUNT(*) AS num_cases,
       ROUND(AVG(UNIX_TIMESTAMP(a.dischtime) - UNIX_TIMESTAMP(a.admittime)) / 86400, 2) AS avg_los_days
FROM admissions a
JOIN diagnoses_icd d ON a.hadm_id = d.hadm_id
WHERE a.admittime IS NOT NULL AND a.dischtime IS NOT NULL
GROUP BY d.icd9_code
ORDER BY avg_los_days DESC
LIMIT 20;
```

###  ICU Readmission Distribution:
```sql
SELECT subject_id,
       COUNT(icustay_id) AS icu_admissions
FROM icustays
GROUP BY subject_id
HAVING COUNT(icustay_id) > 1
ORDER BY icu_admissions DESC
LIMIT 20;
```

###  Mortality by Demographic:
```sql
SELECT p.gender, a.ethnicity,
       COUNT(*) AS total_admissions,
       SUM(a.hospital_expire_flag) AS num_deaths,
       ROUND(SUM(a.hospital_expire_flag) * 100.0 / COUNT(*), 2) AS mortality_rate_percent
FROM admissions a
JOIN patients p ON a.subject_id = p.subject_id
GROUP BY p.gender, a.ethnicity
ORDER BY mortality_rate_percent DESC;
```

---

##  Step 6: MapReduce Job – Average Age

### 1. Copy `PATIENTS.csv` to container:
```bash
docker cp patients_cleaned.csv namenode:/root/
```

### 2. Compile the MapReduce Java code
```bash
mkdir -p avg_classes
javac -classpath $(hadoop classpath) -d avg_classes AverageAge.java
jar -cvf avg.jar -C avg_classes/ .
```

### 3. Upload to HDFS:
```bash
hdfs dfs -mkdir -p /user/root/mimic
hdfs dfs -put /root/patients_cleaned.csv /user/root/mimic/
```

### 4. Run the job:
```bash
hadoop jar avg.jar AverageAge /user/root/mimic/patients_cleaned.csv /user/root/output_avg
```

### 5. Output:
```bash
hdfs dfs -cat /user/root/output_avg/part-r-00000
```

Result example:
```
Average Age    70
```

---

##  Learning Outcomes

- Built a full big data batch analytics pipeline from scratch
- Understood HDFS file management and Parquet optimization
- Performed large-scale SQL queries with Hive
- Implemented Java-based MapReduce job
- Worked with MIMIC-III, a real-world clinical dataset

---

##  Author

**Youssef Mansour**  
Data Engineering Trainee – ITI Intensive Code Camp
youssef.abdo2910@gmail.com